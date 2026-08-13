package clusterha

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/hashicorp/raft"
)

const (
	CommandAcquireLeadership    = "acquire_leadership"
	CommandPutMetadata          = "put_metadata"
	CommandPutMetadataBatch     = "put_metadata_batch"
	CommandCommitSnapshot       = "commit_snapshot"
	CommandActivateCapabilities = "activate_capabilities"
)

type Command struct {
	ProtocolVersion  uint32          `json:"protocol_version"`
	CommandVersion   uint32          `json:"command_version"`
	ID               string          `json:"command_id"`
	Type             string          `json:"command_type"`
	ExpectedRevision *uint64         `json:"expected_revision,omitempty"`
	LeaderEpoch      uint64          `json:"leader_epoch"`
	Payload          json.RawMessage `json:"payload,omitempty"`
	CreatedAt        time.Time       `json:"created_at"`
}

type MetadataState struct {
	ClusterID           string                     `json:"cluster_id"`
	Revision            uint64                     `json:"revision"`
	LeaderEpoch         uint64                     `json:"leader_epoch"`
	LeaderOwner         string                     `json:"leader_owner,omitempty"`
	LastLeadershipTerm  uint64                     `json:"last_leadership_term"`
	Values              map[string]json.RawMessage `json:"values,omitempty"`
	Snapshots           map[string]Manifest        `json:"snapshots,omitempty"`
	ActiveSnapshot      string                     `json:"active_snapshot,omitempty"`
	ActiveSnapshotIndex uint64                     `json:"active_snapshot_index"`
	CapabilityGate      CapabilityGate             `json:"capability_gate"`
	ApplyFault          *FSMApplyFault             `json:"apply_fault,omitempty"`
	AppliedCommands     map[string]AppliedCommand  `json:"applied_commands,omitempty"`
}

func (s *MetadataState) UnmarshalJSON(data []byte) error {
	type metadataState MetadataState
	var raw struct {
		metadataState
		AppliedCommands json.RawMessage `json:"applied_commands,omitempty"`
	}
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	*s = MetadataState(raw.metadataState)
	s.CapabilityGate = normalizeCapabilityGate(s.CapabilityGate)
	s.ApplyFault = cloneFSMApplyFaultPtr(s.ApplyFault)
	s.AppliedCommands = make(map[string]AppliedCommand)
	if len(raw.AppliedCommands) == 0 || string(raw.AppliedCommands) == "null" {
		return nil
	}
	if err := json.Unmarshal(raw.AppliedCommands, &s.AppliedCommands); err == nil {
		return nil
	}
	var legacy map[string]uint64
	if err := json.Unmarshal(raw.AppliedCommands, &legacy); err != nil {
		return err
	}
	for id, revision := range legacy {
		s.AppliedCommands[id] = AppliedCommand{Revision: revision}
	}
	return nil
}

type AppliedCommand struct {
	Revision uint64 `json:"revision"`
	Digest   string `json:"digest"`
}

type acquireLeadershipPayload struct {
	NodeID string `json:"node_id"`
	Term   uint64 `json:"term"`
}

type putMetadataPayload struct {
	Key   string          `json:"key"`
	Value json.RawMessage `json:"value"`
}

type putMetadataBatchPayload struct {
	Values map[string]json.RawMessage `json:"values"`
}

type commitSnapshotPayload struct {
	Manifest Manifest `json:"manifest"`
}

type activateCapabilitiesPayload struct {
	Gate CapabilityGate `json:"gate"`
}

type applyResult struct {
	Revision    uint64 `json:"revision"`
	LeaderEpoch uint64 `json:"leader_epoch"`
	Error       string `json:"error,omitempty"`
}

type metadataFSM struct {
	mu                sync.RWMutex
	state             MetadataState
	capabilities      NodeCapabilities
	faultMu           sync.RWMutex
	applyFault        FSMApplyFault
	faultPersistError string
	faultFile         string
}

type FSMApplyFault struct {
	Index uint64 `json:"index"`
	Error string `json:"error"`
}

func newMetadataFSM(clusterID string) *metadataFSM {
	return &metadataFSM{capabilities: SupportedNodeCapabilities(), state: MetadataState{
		ClusterID:       clusterID,
		Values:          make(map[string]json.RawMessage),
		Snapshots:       make(map[string]Manifest),
		CapabilityGate:  LegacyCapabilityGate(),
		AppliedCommands: make(map[string]AppliedCommand),
	}}
}

func (f *metadataFSM) Apply(log *raft.Log) any {
	var command Command
	if err := json.Unmarshal(log.Data, &command); err != nil {
		f.mu.Lock()
		defer f.mu.Unlock()
		return f.recordApplyFaultLocked(log.Index, fmt.Sprintf("decode command: %v", err))
	}
	if command.ID == "" {
		f.mu.Lock()
		defer f.mu.Unlock()
		return f.recordApplyFaultLocked(log.Index, "command_id is required")
	}
	if command.ProtocolVersion == 0 {
		command.ProtocolVersion = LegacyProtocolVersion
	}
	if command.CommandVersion == 0 {
		command.CommandVersion = LegacyCommandVersion
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if fault, faulted := f.ApplyFault(); faulted {
		return applyResult{Revision: f.state.Revision, LeaderEpoch: f.state.LeaderEpoch,
			Error: fmt.Sprintf("FSM is halted after apply fault at index %d: %s", fault.Index, fault.Error)}
	}
	if !f.capabilities.Protocol.Supports(command.ProtocolVersion) || !f.capabilities.Command.Supports(command.CommandVersion) {
		return f.recordApplyFaultLocked(log.Index, fmt.Sprintf("unsupported command version protocol=%d command=%d", command.ProtocolVersion, command.CommandVersion))
	}
	gate := normalizeCapabilityGate(f.state.CapabilityGate)
	if command.ProtocolVersion != gate.ProtocolVersion || command.CommandVersion != gate.CommandVersion {
		return f.recordApplyFaultLocked(log.Index, fmt.Sprintf("command version protocol=%d command=%d is not active; active protocol=%d command=%d",
			command.ProtocolVersion, command.CommandVersion, gate.ProtocolVersion, gate.CommandVersion))
	}
	digest, err := commandDigest(command)
	if err != nil {
		return applyResult{Revision: f.state.Revision, LeaderEpoch: f.state.LeaderEpoch, Error: err.Error()}
	}
	if applied, ok := f.state.AppliedCommands[command.ID]; ok {
		if applied.Digest != "" && applied.Digest != digest {
			return applyResult{Revision: f.state.Revision, LeaderEpoch: f.state.LeaderEpoch,
				Error: fmt.Sprintf("command_id %q was already applied with a different payload", command.ID)}
		}
		return applyResult{Revision: applied.Revision, LeaderEpoch: f.state.LeaderEpoch}
	}
	if command.ExpectedRevision != nil && *command.ExpectedRevision != f.state.Revision {
		return applyResult{Revision: f.state.Revision, LeaderEpoch: f.state.LeaderEpoch,
			Error: fmt.Sprintf("revision conflict: expected %d, current %d", *command.ExpectedRevision, f.state.Revision)}
	}

	switch command.Type {
	case CommandAcquireLeadership:
		var payload acquireLeadershipPayload
		if err := json.Unmarshal(command.Payload, &payload); err != nil {
			return applyResult{Revision: f.state.Revision, LeaderEpoch: f.state.LeaderEpoch, Error: fmt.Sprintf("decode acquire leadership payload: %v", err)}
		}
		if payload.NodeID == "" || payload.Term == 0 {
			return applyResult{Revision: f.state.Revision, LeaderEpoch: f.state.LeaderEpoch, Error: "acquire leadership requires node_id and term"}
		}
		if payload.Term < f.state.LastLeadershipTerm {
			return applyResult{Revision: f.state.Revision, LeaderEpoch: f.state.LeaderEpoch, Error: "leadership term moved backwards"}
		}
		if payload.Term > f.state.LastLeadershipTerm {
			f.state.LeaderEpoch++
			f.state.LastLeadershipTerm = payload.Term
			f.state.LeaderOwner = payload.NodeID
		}
	case CommandPutMetadata:
		if command.LeaderEpoch != f.state.LeaderEpoch || f.state.LeaderOwner == "" {
			return applyResult{Revision: f.state.Revision, LeaderEpoch: f.state.LeaderEpoch, Error: "leader epoch is not current"}
		}
		var payload putMetadataPayload
		if err := json.Unmarshal(command.Payload, &payload); err != nil {
			return applyResult{Revision: f.state.Revision, LeaderEpoch: f.state.LeaderEpoch, Error: fmt.Sprintf("decode metadata payload: %v", err)}
		}
		if payload.Key == "" {
			return applyResult{Revision: f.state.Revision, LeaderEpoch: f.state.LeaderEpoch, Error: "metadata key is required"}
		}
		f.state.Values[payload.Key] = append(json.RawMessage(nil), payload.Value...)
	case CommandPutMetadataBatch:
		if command.LeaderEpoch != f.state.LeaderEpoch || f.state.LeaderOwner == "" {
			return applyResult{Revision: f.state.Revision, LeaderEpoch: f.state.LeaderEpoch, Error: "leader epoch is not current"}
		}
		var payload putMetadataBatchPayload
		if err := json.Unmarshal(command.Payload, &payload); err != nil {
			return applyResult{Revision: f.state.Revision, LeaderEpoch: f.state.LeaderEpoch, Error: fmt.Sprintf("decode metadata batch payload: %v", err)}
		}
		if len(payload.Values) == 0 {
			return applyResult{Revision: f.state.Revision, LeaderEpoch: f.state.LeaderEpoch, Error: "metadata batch values are required"}
		}
		validated := make(map[string]json.RawMessage, len(payload.Values))
		for key, value := range payload.Values {
			if key == "" {
				return applyResult{Revision: f.state.Revision, LeaderEpoch: f.state.LeaderEpoch, Error: "metadata key is required"}
			}
			validated[key] = append(json.RawMessage(nil), value...)
		}
		for key, value := range validated {
			f.state.Values[key] = value
		}
	case CommandCommitSnapshot:
		if command.LeaderEpoch != f.state.LeaderEpoch || f.state.LeaderOwner == "" {
			return applyResult{Revision: f.state.Revision, LeaderEpoch: f.state.LeaderEpoch, Error: "leader epoch is not current"}
		}
		var payload commitSnapshotPayload
		if err := json.Unmarshal(command.Payload, &payload); err != nil {
			return applyResult{Revision: f.state.Revision, LeaderEpoch: f.state.LeaderEpoch, Error: fmt.Sprintf("decode snapshot payload: %v", err)}
		}
		if payload.Manifest.Generation.ClusterID != f.state.ClusterID {
			return applyResult{Revision: f.state.Revision, LeaderEpoch: f.state.LeaderEpoch, Error: "snapshot cluster_id is not current"}
		}
		if payload.Manifest.Generation.LeaderEpoch != f.state.LeaderEpoch {
			return applyResult{Revision: f.state.Revision, LeaderEpoch: f.state.LeaderEpoch, Error: "snapshot leader epoch is not current"}
		}
		if gate.MinimumVoterCapability >= CapabilityLevelMonotonicSnapshotSequence {
			var previous Manifest
			hasPrevious := f.state.ActiveSnapshot != ""
			if hasPrevious {
				var ok bool
				previous, ok = f.state.Snapshots[f.state.ActiveSnapshot]
				if !ok {
					return f.recordApplyFaultLocked(log.Index, "active snapshot manifest is missing from FSM state")
				}
			}
			if err := validateNextSnapshotSequence(previous, hasPrevious, payload.Manifest.Generation.Sequence); err != nil {
				return applyResult{Revision: f.state.Revision, LeaderEpoch: f.state.LeaderEpoch, Error: err.Error()}
			}
		}
		if err := validateManifestCapability(payload.Manifest, gate); err != nil {
			return f.recordApplyFaultLocked(log.Index, err.Error())
		}
		hash, err := ComputeManifestHash(payload.Manifest)
		if err != nil {
			return applyResult{Revision: f.state.Revision, LeaderEpoch: f.state.LeaderEpoch, Error: err.Error()}
		}
		if hash != payload.Manifest.Generation.ManifestHash {
			return applyResult{Revision: f.state.Revision, LeaderEpoch: f.state.LeaderEpoch, Error: "manifest hash mismatch"}
		}
		if f.state.Snapshots == nil {
			f.state.Snapshots = make(map[string]Manifest)
		}
		f.state.Snapshots[hash] = payload.Manifest
		f.state.ActiveSnapshot = hash
		f.state.ActiveSnapshotIndex = log.Index
	case CommandActivateCapabilities:
		if command.LeaderEpoch != f.state.LeaderEpoch || f.state.LeaderOwner == "" {
			return applyResult{Revision: f.state.Revision, LeaderEpoch: f.state.LeaderEpoch, Error: "leader epoch is not current"}
		}
		var payload activateCapabilitiesPayload
		if err := json.Unmarshal(command.Payload, &payload); err != nil {
			return applyResult{Revision: f.state.Revision, LeaderEpoch: f.state.LeaderEpoch, Error: fmt.Sprintf("decode capability gate payload: %v", err)}
		}
		payload.Gate = normalizeCapabilityGate(payload.Gate)
		if err := validateCapabilityGate(payload.Gate); err != nil {
			return applyResult{Revision: f.state.Revision, LeaderEpoch: f.state.LeaderEpoch, Error: err.Error()}
		}
		if !f.capabilities.SupportsGate(payload.Gate, false) {
			return f.recordApplyFaultLocked(log.Index, "local node does not support requested capability gate")
		}
		f.state.CapabilityGate = payload.Gate
	default:
		return f.recordApplyFaultLocked(log.Index, fmt.Sprintf("unsupported command type %q", command.Type))
	}

	f.state.Revision++
	f.state.AppliedCommands[command.ID] = AppliedCommand{Revision: f.state.Revision, Digest: digest}
	return applyResult{Revision: f.state.Revision, LeaderEpoch: f.state.LeaderEpoch}
}

func (f *metadataFSM) recordApplyFaultLocked(index uint64, message string) applyResult {
	f.faultMu.Lock()
	if f.applyFault.Error == "" {
		f.applyFault = FSMApplyFault{Index: index, Error: message}
		f.state.ApplyFault = cloneFSMApplyFaultPtr(&f.applyFault)
		if err := persistFSMApplyFault(f.faultFile, f.applyFault); err != nil {
			f.faultPersistError = err.Error()
		} else {
			f.faultPersistError = ""
		}
	} else if f.state.ApplyFault == nil {
		f.state.ApplyFault = cloneFSMApplyFaultPtr(&f.applyFault)
	}
	f.faultMu.Unlock()
	return applyResult{Revision: f.state.Revision, LeaderEpoch: f.state.LeaderEpoch, Error: message}
}

func (f *metadataFSM) ApplyFault() (FSMApplyFault, bool) {
	f.faultMu.RLock()
	defer f.faultMu.RUnlock()
	return f.applyFault, f.applyFault.Error != ""
}

func (f *metadataFSM) ApplyFaultPersistError() string {
	f.faultMu.RLock()
	defer f.faultMu.RUnlock()
	return f.faultPersistError
}

func (f *metadataFSM) configureApplyFaultFile(path string) error {
	f.faultMu.Lock()
	f.faultFile = path
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			f.faultMu.Unlock()
			return nil
		}
		f.faultMu.Unlock()
		return err
	}
	if err := json.Unmarshal(data, &f.applyFault); err != nil {
		f.faultMu.Unlock()
		return fmt.Errorf("decode persisted FSM apply fault: %w", err)
	}
	fault := cloneFSMApplyFaultPtr(&f.applyFault)
	f.faultMu.Unlock()
	if fault != nil {
		f.mu.Lock()
		f.state.ApplyFault = fault
		f.mu.Unlock()
	}
	return nil
}

func persistFSMApplyFault(path string, fault FSMApplyFault) error {
	if path == "" {
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return err
	}
	data, err := json.Marshal(fault)
	if err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), ".fsm-apply-fault-*")
	if err != nil {
		return err
	}
	tmpPath := tmp.Name()
	defer os.Remove(tmpPath)
	if err := tmp.Chmod(0600); err != nil {
		_ = tmp.Close()
		return err
	}
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmpPath, path)
}

func commandDigest(command Command) (string, error) {
	var payload any
	if len(command.Payload) > 0 {
		if err := json.Unmarshal(command.Payload, &payload); err != nil {
			return "", fmt.Errorf("canonicalize command payload: %w", err)
		}
	}
	canonical := struct {
		Type             string  `json:"command_type"`
		ExpectedRevision *uint64 `json:"expected_revision,omitempty"`
		LeaderEpoch      uint64  `json:"leader_epoch"`
		ProtocolVersion  uint32  `json:"protocol_version"`
		CommandVersion   uint32  `json:"command_version"`
		Payload          any     `json:"payload,omitempty"`
	}{
		Type:             command.Type,
		ExpectedRevision: command.ExpectedRevision,
		LeaderEpoch:      command.LeaderEpoch,
		ProtocolVersion:  command.ProtocolVersion,
		CommandVersion:   command.CommandVersion,
		Payload:          payload,
	}
	data, err := json.Marshal(canonical)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(data)
	return "sha256:" + hex.EncodeToString(sum[:]), nil
}

func (f *metadataFSM) Snapshot() (raft.FSMSnapshot, error) {
	if fault, faulted := f.ApplyFault(); faulted {
		return nil, fmt.Errorf("FSM apply fault at index %d blocks metadata snapshot: %s", fault.Index, fault.Error)
	}
	state := f.State()
	return &metadataSnapshot{state: state}, nil
}

func (f *metadataFSM) Restore(reader io.ReadCloser) error {
	defer reader.Close()
	var state MetadataState
	if err := json.NewDecoder(reader).Decode(&state); err != nil {
		return err
	}
	if state.ClusterID != f.state.ClusterID {
		return fmt.Errorf("snapshot cluster_id %q does not match configured cluster_id %q", state.ClusterID, f.state.ClusterID)
	}
	if state.Values == nil {
		state.Values = make(map[string]json.RawMessage)
	}
	if state.Snapshots == nil {
		state.Snapshots = make(map[string]Manifest)
	}
	if state.AppliedCommands == nil {
		state.AppliedCommands = make(map[string]AppliedCommand)
	}
	state.CapabilityGate = normalizeCapabilityGate(state.CapabilityGate)
	state.ApplyFault = cloneFSMApplyFaultPtr(state.ApplyFault)
	if state.ApplyFault != nil {
		f.faultMu.Lock()
		f.applyFault = *state.ApplyFault
		if err := persistFSMApplyFault(f.faultFile, f.applyFault); err != nil {
			f.faultPersistError = err.Error()
		} else {
			f.faultPersistError = ""
		}
		f.faultMu.Unlock()
	} else if fault, faulted := f.ApplyFault(); faulted {
		state.ApplyFault = cloneFSMApplyFaultPtr(&fault)
	}
	f.mu.Lock()
	f.state = state
	f.mu.Unlock()
	return nil
}

func (f *metadataFSM) State() MetadataState {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return cloneMetadataState(f.state)
}

type metadataSnapshot struct {
	state MetadataState
}

func (s *metadataSnapshot) Persist(sink raft.SnapshotSink) error {
	if err := json.NewEncoder(sink).Encode(s.state); err != nil {
		_ = sink.Cancel()
		return err
	}
	return sink.Close()
}

func (*metadataSnapshot) Release() {}

func cloneMetadataState(state MetadataState) MetadataState {
	copyState := state
	copyState.CapabilityGate = normalizeCapabilityGate(state.CapabilityGate)
	copyState.ApplyFault = cloneFSMApplyFaultPtr(state.ApplyFault)
	copyState.Values = make(map[string]json.RawMessage, len(state.Values))
	for key, value := range state.Values {
		copyState.Values[key] = append(json.RawMessage(nil), value...)
	}
	copyState.Snapshots = make(map[string]Manifest, len(state.Snapshots))
	for key, manifest := range state.Snapshots {
		copyState.Snapshots[key] = cloneManifest(manifest)
	}
	copyState.AppliedCommands = make(map[string]AppliedCommand, len(state.AppliedCommands))
	for key, command := range state.AppliedCommands {
		copyState.AppliedCommands[key] = command
	}
	return copyState
}

func cloneFSMApplyFaultPtr(fault *FSMApplyFault) *FSMApplyFault {
	if fault == nil || fault.Error == "" {
		return nil
	}
	copyFault := *fault
	return &copyFault
}
