package clusterha

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/hashicorp/raft"
)

func TestMetadataBatchApplyIsAtomicOnValidationFailure(t *testing.T) {
	fsm := newMetadataFSM("cluster")
	fsm.state.LeaderEpoch = 1
	fsm.state.LeaderOwner = "node-a"
	fsm.state.Values["existing"] = json.RawMessage(`"kept"`)
	payload, err := json.Marshal(putMetadataBatchPayload{Values: map[string]json.RawMessage{
		"valid": json.RawMessage(`"should-not-commit"`),
		"":      json.RawMessage(`"invalid"`),
	}})
	if err != nil {
		t.Fatal(err)
	}
	expected := uint64(0)
	result := fsm.Apply(&raft.Log{Index: 1, Data: mustJSON(t, Command{
		ID: "bad-batch", Type: CommandPutMetadataBatch, ExpectedRevision: &expected,
		LeaderEpoch: 1, Payload: payload, CreatedAt: time.Unix(0, 0).UTC(),
	})}).(applyResult)
	if result.Error == "" {
		t.Fatal("expected invalid batch to fail")
	}
	state := fsm.State()
	if state.Revision != 0 {
		t.Fatalf("revision=%d want 0", state.Revision)
	}
	if _, ok := state.Values["valid"]; ok {
		t.Fatal("valid entry from failed batch was partially committed")
	}
	if string(state.Values["existing"]) != `"kept"` {
		t.Fatalf("existing value mutated: %s", state.Values["existing"])
	}
}

func TestMetadataFSMApplyFaultPersistsAcrossRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "fsm-apply-fault.json")
	fsm := newMetadataFSM("cluster")
	if err := fsm.configureApplyFaultFile(path); err != nil {
		t.Fatal(err)
	}
	result := fsm.Apply(&raft.Log{Index: 17, Data: mustJSON(t, Command{
		ID: "future", Type: CommandAcquireLeadership, ProtocolVersion: 2, CommandVersion: 2,
	})}).(applyResult)
	if result.Error == "" {
		t.Fatal("expected unsupported command failure")
	}
	restarted := newMetadataFSM("cluster")
	if err := restarted.configureApplyFaultFile(path); err != nil {
		t.Fatal(err)
	}
	if fault, ok := restarted.ApplyFault(); !ok || fault.Index != 17 || fault.Error == "" {
		t.Fatalf("persisted fault missing after restart: fault=%#v ok=%t", fault, ok)
	}
	restarted.state.LeaderEpoch = 1
	restarted.state.LeaderOwner = "node-a"
	payload := mustJSON(t, putMetadataPayload{Key: "must-not-apply", Value: json.RawMessage(`true`)})
	result = restarted.Apply(&raft.Log{Index: 18, Data: mustJSON(t, Command{
		ID: "after-fault", Type: CommandPutMetadata, ProtocolVersion: 1, CommandVersion: 1,
		LeaderEpoch: 1, Payload: payload,
	})}).(applyResult)
	if result.Error == "" {
		t.Fatal("faulted FSM accepted a later command")
	}
	if _, ok := restarted.State().Values["must-not-apply"]; ok {
		t.Fatal("faulted FSM mutated state after its first apply fault")
	}
}

func TestMetadataFSMApplyFaultBlocksSnapshotAndRestoresFromState(t *testing.T) {
	fsm := newMetadataFSM("cluster")
	result := fsm.Apply(&raft.Log{Index: 17, Data: mustJSON(t, Command{
		ID: "future", Type: CommandAcquireLeadership, ProtocolVersion: 2, CommandVersion: 2,
	})}).(applyResult)
	if result.Error == "" {
		t.Fatal("expected unsupported command failure")
	}
	state := fsm.State()
	if state.ApplyFault == nil || state.ApplyFault.Index != 17 || state.ApplyFault.Error == "" {
		t.Fatalf("apply fault missing from metadata state: %#v", state.ApplyFault)
	}
	if _, err := fsm.Snapshot(); err == nil {
		t.Fatal("faulted FSM was allowed to create a snapshot")
	}

	persistedState := newMetadataFSM("cluster").State()
	persistedState.ApplyFault = &FSMApplyFault{Index: 23, Error: "snapshot fault"}
	data := mustJSON(t, persistedState)
	restarted := newMetadataFSM("cluster")
	faultPath := filepath.Join(t.TempDir(), "fsm-apply-fault.json")
	if err := restarted.configureApplyFaultFile(faultPath); err != nil {
		t.Fatal(err)
	}
	if err := restarted.Restore(io.NopCloser(bytes.NewReader(data))); err != nil {
		t.Fatal(err)
	}
	if fault, ok := restarted.ApplyFault(); !ok || fault.Index != 23 || fault.Error != "snapshot fault" {
		t.Fatalf("snapshot apply fault was not restored: fault=%#v ok=%t", fault, ok)
	}
	if _, err := os.Stat(faultPath); err != nil {
		t.Fatalf("snapshot apply fault was not persisted to sidecar: %v", err)
	}
}

func TestMetadataFSMApplyFaultPersistenceErrorIsExposedInStatus(t *testing.T) {
	fsm := newMetadataFSM("cluster")
	fsm.faultFile = t.TempDir()
	result := fsm.Apply(&raft.Log{Index: 17, Data: mustJSON(t, Command{
		ID: "future", Type: CommandAcquireLeadership, ProtocolVersion: 2, CommandVersion: 2,
	})}).(applyResult)
	if result.Error == "" {
		t.Fatal("expected unsupported command failure")
	}
	if fsm.ApplyFaultPersistError() == "" {
		t.Fatal("expected apply fault sidecar persistence error")
	}
	node := &Node{cfg: Config{}, fsm: fsm, capabilities: SupportedNodeCapabilities()}
	node.updateStatus(RoleFollower, "", "", fsm.State(), map[string]string{
		"term": "1", "commit_index": "17", "applied_index": "17",
	}, false, time.Time{}, "", time.Now())
	status := node.Status()
	if status.FSMApplyPersistError == "" {
		t.Fatalf("status did not expose apply fault persistence error: %#v", status)
	}
}

func TestMetadataFSMRejectsUnsupportedCommandVersion(t *testing.T) {
	fsm := newMetadataFSM("cluster")
	result := fsm.Apply(&raft.Log{Index: 1, Data: mustJSON(t, Command{
		ID: "future", Type: CommandAcquireLeadership, ProtocolVersion: CurrentProtocolVersion + 1,
		CommandVersion: CurrentCommandVersion, CreatedAt: time.Unix(0, 0).UTC(),
	})}).(applyResult)
	if result.Error == "" {
		t.Fatal("expected unsupported protocol version to fail")
	}
	if fsm.State().Revision != 0 {
		t.Fatal("unsupported command must not mutate state")
	}
	if fault, ok := fsm.ApplyFault(); !ok || fault.Index != 1 || fault.Error == "" {
		t.Fatalf("unsupported command did not record FSM fault: fault=%#v ok=%t", fault, ok)
	}
}

func TestMetadataFSMRevisionConflictDoesNotPoisonHealth(t *testing.T) {
	fsm := newMetadataFSM("cluster")
	fsm.state.LeaderEpoch = 1
	fsm.state.LeaderOwner = "node-a"
	expected := uint64(9)
	result := fsm.Apply(&raft.Log{Index: 1, Data: mustJSON(t, Command{
		ID: "conflict", Type: CommandPutMetadata, ProtocolVersion: 1, CommandVersion: 1,
		ExpectedRevision: &expected, LeaderEpoch: 1,
		Payload:   mustJSON(t, putMetadataPayload{Key: "key", Value: json.RawMessage(`"value"`)}),
		CreatedAt: time.Unix(0, 0).UTC(),
	})}).(applyResult)
	if result.Error == "" {
		t.Fatal("expected revision conflict")
	}
	if fault, ok := fsm.ApplyFault(); ok {
		t.Fatalf("deterministic CAS rejection poisoned FSM health: %#v", fault)
	}
}

func TestMetadataFSMLegacyZeroVersionNeverTracksCurrentVersion(t *testing.T) {
	fsm := newMetadataFSM("cluster")
	fsm.capabilities.Protocol.Max = 2
	fsm.capabilities.Command.Max = 2
	result := fsm.Apply(&raft.Log{Index: 1, Data: mustJSON(t, Command{
		ID: "legacy-zero", Type: CommandAcquireLeadership,
		Payload: mustJSON(t, acquireLeadershipPayload{NodeID: "node-a", Term: 1}), CreatedAt: time.Unix(0, 0).UTC(),
	})}).(applyResult)
	if result.Error != "" {
		t.Fatalf("legacy zero-version command failed: %s", result.Error)
	}
	result = fsm.Apply(&raft.Log{Index: 2, Data: mustJSON(t, Command{
		ID: "inactive-v2", Type: CommandAcquireLeadership, ProtocolVersion: 2, CommandVersion: 2,
		Payload: mustJSON(t, acquireLeadershipPayload{NodeID: "node-a", Term: 2}), CreatedAt: time.Unix(0, 0).UTC(),
	})}).(applyResult)
	if result.Error == "" {
		t.Fatal("supported but inactive v2 command must be rejected")
	}
}

func TestMetadataFSMRejectsManifestOutsideCapabilityGate(t *testing.T) {
	fsm := newMetadataFSM("cluster")
	fsm.state.LeaderEpoch = 1
	fsm.state.LeaderOwner = "node-a"
	manifest := Manifest{
		SchemaVersion: 2,
		Generation:    Generation{ClusterID: "cluster", LeaderEpoch: 1, Sequence: 1},
		Datasets:      map[string]DatasetRef{"wan": {Name: "wan", SchemaVersion: 1}},
	}
	manifest.Generation.ManifestHash, _ = ComputeManifestHash(manifest)
	result := fsm.Apply(&raft.Log{Index: 1, Data: mustJSON(t, Command{
		ID: "future-manifest", Type: CommandCommitSnapshot, ProtocolVersion: 1, CommandVersion: 1, LeaderEpoch: 1,
		Payload: mustJSON(t, commitSnapshotPayload{Manifest: manifest}), CreatedAt: time.Unix(0, 0).UTC(),
	})}).(applyResult)
	if result.Error == "" || fsm.State().ActiveSnapshot != "" {
		t.Fatalf("future manifest was accepted: result=%#v", result)
	}
}

func TestMetadataFSMRejectsNonConsecutiveSnapshotSequence(t *testing.T) {
	fsm := newMetadataFSM("cluster")
	fsm.state.LeaderEpoch = 1
	fsm.state.LeaderOwner = "node-a"
	fsm.state.CapabilityGate = LegacyCapabilityGate()
	fsm.state.CapabilityGate.MinimumLeaderCapability = CapabilityLevelMonotonicSnapshotSequence
	fsm.state.CapabilityGate.MinimumVoterCapability = CapabilityLevelMonotonicSnapshotSequence
	apply := func(index, sequence uint64) applyResult {
		manifest := Manifest{SchemaVersion: 1,
			Generation: Generation{ClusterID: "cluster", LeaderEpoch: 1, Sequence: sequence},
			CreatedAt:  time.Unix(int64(index), 0).UTC(), Datasets: map[string]DatasetRef{}}
		manifest.Generation.ManifestHash, _ = ComputeManifestHash(manifest)
		return fsm.Apply(&raft.Log{Index: index, Data: mustJSON(t, Command{
			ID: fmt.Sprintf("snapshot-%d", index), Type: CommandCommitSnapshot, LeaderEpoch: 1,
			Payload: mustJSON(t, commitSnapshotPayload{Manifest: manifest}), CreatedAt: time.Unix(0, 0).UTC(),
		})}).(applyResult)
	}
	if result := apply(1, 1); result.Error != "" {
		t.Fatalf("initial snapshot failed: %s", result.Error)
	}
	first := fsm.State().ActiveSnapshot
	if result := apply(2, 1); result.Error == "" {
		t.Fatal("duplicate sequence was accepted")
	}
	if fsm.State().ActiveSnapshot != first {
		t.Fatal("rejected duplicate sequence changed the active snapshot")
	}
	if result := apply(3, 2); result.Error != "" {
		t.Fatalf("consecutive snapshot failed: %s", result.Error)
	}
}

func TestMetadataFSMPreservesLegacySnapshotSequenceBeforeCapabilityActivation(t *testing.T) {
	fsm := newMetadataFSM("cluster")
	fsm.state.LeaderEpoch = 1
	fsm.state.LeaderOwner = "node-a"
	manifest := Manifest{SchemaVersion: 1,
		Generation: Generation{ClusterID: "cluster", LeaderEpoch: 1, Sequence: 123456789},
		CreatedAt:  time.Unix(1, 0).UTC(), Datasets: map[string]DatasetRef{}}
	manifest.Generation.ManifestHash, _ = ComputeManifestHash(manifest)
	result := fsm.Apply(&raft.Log{Index: 1, Data: mustJSON(t, Command{
		ID: "legacy-snapshot-sequence", Type: CommandCommitSnapshot, LeaderEpoch: 1,
		Payload: mustJSON(t, commitSnapshotPayload{Manifest: manifest}), CreatedAt: time.Unix(0, 0).UTC(),
	})}).(applyResult)
	if result.Error != "" {
		t.Fatalf("legacy sequence was rejected before capability activation: %s", result.Error)
	}
}

func mustJSON(t *testing.T, value any) []byte {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return data
}
