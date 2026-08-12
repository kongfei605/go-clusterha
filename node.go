package clusterha

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"crypto/tls"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
)

const (
	RoleDisabled      = "disabled"
	RoleStandalone    = "standalone"
	RoleFollower      = "follower"
	RoleLeaderWarming = "leader_warming"
	RoleLeader        = "leader"

	ReadinessReady    = "ready"
	ReadinessNotReady = "not_ready"
)

var (
	ErrNotBusinessLeader = errors.New("node does not hold valid business leadership")
	ErrNodeNotStarted    = errors.New("cluster node is not started")
	ErrProxyUnavailable  = errors.New("verified leader proxy is unavailable")
	errNodeUnjoined      = errors.New("cluster node is waiting to join")
)

const (
	HeaderRequestID        = "X-ClusterHA-Request-ID"
	HeaderForwardedBy      = "X-ClusterHA-Forwarded-By"
	HeaderForwardHop       = "X-ClusterHA-Forward-Hop"
	HeaderTargetGeneration = "X-ClusterHA-Target-Generation"
	HeaderLeaderEpoch      = "X-ClusterHA-Leader-Epoch"
)

type Status struct {
	Enabled                  bool             `json:"enabled"`
	Ready                    bool             `json:"ready"`
	Readiness                string           `json:"readiness"`
	Reason                   string           `json:"reason,omitempty"`
	ClusterID                string           `json:"cluster_id,omitempty"`
	NodeID                   string           `json:"node_id,omitempty"`
	Role                     string           `json:"role"`
	LeaderID                 string           `json:"leader_id,omitempty"`
	LeaderAddress            string           `json:"leader_address,omitempty"`
	LeaderEpoch              uint64           `json:"leader_epoch"`
	Term                     uint64           `json:"term"`
	CommitIndex              uint64           `json:"commit_index"`
	AppliedIndex             uint64           `json:"applied_index"`
	HasQuorum                bool             `json:"has_quorum"`
	QuorumVerifiedAt         time.Time        `json:"quorum_verified_at,omitempty"`
	LastLeaderContactAt      time.Time        `json:"last_leader_contact_at,omitempty"`
	ReadConsistency          string           `json:"read_consistency,omitempty"`
	EmergencyStaleRead       bool             `json:"emergency_stale_read"`
	MaxLeaderContactAge      string           `json:"max_leader_contact_age,omitempty"`
	MaxQuorumVerificationAge string           `json:"max_quorum_verification_age,omitempty"`
	CommittedGeneration      Generation       `json:"committed_generation"`
	CommittedGenerationIndex uint64           `json:"committed_generation_index"`
	AvailableGenerations     []Generation     `json:"available_generations,omitempty"`
	ProtocolVersion          uint32           `json:"protocol_version"`
	CommandVersion           uint32           `json:"command_version"`
	Capabilities             NodeCapabilities `json:"capabilities"`
	ActiveCapabilityGate     CapabilityGate   `json:"active_capability_gate"`
	LeaderEligible           bool             `json:"leader_eligible"`
}

const membershipOperationMetadataKey = "cluster/membership_operation"

const (
	MembershipOperationJoin   = "join"
	MembershipOperationRemove = "remove"
	MembershipPhasePrepared   = "prepared"
	MembershipPhaseNonVoter   = "non_voter"
	MembershipPhaseCaughtUp   = "caught_up"
	MembershipPhaseVoter      = "voter"
	MembershipPhaseRemoved    = "removed"
	MembershipPhaseCompleted  = "completed"
)

type MembershipOperation struct {
	ID           string           `json:"id"`
	Type         string           `json:"type"`
	Phase        string           `json:"phase"`
	Member       Member           `json:"member"`
	Capabilities NodeCapabilities `json:"capabilities,omitempty"`
	CreatedAt    time.Time        `json:"created_at"`
	UpdatedAt    time.Time        `json:"updated_at"`
}

type LeadershipEvent struct {
	Active bool   `json:"active"`
	Epoch  uint64 `json:"epoch"`
	Reason string `json:"reason,omitempty"`
}

type Node struct {
	mu               sync.RWMutex
	cfg              Config
	status           Status
	raft             *raft.Raft
	fsm              *metadataFSM
	transport        *raft.NetworkTransport
	store            *raftboltdb.BoltStore
	blobStore        *BlobStore
	internalServer   *http.Server
	internalListener net.Listener
	cancel           context.CancelFunc
	done             chan struct{}
	started          bool
	unjoined         bool
	subs             map[chan LeadershipEvent]struct{}
	leadership       LeadershipEvent
	servingReady     func() (bool, string)
	proxyHandler     http.Handler
	stagingBlobs     map[string]int
	peers            *peerRegistry
	pendingMembers   map[string]Member
	blobPutSem       chan struct{}
	configurationMu  sync.Mutex
	membershipMu     sync.Mutex
}

func NewNode(cfg Config) (*Node, error) {
	NormalizeConfig(&cfg)
	if err := ValidateConfig(cfg); err != nil {
		return nil, err
	}
	node := &Node{cfg: cfg, done: make(chan struct{}), subs: make(map[chan LeadershipEvent]struct{}),
		stagingBlobs: make(map[string]int), pendingMembers: make(map[string]Member), blobPutSem: make(chan struct{}, cfg.MaxSnapshotBlobPuts)}
	if !cfg.Enabled {
		node.status = Status{Enabled: false, Ready: true, Readiness: ReadinessReady, Role: RoleDisabled,
			Reason: "cluster disabled; running existing single-instance mode"}
		return node, nil
	}
	members, _ := ParseInitialMembers(cfg.InitialMembers)
	node.peers = newPeerRegistry(members)
	node.status = Status{
		Enabled: true, Ready: false, Readiness: ReadinessNotReady, Reason: "cluster runtime is starting",
		ClusterID: cfg.ClusterID, NodeID: cfg.NodeID, Role: RoleFollower,
		ReadConsistency: cfg.ReadConsistency, EmergencyStaleRead: cfg.EmergencyStaleRead,
		MaxLeaderContactAge:      time.Duration(cfg.MaxLeaderContactAge).String(),
		MaxQuorumVerificationAge: time.Duration(cfg.MaxQuorumVerificationAge).String(),
	}
	return node, nil
}

func (n *Node) Start(parent context.Context) error {
	n.mu.Lock()
	if n.started {
		n.mu.Unlock()
		return nil
	}
	n.started = true
	if !n.cfg.Enabled {
		close(n.done)
		n.mu.Unlock()
		return nil
	}
	ctx, cancel := context.WithCancel(parent)
	n.cancel = cancel
	n.mu.Unlock()

	err := n.startRaft()
	unjoined := errors.Is(err, errNodeUnjoined)
	if err != nil && !unjoined {
		cancel()
		n.mu.Lock()
		n.started = false
		n.status.Reason = err.Error()
		n.mu.Unlock()
		return err
	}
	if err := n.startInternalServer(); err != nil {
		_ = n.closeRuntime()
		cancel()
		n.mu.Lock()
		n.started = false
		n.status.Reason = err.Error()
		n.mu.Unlock()
		return err
	}
	if unjoined {
		n.updateUnjoinedStatus()
	}
	go n.monitor(ctx)
	go n.watchRaftLeadership(ctx)
	go n.snapshotMaintenance(ctx)
	go n.membershipMaintenance(ctx)
	return nil
}

func (n *Node) startRaft() error {
	raftDir := filepath.Join(n.cfg.DataDir, "raft")
	if err := os.MkdirAll(filepath.Join(raftDir, "snapshots"), 0700); err != nil {
		return fmt.Errorf("create raft data directory: %w", err)
	}
	store, err := raftboltdb.NewBoltStore(filepath.Join(raftDir, "raft.db"))
	if err != nil {
		return fmt.Errorf("open raft BoltDB: %w", err)
	}
	snapshots, err := raft.NewFileSnapshotStore(filepath.Join(raftDir, "snapshots"), 3, os.Stderr)
	if err != nil {
		_ = store.Close()
		return fmt.Errorf("create raft snapshot store: %w", err)
	}
	layer, err := newTLSStreamLayer(n.cfg, n.peers)
	if err != nil {
		_ = store.Close()
		return err
	}
	transport := raft.NewNetworkTransportWithConfig(&raft.NetworkTransportConfig{
		Stream: layer, MaxPool: 3, Timeout: 5 * time.Second,
	})
	configuration, err := n.bootstrapConfiguration()
	if err != nil {
		transport.Close()
		_ = store.Close()
		return err
	}
	config := raft.DefaultConfig()
	config.LocalID = raft.ServerID(n.cfg.NodeID)
	config.LogLevel = "warn"
	config.LeaderLeaseTimeout = minDuration(config.LeaderLeaseTimeout, time.Duration(n.cfg.MaxQuorumVerificationAge))

	existing, err := raft.HasExistingState(store, store, snapshots)
	if err != nil {
		transport.Close()
		_ = store.Close()
		return fmt.Errorf("inspect raft state: %w", err)
	}
	if !existing {
		if n.cfg.JoinExisting {
			n.mu.Lock()
			n.unjoined = true
			n.mu.Unlock()
		} else if err := raft.BootstrapCluster(config, store, store, snapshots, transport, configuration); err != nil && !errors.Is(err, raft.ErrCantBootstrap) {
			transport.Close()
			_ = store.Close()
			return fmt.Errorf("bootstrap raft cluster: %w", err)
		}
	}
	fsm := newMetadataFSM(n.cfg.ClusterID)
	r, err := raft.NewRaft(config, fsm, store, store, snapshots, transport)
	if err != nil {
		transport.Close()
		_ = store.Close()
		return fmt.Errorf("start raft: %w", err)
	}
	n.mu.Lock()
	n.raft = r
	n.fsm = fsm
	n.transport = transport
	n.store = store
	n.blobStore = NewBlobStore(filepath.Join(n.cfg.DataDir, "cas"))
	n.mu.Unlock()
	if !existing && n.cfg.JoinExisting {
		return errNodeUnjoined
	}
	return nil
}

func (n *Node) bootstrapConfiguration() (raft.Configuration, error) {
	members, err := ParseInitialMembers(n.cfg.InitialMembers)
	if err != nil {
		return raft.Configuration{}, err
	}
	servers := make([]raft.Server, 0, len(members))
	for _, member := range members {
		servers = append(servers, raft.Server{Suffrage: raft.Voter, ID: raft.ServerID(member.NodeID), Address: raft.ServerAddress(member.Addr)})
	}
	return raft.Configuration{Servers: servers}, nil
}

func (n *Node) monitor(ctx context.Context) {
	defer close(n.done)
	tickerInterval := time.Duration(n.cfg.MaxQuorumVerificationAge) / 3
	if tickerInterval < 100*time.Millisecond {
		tickerInterval = 100 * time.Millisecond
	}
	ticker := time.NewTicker(tickerInterval)
	defer ticker.Stop()
	n.refreshStatus(false)
	for {
		select {
		case <-ctx.Done():
			n.setLeadership(false, 0, "cluster node stopped")
			return
		case <-ticker.C:
			n.refreshStatus(true)
		}
	}
}

func (n *Node) membershipMaintenance(ctx context.Context) {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if n.HasBusinessLeadership() {
				_ = n.reconcileMembershipOperation(ctx)
			}
		}
	}
}

func (n *Node) watchRaftLeadership(ctx context.Context) {
	n.mu.RLock()
	r := n.raft
	n.mu.RUnlock()
	if r == nil {
		return
	}
	for {
		select {
		case <-ctx.Done():
			return
		case isLeader, ok := <-r.LeaderCh():
			if !ok {
				return
			}
			if !isLeader {
				n.updateCachedLeadership(false, 0, "raft leadership lost")
				n.setLeadership(false, 0, "raft leadership lost")
			}
		}
	}
}

func (n *Node) refreshStatus(verify bool) {
	n.refreshPeerRegistry()
	n.mu.RLock()
	r := n.raft
	fsm := n.fsm
	n.mu.RUnlock()
	if r == nil || fsm == nil {
		if n.isUnjoined() {
			n.updateUnjoinedStatus()
		}
		return
	}
	stats := r.Stats()
	leaderAddr, leaderID := r.LeaderWithID()
	if n.isUnjoined() && n.raftConfigurationContainsSelf(r) {
		n.mu.Lock()
		n.unjoined = false
		n.mu.Unlock()
	}
	state := fsm.State()
	now := time.Now()
	role := RoleFollower
	hasQuorum := false
	var verifiedAt time.Time
	if r.State() == raft.Leader {
		role = RoleLeaderWarming
		if !SupportedNodeCapabilities().SupportsGate(state.CapabilityGate, true) {
			reason := "node does not satisfy the active minimum leader capability"
			n.updateStatus(role, leaderID, leaderAddr, state, stats, false, time.Time{}, reason, now)
			n.setLeadership(false, state.LeaderEpoch, reason)
			go func() { _ = r.LeadershipTransfer().Error() }()
			return
		}
		if state.LeaderOwner != n.cfg.NodeID || state.LastLeadershipTerm != parseUint(stats["term"]) {
			if err := n.acquireLeadership(parseUint(stats["term"])); err != nil {
				n.updateStatus(role, leaderID, leaderAddr, state, stats, false, time.Time{}, err.Error(), now)
				n.setLeadership(false, 0, err.Error())
				return
			}
			state = fsm.State()
		}
		if verify {
			err := r.VerifyLeader().Error()
			if err == nil {
				err = r.Barrier(time.Duration(n.cfg.MaxQuorumVerificationAge)).Error()
			}
			if err == nil {
				hasQuorum = true
				verifiedAt = now
			} else {
				n.updateStatus(role, leaderID, leaderAddr, state, stats, false, time.Time{}, err.Error(), now)
				n.setLeadership(false, state.LeaderEpoch, err.Error())
				return
			}
		} else {
			verifiedAt = n.Status().QuorumVerifiedAt
			hasQuorum = !verifiedAt.IsZero() && now.Sub(verifiedAt) <= time.Duration(n.cfg.MaxQuorumVerificationAge)
		}
		if hasQuorum && state.LeaderOwner == n.cfg.NodeID {
			role = RoleLeader
		}
	}
	reason := ""
	ready := leaderID != ""
	if !ready {
		reason = "raft leader is unknown"
	}
	n.updateStatus(role, leaderID, leaderAddr, state, stats, hasQuorum, verifiedAt, reason, now)
	n.setLeadership(role == RoleLeader && hasQuorum, state.LeaderEpoch, reason)
}

func (n *Node) isUnjoined() bool {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.unjoined
}

func (n *Node) updateUnjoinedStatus() {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.status.Role = RoleFollower
	n.status.Ready = false
	n.status.Readiness = ReadinessNotReady
	n.status.Reason = "node is waiting for cluster join"
	n.status.LeaderID = ""
	n.status.LeaderAddress = ""
	n.status.HasQuorum = false
	n.status.ProtocolVersion = CurrentProtocolVersion
	n.status.CommandVersion = CurrentCommandVersion
	n.status.Capabilities = SupportedNodeCapabilities()
	n.status.ActiveCapabilityGate = LegacyCapabilityGate()
	n.status.LeaderEligible = false
}

func (n *Node) raftConfigurationContainsSelf(r *raft.Raft) bool {
	future := r.GetConfiguration()
	if err := future.Error(); err != nil {
		return false
	}
	for _, server := range future.Configuration().Servers {
		if server.ID == raft.ServerID(n.cfg.NodeID) {
			return true
		}
	}
	return false
}

func (n *Node) acquireLeadership(term uint64) error {
	if term == 0 {
		return errors.New("raft term is unavailable")
	}
	payload, _ := json.Marshal(acquireLeadershipPayload{NodeID: n.cfg.NodeID, Term: term})
	protocolVersion, commandVersion := n.activeCommandVersions()
	command := Command{ID: fmt.Sprintf("acquire/%d/%s", term, n.cfg.NodeID), Type: CommandAcquireLeadership,
		ProtocolVersion: protocolVersion, CommandVersion: commandVersion, Payload: payload, CreatedAt: time.Unix(0, 0).UTC()}
	if _, err := n.apply(command, 5*time.Second); err != nil {
		return err
	}
	state := n.Metadata()
	if _, ok := state.Values["cluster/members"]; ok {
		return nil
	}
	members := make(map[string]Member)
	initial, _ := ParseInitialMembers(n.cfg.InitialMembers)
	for _, member := range initial {
		members[member.NodeID] = member
	}
	value, _ := json.Marshal(members)
	payload, _ = json.Marshal(putMetadataPayload{Key: "cluster/members", Value: value})
	command = Command{ID: "initialize/cluster-members/v1", Type: CommandPutMetadata, ExpectedRevision: &state.Revision,
		ProtocolVersion: protocolVersion, CommandVersion: commandVersion,
		LeaderEpoch: state.LeaderEpoch, Payload: payload, CreatedAt: time.Unix(0, 0).UTC()}
	_, err := n.apply(command, 5*time.Second)
	return err
}

func (n *Node) apply(command Command, timeout time.Duration) (applyResult, error) {
	data, err := json.Marshal(command)
	if err != nil {
		return applyResult{}, err
	}
	n.mu.RLock()
	r := n.raft
	n.mu.RUnlock()
	if r == nil {
		return applyResult{}, ErrNodeNotStarted
	}
	future := r.Apply(data, timeout)
	if err := future.Error(); err != nil {
		return applyResult{}, err
	}
	result, ok := future.Response().(applyResult)
	if !ok {
		return applyResult{}, fmt.Errorf("unexpected FSM response %T", future.Response())
	}
	if result.Error != "" {
		return result, errors.New(result.Error)
	}
	return result, nil
}

func (n *Node) activeCommandVersions() (uint32, uint32) {
	gate := normalizeCapabilityGate(n.Metadata().CapabilityGate)
	return gate.ProtocolVersion, gate.CommandVersion
}

func (n *Node) PutMetadata(ctx context.Context, commandID, key string, value any, expectedRevision uint64) (uint64, error) {
	if err := n.VerifyBusinessLeadership(ctx); err != nil {
		return 0, err
	}
	rawValue, err := json.Marshal(value)
	if err != nil {
		return 0, err
	}
	payload, _ := json.Marshal(putMetadataPayload{Key: key, Value: rawValue})
	state := n.Metadata()
	protocolVersion, commandVersion := n.activeCommandVersions()
	command := Command{ID: commandID, Type: CommandPutMetadata, ExpectedRevision: &expectedRevision,
		ProtocolVersion: protocolVersion, CommandVersion: commandVersion,
		LeaderEpoch: state.LeaderEpoch, Payload: payload, CreatedAt: time.Unix(0, 0).UTC()}
	result, err := n.apply(command, 5*time.Second)
	return result.Revision, err
}

func (n *Node) PutMetadataBatch(ctx context.Context, commandID string, values map[string]any, expectedRevision uint64) (uint64, error) {
	if err := n.VerifyBusinessLeadership(ctx); err != nil {
		return 0, err
	}
	rawValues := make(map[string]json.RawMessage, len(values))
	for key, value := range values {
		rawValue, err := json.Marshal(value)
		if err != nil {
			return 0, err
		}
		rawValues[key] = rawValue
	}
	payload, _ := json.Marshal(putMetadataBatchPayload{Values: rawValues})
	state := n.Metadata()
	protocolVersion, commandVersion := n.activeCommandVersions()
	command := Command{ID: commandID, Type: CommandPutMetadataBatch, ExpectedRevision: &expectedRevision,
		ProtocolVersion: protocolVersion, CommandVersion: commandVersion,
		LeaderEpoch: state.LeaderEpoch, Payload: payload, CreatedAt: time.Unix(0, 0).UTC()}
	result, err := n.apply(command, 5*time.Second)
	return result.Revision, err
}

func (n *Node) ActivateCapabilities(ctx context.Context, commandID string, gate CapabilityGate) (uint64, error) {
	if err := n.VerifyBusinessLeadership(ctx); err != nil {
		return 0, err
	}
	gate = normalizeCapabilityGate(gate)
	if err := validateCapabilityGate(gate); err != nil {
		return 0, err
	}
	voters, err := n.voterStatuses(ctx)
	if err != nil {
		return 0, err
	}
	for nodeID, capabilities := range voters {
		if !capabilities.SupportsGate(gate, false) {
			return 0, fmt.Errorf("voter %q does not support requested capability gate", nodeID)
		}
	}
	payload, _ := json.Marshal(activateCapabilitiesPayload{Gate: gate})
	state := n.Metadata()
	protocolVersion, commandVersion := n.activeCommandVersions()
	command := Command{ID: commandID, Type: CommandActivateCapabilities, ExpectedRevision: &state.Revision,
		ProtocolVersion: protocolVersion, CommandVersion: commandVersion, LeaderEpoch: state.LeaderEpoch,
		Payload: payload, CreatedAt: time.Now().UTC()}
	result, err := n.apply(command, 5*time.Second)
	return result.Revision, err
}

func (n *Node) voterStatuses(ctx context.Context) (map[string]NodeCapabilities, error) {
	_, voters, err := n.voterConfiguration()
	if err != nil {
		return nil, err
	}
	result := make(map[string]NodeCapabilities, len(voters))
	client, err := n.internalHTTPClient()
	if err != nil {
		return nil, err
	}
	for _, voter := range voters {
		nodeID := string(voter.ID)
		if nodeID == n.cfg.NodeID {
			result[nodeID] = SupportedNodeCapabilities()
			continue
		}
		base, ok := n.internalAPIURLForNode(nodeID)
		if !ok {
			return nil, fmt.Errorf("internal API URL for voter %q is unavailable", nodeID)
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, base+"/internal/v1/status", nil)
		if err != nil {
			return nil, err
		}
		resp, err := client.Do(req)
		if err != nil {
			return nil, fmt.Errorf("read voter %q capabilities: %w", nodeID, err)
		}
		var status Status
		decodeErr := json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&status)
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusOK || decodeErr != nil {
			return nil, fmt.Errorf("read voter %q capabilities returned %s: %v", nodeID, resp.Status, decodeErr)
		}
		result[nodeID] = status.Capabilities
	}
	return result, nil
}

func (n *Node) PublishSnapshot(ctx context.Context, commandID string, manifest Manifest, blobs map[string]io.Reader) (uint64, error) {
	if err := n.VerifyBusinessLeadership(ctx); err != nil {
		return 0, err
	}
	n.configurationMu.Lock()
	defer n.configurationMu.Unlock()
	n.mu.RLock()
	store := n.blobStore
	n.mu.RUnlock()
	if store == nil {
		return 0, ErrNodeNotStarted
	}
	if err := validateManifestCapability(manifest, n.Metadata().CapabilityGate); err != nil {
		return 0, err
	}
	n.retainStagingBlobs(manifest)
	defer n.releaseStagingBlobs(manifest)
	for hash, reader := range blobs {
		if err := store.PutHash(hash, reader); err != nil {
			return 0, fmt.Errorf("store local blob %s: %w", hash, err)
		}
	}
	configurationIndex, voters, err := n.voterConfiguration()
	if err != nil {
		return 0, err
	}
	manifest.VoterConfigurationIndex = configurationIndex
	hash, err := ComputeManifestHash(manifest)
	if err != nil {
		return 0, err
	}
	manifest.Generation.ManifestHash = hash
	if err := n.replicateSnapshotBlobs(ctx, manifest, voters); err != nil {
		return 0, err
	}
	currentIndex, _, err := n.voterConfiguration()
	if err != nil {
		return 0, err
	}
	if currentIndex != configurationIndex {
		return 0, fmt.Errorf("raft voter configuration changed during snapshot prepare: was %d now %d", configurationIndex, currentIndex)
	}
	payload, _ := json.Marshal(commitSnapshotPayload{Manifest: manifest})
	state := n.Metadata()
	protocolVersion, commandVersion := n.activeCommandVersions()
	command := Command{ID: commandID, Type: CommandCommitSnapshot, ExpectedRevision: &state.Revision,
		ProtocolVersion: protocolVersion, CommandVersion: commandVersion,
		LeaderEpoch: state.LeaderEpoch, Payload: payload, CreatedAt: time.Unix(0, 0).UTC()}
	result, err := n.apply(command, 5*time.Second)
	return result.Revision, err
}

func (n *Node) ActiveSnapshot() (Manifest, bool) {
	state := n.Metadata()
	if state.ActiveSnapshot == "" {
		return Manifest{}, false
	}
	manifest, ok := state.Snapshots[state.ActiveSnapshot]
	return cloneManifest(manifest), ok
}

func (n *Node) Snapshot(generation Generation) (Manifest, bool) {
	manifest, ok := n.Metadata().Snapshots[generation.ManifestHash]
	return cloneManifest(manifest), ok && manifest.Generation.Equal(generation)
}

func (n *Node) OpenSnapshotDataset(name string) (io.ReadCloser, DatasetRef, error) {
	manifest, ok := n.ActiveSnapshot()
	if !ok {
		return nil, DatasetRef{}, os.ErrNotExist
	}
	ref, ok := manifest.Datasets[name]
	if !ok {
		return nil, DatasetRef{}, os.ErrNotExist
	}
	n.mu.RLock()
	store := n.blobStore
	n.mu.RUnlock()
	if store == nil {
		return nil, DatasetRef{}, ErrNodeNotStarted
	}
	file, err := store.Open(ref.BlobHash)
	if err != nil {
		return nil, DatasetRef{}, err
	}
	return file, ref, nil
}

func (n *Node) OpenSnapshotDatasetAt(generation Generation, name string) (io.ReadCloser, DatasetRef, error) {
	state := n.Metadata()
	manifest, ok := state.Snapshots[generation.ManifestHash]
	if !ok || !manifest.Generation.Equal(generation) {
		return nil, DatasetRef{}, os.ErrNotExist
	}
	ref, ok := manifest.Datasets[name]
	if !ok {
		return nil, DatasetRef{}, os.ErrNotExist
	}
	n.mu.RLock()
	store := n.blobStore
	n.mu.RUnlock()
	if store == nil {
		return nil, DatasetRef{}, ErrNodeNotStarted
	}
	file, err := store.Open(ref.BlobHash)
	return file, ref, err
}

func (n *Node) ParseGeneration(value string) (Generation, error) {
	parts := strings.SplitN(value, "/", 4)
	if len(parts) != 4 {
		return Generation{}, fmt.Errorf("invalid generation %q", value)
	}
	epoch, err := strconv.ParseUint(parts[1], 10, 64)
	if err != nil {
		return Generation{}, fmt.Errorf("invalid generation epoch: %w", err)
	}
	sequence, err := strconv.ParseUint(parts[2], 10, 64)
	if err != nil {
		return Generation{}, fmt.Errorf("invalid generation sequence: %w", err)
	}
	generation := Generation{ClusterID: parts[0], LeaderEpoch: epoch, Sequence: sequence, ManifestHash: parts[3]}
	if generation.ClusterID != n.cfg.ClusterID || !strings.HasPrefix(generation.ManifestHash, HashPrefixSHA256) {
		return Generation{}, fmt.Errorf("generation is not valid for this cluster")
	}
	return generation, nil
}

func (n *Node) replicateSnapshotBlobs(ctx context.Context, manifest Manifest, voters []raft.Server) error {
	quorum := len(voters)/2 + 1
	acks := 0
	for _, server := range voters {
		if string(server.ID) == n.cfg.NodeID {
			if err := n.verifyManifestBlobs(manifest); err != nil {
				return err
			}
			acks++
			continue
		}
		if err := n.replicateManifestToPeer(ctx, string(server.ID), manifest); err != nil {
			noteReplicationFailure(string(server.ID), err)
			continue
		}
		acks++
	}
	if acks < quorum {
		return fmt.Errorf("snapshot durable ACK quorum not reached: got %d want %d voters=%d", acks, quorum, len(voters))
	}
	return nil
}

func (n *Node) voterConfiguration() (uint64, []raft.Server, error) {
	n.mu.RLock()
	r := n.raft
	n.mu.RUnlock()
	if r == nil {
		return 0, nil, ErrNodeNotStarted
	}
	future := r.GetConfiguration()
	if err := future.Error(); err != nil {
		return 0, nil, err
	}
	voters := make([]raft.Server, 0, len(future.Configuration().Servers))
	for _, server := range future.Configuration().Servers {
		if server.Suffrage == raft.Voter {
			voters = append(voters, server)
		}
	}
	if len(voters) == 0 {
		return 0, nil, errors.New("raft configuration has no voters")
	}
	return future.Index(), voters, nil
}

func (n *Node) voterServers() ([]raft.Server, error) {
	_, voters, err := n.voterConfiguration()
	return voters, err
}

func (n *Node) snapshotMaintenance(ctx context.Context) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		n.syncActiveSnapshot(ctx)
		n.gcSnapshotBlobs()
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (n *Node) syncActiveSnapshot(ctx context.Context) {
	manifest, ok := n.ActiveSnapshot()
	if !ok || n.verifyManifestBlobs(manifest) == nil {
		return
	}
	voters, err := n.voterServers()
	if err != nil {
		return
	}
	for _, dataset := range manifest.Datasets {
		n.mu.RLock()
		store := n.blobStore
		n.mu.RUnlock()
		if store == nil || store.Verify(dataset.BlobHash) == nil {
			continue
		}
		for _, server := range voters {
			nodeID := string(server.ID)
			if nodeID == n.cfg.NodeID {
				continue
			}
			if err := n.pullBlobFromPeer(ctx, nodeID, dataset.BlobHash); err == nil {
				break
			}
		}
	}
}

func (n *Node) pullBlobFromPeer(ctx context.Context, nodeID, hash string) error {
	base, ok := n.internalAPIURLForNode(nodeID)
	if !ok {
		return fmt.Errorf("internal API address for node %q is not configured", nodeID)
	}
	client, err := n.internalHTTPClient()
	if err != nil {
		return err
	}
	target := strings.TrimRight(base, "/") + "/internal/v1/blobs/" + url.PathEscape(hash)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
	if err != nil {
		return err
	}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("pull blob %s from node %s returned %s", hash, nodeID, resp.Status)
	}
	n.mu.RLock()
	store := n.blobStore
	n.mu.RUnlock()
	if store == nil {
		return ErrNodeNotStarted
	}
	return store.PutHash(hash, resp.Body)
}

func (n *Node) gcSnapshotBlobs() {
	state := n.Metadata()
	manifests := make([]Manifest, 0, len(state.Snapshots))
	for _, manifest := range state.Snapshots {
		manifests = append(manifests, manifest)
	}
	sort.Slice(manifests, func(i, j int) bool { return manifests[i].CreatedAt.After(manifests[j].CreatedAt) })
	retained := make(map[string]struct{})
	n.mu.RLock()
	for hash := range n.stagingBlobs {
		retained[hash] = struct{}{}
	}
	n.mu.RUnlock()
	pinCutoff := time.Now().Add(-time.Duration(n.cfg.GenerationPinTTL))
	for index, manifest := range manifests {
		if index >= n.cfg.GenerationRetention && manifest.CreatedAt.Before(pinCutoff) && manifest.Generation.ManifestHash != state.ActiveSnapshot {
			continue
		}
		for _, dataset := range manifest.Datasets {
			retained[dataset.BlobHash] = struct{}{}
			if dataset.PreviousBlobHash != "" {
				retained[dataset.PreviousBlobHash] = struct{}{}
			}
		}
	}
	n.mu.RLock()
	store := n.blobStore
	n.mu.RUnlock()
	if store != nil {
		_, _ = store.GC(retained)
	}
}

func (n *Node) retainStagingBlobs(manifest Manifest) {
	n.mu.Lock()
	defer n.mu.Unlock()
	for _, dataset := range manifest.Datasets {
		n.stagingBlobs[dataset.BlobHash]++
	}
}

func (n *Node) releaseStagingBlobs(manifest Manifest) {
	n.mu.Lock()
	defer n.mu.Unlock()
	for _, dataset := range manifest.Datasets {
		if n.stagingBlobs[dataset.BlobHash] <= 1 {
			delete(n.stagingBlobs, dataset.BlobHash)
		} else {
			n.stagingBlobs[dataset.BlobHash]--
		}
	}
}

func (n *Node) isCommittedBlob(hash string) bool {
	state := n.Metadata()
	for _, manifest := range state.Snapshots {
		for _, dataset := range manifest.Datasets {
			if dataset.BlobHash == hash || dataset.PreviousBlobHash == hash {
				return true
			}
		}
	}
	return false
}

func (n *Node) verifyManifestBlobs(manifest Manifest) error {
	n.mu.RLock()
	store := n.blobStore
	n.mu.RUnlock()
	if store == nil {
		return ErrNodeNotStarted
	}
	for _, dataset := range manifest.Datasets {
		if err := store.Verify(dataset.BlobHash); err != nil {
			return fmt.Errorf("verify local blob %s: %w", dataset.BlobHash, err)
		}
	}
	return nil
}

func (n *Node) replicateManifestToPeer(ctx context.Context, nodeID string, manifest Manifest) error {
	base, ok := n.internalAPIURLForNode(nodeID)
	if !ok {
		return fmt.Errorf("internal API address for node %q is not configured", nodeID)
	}
	client, err := n.internalHTTPClient()
	if err != nil {
		return err
	}
	for _, dataset := range manifest.Datasets {
		if err := n.putBlobToPeer(ctx, client, base, dataset.BlobHash); err != nil {
			return err
		}
	}
	return nil
}

func (n *Node) putBlobToPeer(ctx context.Context, client *http.Client, base, hash string) error {
	n.mu.RLock()
	store := n.blobStore
	n.mu.RUnlock()
	if store == nil {
		return ErrNodeNotStarted
	}
	file, err := store.Open(hash)
	if err != nil {
		return err
	}
	defer file.Close()
	target := strings.TrimRight(base, "/") + "/internal/v1/blobs/" + url.PathEscape(hash)
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, target, file)
	if err != nil {
		return err
	}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		return fmt.Errorf("replicate blob %s to %s returned %s: %s", hash, target, resp.Status, string(body))
	}
	return nil
}

func (n *Node) internalAPIURLForNode(nodeID string) (string, bool) {
	if nodeID == n.cfg.NodeID {
		return strings.TrimRight(n.cfg.InternalAPIAdvertiseAddr, "/"), true
	}
	if member, ok := n.peers.Member(nodeID); ok && member.InternalAPIURL != "" {
		return strings.TrimRight(member.InternalAPIURL, "/"), true
	}
	return "", false
}

func (n *Node) AddVoter(context.Context, Member) error {
	return errors.New("safe dynamic membership is not implemented; start replacements with join_existing=true and use JoinVoter")
}

func (n *Node) RemoveServer(ctx context.Context, nodeID string) error {
	if err := n.VerifyBusinessLeadership(ctx); err != nil {
		return err
	}
	if nodeID == "" {
		return errors.New("node_id is required")
	}
	if nodeID == n.cfg.NodeID {
		return errors.New("transfer leadership before removing the current leader")
	}
	if operation, ok, err := n.membershipOperation(); err != nil {
		return err
	} else if ok && operation.Phase != MembershipPhaseCompleted {
		if operation.Type != MembershipOperationRemove || operation.Member.NodeID != nodeID {
			return fmt.Errorf("membership operation %q for node %q is already active", operation.Type, operation.Member.NodeID)
		}
		return n.reconcileMembershipOperation(ctx)
	}
	members := n.committedMembers()
	member, ok := members[nodeID]
	if !ok {
		return nil
	}
	if len(members) <= n.cfg.BootstrapExpect {
		return fmt.Errorf("removal would reduce committed membership below %d voters", n.cfg.BootstrapExpect)
	}
	op := MembershipOperation{ID: fmt.Sprintf("remove/%s/%d", nodeID, time.Now().UnixNano()), Type: MembershipOperationRemove,
		Phase: MembershipPhasePrepared, Member: member, CreatedAt: time.Now().UTC(), UpdatedAt: time.Now().UTC()}
	if err := n.beginMembershipOperation(ctx, op); err != nil {
		return err
	}
	return n.reconcileMembershipOperation(ctx)
}

func (n *Node) JoinVoter(ctx context.Context, member Member) error {
	if err := n.VerifyBusinessLeadership(ctx); err != nil {
		return err
	}
	if member.NodeID == "" || member.Addr == "" || member.InternalAPIURL == "" {
		return errors.New("node_id, raft address and internal API URL are required")
	}
	if member.NodeID == n.cfg.NodeID {
		return errors.New("cannot join the current leader")
	}
	if operation, ok, err := n.membershipOperation(); err != nil {
		return err
	} else if ok && operation.Phase != MembershipPhaseCompleted {
		if operation.Type != MembershipOperationJoin || operation.Member != member {
			return fmt.Errorf("membership operation %q for node %q is already active", operation.Type, operation.Member.NodeID)
		}
		n.addPendingMember(member)
		n.refreshPeerRegistry()
		return n.reconcileMembershipOperation(ctx)
	}
	if existing, ok := n.committedMembers()[member.NodeID]; ok {
		if existing == member {
			return nil
		}
		return fmt.Errorf("node %q is already in committed membership with different addresses", member.NodeID)
	}
	n.addPendingMember(member)
	n.refreshPeerRegistry()
	capabilities, err := n.prepareJoiningMember(ctx, member)
	if err != nil {
		n.removePendingMember(member.NodeID)
		n.refreshPeerRegistry()
		return err
	}
	gate := n.Metadata().CapabilityGate
	if !capabilities.SupportsGate(gate, false) {
		n.removePendingMember(member.NodeID)
		n.refreshPeerRegistry()
		return fmt.Errorf("node %q does not support the active voter capability gate", member.NodeID)
	}
	op := MembershipOperation{ID: fmt.Sprintf("join/%s/%d", member.NodeID, time.Now().UnixNano()), Type: MembershipOperationJoin,
		Phase: MembershipPhasePrepared, Member: member, Capabilities: capabilities, CreatedAt: time.Now().UTC(), UpdatedAt: time.Now().UTC()}
	if err := n.beginMembershipOperation(ctx, op); err != nil {
		n.removePendingMember(member.NodeID)
		n.refreshPeerRegistry()
		return err
	}
	return n.reconcileMembershipOperation(ctx)
}

func (n *Node) prepareJoiningMember(ctx context.Context, member Member) (NodeCapabilities, error) {
	base := strings.TrimRight(member.InternalAPIURL, "/")
	client, err := n.internalHTTPClient()
	if err != nil {
		return NodeCapabilities{}, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, base+"/internal/v1/join/prepare", nil)
	if err != nil {
		return NodeCapabilities{}, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return NodeCapabilities{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		return NodeCapabilities{}, fmt.Errorf("join prepare for node %s returned %s: %s", member.NodeID, resp.Status, string(body))
	}
	var prepared struct {
		ClusterID          string           `json:"cluster_id"`
		NodeID             string           `json:"node_id"`
		RaftAdvertiseAddr  string           `json:"raft_advertise_addr"`
		InternalAPIURL     string           `json:"internal_api_url"`
		JoinExisting       bool             `json:"join_existing"`
		InitialMemberCount int              `json:"initial_member_count"`
		BootstrapExpect    int              `json:"bootstrap_expectation"`
		Capabilities       NodeCapabilities `json:"capabilities"`
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&prepared); err != nil {
		return NodeCapabilities{}, err
	}
	if prepared.ClusterID != n.cfg.ClusterID || prepared.NodeID != member.NodeID || prepared.RaftAdvertiseAddr != member.Addr ||
		prepared.InternalAPIURL != member.InternalAPIURL || !prepared.JoinExisting || prepared.InitialMemberCount != n.cfg.BootstrapExpect ||
		prepared.BootstrapExpect != n.cfg.BootstrapExpect {
		return NodeCapabilities{}, fmt.Errorf("join prepare response for node %s does not match requested membership", member.NodeID)
	}
	return prepared.Capabilities, nil
}

func (n *Node) beginMembershipOperation(ctx context.Context, operation MembershipOperation) error {
	if existing, ok, err := n.membershipOperation(); err != nil {
		return err
	} else if ok && existing.Phase != MembershipPhaseCompleted {
		if existing.Type == operation.Type && existing.Member == operation.Member {
			return nil
		}
		return fmt.Errorf("membership operation %q for node %q is already active", existing.Type, existing.Member.NodeID)
	}
	state := n.Metadata()
	_, err := n.PutMetadata(ctx, operation.ID+"/prepared", membershipOperationMetadataKey, operation, state.Revision)
	return err
}

func (n *Node) membershipOperation() (MembershipOperation, bool, error) {
	raw, ok := n.Metadata().Values[membershipOperationMetadataKey]
	if !ok || len(raw) == 0 || string(raw) == "null" {
		return MembershipOperation{}, false, nil
	}
	var operation MembershipOperation
	if err := json.Unmarshal(raw, &operation); err != nil {
		return MembershipOperation{}, false, fmt.Errorf("decode membership operation: %w", err)
	}
	return operation, true, nil
}

func (n *Node) reconcileMembershipOperation(ctx context.Context) error {
	n.membershipMu.Lock()
	defer n.membershipMu.Unlock()
	if err := n.VerifyBusinessLeadership(ctx); err != nil {
		return err
	}
	operation, ok, err := n.membershipOperation()
	if err != nil || !ok || operation.Phase == MembershipPhaseCompleted {
		return err
	}
	n.addPendingMember(operation.Member)
	defer func() {
		if latest, exists, _ := n.membershipOperation(); exists &&
			(latest.Phase == MembershipPhaseCompleted || latest.Type == MembershipOperationRemove && latest.Phase == MembershipPhaseRemoved) {
			n.removePendingMember(operation.Member.NodeID)
		}
		n.refreshPeerRegistry()
	}()
	n.refreshPeerRegistry()
	switch operation.Type {
	case MembershipOperationJoin:
		return n.reconcileJoinOperation(ctx, operation)
	case MembershipOperationRemove:
		return n.reconcileRemoveOperation(ctx, operation)
	default:
		return fmt.Errorf("unsupported membership operation type %q", operation.Type)
	}
}

func (n *Node) reconcileJoinOperation(ctx context.Context, operation MembershipOperation) error {
	if !operation.Capabilities.SupportsGate(n.Metadata().CapabilityGate, false) {
		return fmt.Errorf("joining node %q no longer satisfies the active capability gate", operation.Member.NodeID)
	}
	suffrage, present, err := n.raftServerSuffrage(operation.Member.NodeID)
	if err != nil {
		return err
	}
	if !present {
		if err := n.raftAddNonvoter(ctx, operation.Member); err != nil {
			return err
		}
		if err := n.updateMembershipOperation(ctx, &operation, MembershipPhaseNonVoter); err != nil {
			return err
		}
		suffrage = raft.Nonvoter
	}
	if suffrage == raft.Nonvoter {
		if err := n.waitForJoiningNodeCatchUp(ctx, operation.Member); err != nil {
			return err
		}
		if err := n.updateMembershipOperation(ctx, &operation, MembershipPhaseCaughtUp); err != nil {
			return err
		}
		if err := n.raftAddVoter(ctx, operation.Member); err != nil {
			return err
		}
	}
	if err := n.updateMembershipOperation(ctx, &operation, MembershipPhaseVoter); err != nil {
		return err
	}
	members := n.committedMembers()
	members[operation.Member.NodeID] = operation.Member
	operation.Phase = MembershipPhaseCompleted
	operation.UpdatedAt = time.Now().UTC()
	state := n.Metadata()
	_, err = n.PutMetadataBatch(ctx, operation.ID+"/completed", map[string]any{
		"cluster/members": members, membershipOperationMetadataKey: operation,
	}, state.Revision)
	return err
}

func (n *Node) reconcileRemoveOperation(ctx context.Context, operation MembershipOperation) error {
	_, present, err := n.raftServerSuffrage(operation.Member.NodeID)
	if err != nil {
		return err
	}
	if present {
		if err := n.raftRemoveServer(ctx, operation.Member.NodeID); err != nil {
			return err
		}
	}
	if err := n.updateMembershipOperation(ctx, &operation, MembershipPhaseRemoved); err != nil {
		return err
	}
	n.removePendingMember(operation.Member.NodeID)
	n.refreshPeerRegistry()
	members := n.committedMembers()
	delete(members, operation.Member.NodeID)
	operation.Phase = MembershipPhaseCompleted
	operation.UpdatedAt = time.Now().UTC()
	state := n.Metadata()
	_, err = n.PutMetadataBatch(ctx, operation.ID+"/completed", map[string]any{
		"cluster/members": members, membershipOperationMetadataKey: operation,
	}, state.Revision)
	return err
}

func (n *Node) updateMembershipOperation(ctx context.Context, operation *MembershipOperation, phase string) error {
	if operation.Phase == phase {
		return nil
	}
	operation.Phase = phase
	operation.UpdatedAt = time.Now().UTC()
	state := n.Metadata()
	_, err := n.PutMetadata(ctx, operation.ID+"/"+phase, membershipOperationMetadataKey, *operation, state.Revision)
	return err
}

func (n *Node) raftServerSuffrage(nodeID string) (raft.ServerSuffrage, bool, error) {
	n.mu.RLock()
	r := n.raft
	n.mu.RUnlock()
	if r == nil {
		return 0, false, ErrNodeNotStarted
	}
	future := r.GetConfiguration()
	if err := future.Error(); err != nil {
		return 0, false, err
	}
	for _, server := range future.Configuration().Servers {
		if server.ID == raft.ServerID(nodeID) {
			return server.Suffrage, true, nil
		}
	}
	return 0, false, nil
}

func (n *Node) raftAddNonvoter(ctx context.Context, member Member) error {
	return n.runConfigurationChange(ctx, func(r *raft.Raft) raft.IndexFuture {
		return r.AddNonvoter(raft.ServerID(member.NodeID), raft.ServerAddress(member.Addr), 0, 0)
	})
}

func (n *Node) raftAddVoter(ctx context.Context, member Member) error {
	return n.runConfigurationChange(ctx, func(r *raft.Raft) raft.IndexFuture {
		return r.AddVoter(raft.ServerID(member.NodeID), raft.ServerAddress(member.Addr), 0, 0)
	})
}

func (n *Node) raftRemoveServer(ctx context.Context, nodeID string) error {
	return n.runConfigurationChange(ctx, func(r *raft.Raft) raft.IndexFuture {
		return r.RemoveServer(raft.ServerID(nodeID), 0, 0)
	})
}

func (n *Node) runConfigurationChange(ctx context.Context, start func(*raft.Raft) raft.IndexFuture) error {
	n.configurationMu.Lock()
	n.mu.RLock()
	r := n.raft
	n.mu.RUnlock()
	if r == nil {
		n.configurationMu.Unlock()
		return ErrNodeNotStarted
	}
	done := make(chan error, 1)
	go func() { done <- start(r).Error() }()
	select {
	case <-ctx.Done():
		// Raft configuration futures cannot be canceled. Hold the serialization
		// lock until the in-flight future terminates after this caller returns.
		go func() {
			<-done
			n.configurationMu.Unlock()
		}()
		return ctx.Err()
	case err := <-done:
		n.configurationMu.Unlock()
		return err
	}
}

func (n *Node) waitForJoiningNodeCatchUp(ctx context.Context, member Member) error {
	n.mu.RLock()
	r := n.raft
	n.mu.RUnlock()
	if r == nil {
		return ErrNodeNotStarted
	}
	if err := r.Barrier(time.Duration(n.cfg.MaxQuorumVerificationAge)).Error(); err != nil {
		return err
	}
	target := parseUint(r.Stats()["commit_index"])
	client, err := n.internalHTTPClient()
	if err != nil {
		return err
	}
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		if !n.HasBusinessLeadership() {
			return ErrNotBusinessLeader
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimRight(member.InternalAPIURL, "/")+"/internal/v1/status", nil)
		if err == nil {
			if resp, requestErr := client.Do(req); requestErr == nil {
				var status Status
				decodeErr := json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&status)
				_ = resp.Body.Close()
				if resp.StatusCode == http.StatusOK && decodeErr == nil && status.AppliedIndex >= target {
					return nil
				}
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func (n *Node) TransferLeadership(ctx context.Context, nodeID string) error {
	if nodeID == "" || nodeID == n.cfg.NodeID {
		return nil
	}
	n.mu.RLock()
	r := n.raft
	n.mu.RUnlock()
	if r == nil {
		return ErrNodeNotStarted
	}
	member, ok := n.peers.Member(nodeID)
	if !ok {
		return fmt.Errorf("node %q is not in committed membership", nodeID)
	}
	done := make(chan error, 1)
	go func() {
		done <- r.LeadershipTransferToServer(raft.ServerID(nodeID), raft.ServerAddress(member.Addr)).Error()
	}()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-done:
		return err
	}
}

func (n *Node) committedMembers() map[string]Member {
	state := n.Metadata()
	var members map[string]Member
	if raw, ok := state.Values["cluster/members"]; ok && json.Unmarshal(raw, &members) == nil {
		return members
	}
	members = make(map[string]Member)
	initial, _ := ParseInitialMembers(n.cfg.InitialMembers)
	for _, member := range initial {
		members[member.NodeID] = member
	}
	return members
}

func (n *Node) addPendingMember(member Member) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.pendingMembers[member.NodeID] = member
}

func (n *Node) removePendingMember(nodeID string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	delete(n.pendingMembers, nodeID)
}

func (n *Node) refreshPeerRegistry() {
	members := n.committedMembers()
	if operation, ok, err := n.membershipOperation(); err == nil && ok && operation.Phase != MembershipPhaseCompleted {
		if operation.Type != MembershipOperationRemove || operation.Phase != MembershipPhaseRemoved {
			members[operation.Member.NodeID] = operation.Member
		}
	}
	n.mu.RLock()
	for nodeID, member := range n.pendingMembers {
		members[nodeID] = member
	}
	n.mu.RUnlock()
	list := make([]Member, 0, len(members))
	for _, member := range members {
		list = append(list, member)
	}
	n.peers.Replace(list)
}

func (n *Node) LeaderID() string { return n.Status().LeaderID }

func (n *Node) NodeID() string { return n.cfg.NodeID }

func (n *Node) internalHTTPClient() (*http.Client, error) {
	cert, roots, err := loadTLSIdentity(n.cfg.InternalTLS)
	if err != nil {
		return nil, err
	}
	return &http.Client{
		Timeout: 10 * time.Second,
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{
				MinVersion:   tls.VersionTLS12,
				Certificates: []tls.Certificate{cert},
				RootCAs:      roots,
				ServerName:   n.cfg.InternalTLS.ServerName,
			},
		},
	}, nil
}

func (n *Node) SetProxyHandler(handler http.Handler) {
	n.mu.Lock()
	n.proxyHandler = handler
	n.mu.Unlock()
}

func (n *Node) internalProxyHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get(HeaderForwardHop) != "1" || r.Header.Get(HeaderForwardedBy) == "" {
			http.Error(w, "invalid cluster proxy hop", http.StatusBadRequest)
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), time.Duration(n.cfg.MaxQuorumVerificationAge))
		defer cancel()
		if err := n.VerifyBusinessLeadership(ctx); err != nil {
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "not_leader"})
			return
		}
		n.mu.RLock()
		handler := n.proxyHandler
		n.mu.RUnlock()
		if handler == nil {
			http.Error(w, ErrProxyUnavailable.Error(), http.StatusServiceUnavailable)
			return
		}
		path := strings.TrimPrefix(r.URL.Path, "/internal/v1/proxy")
		if path == "" || path[0] != '/' {
			http.Error(w, "invalid proxy path", http.StatusBadRequest)
			return
		}
		r.URL.Path = path
		handler.ServeHTTP(w, r)
	})
}

func (n *Node) ProxyToLeader(w http.ResponseWriter, r *http.Request, targetGeneration Generation) error {
	status := n.Status()
	state := n.Metadata()
	if status.LeaderID == "" || status.LeaderID == n.cfg.NodeID || state.LeaderOwner == n.cfg.NodeID ||
		state.LeaderOwner == "" || state.LeaderOwner != status.LeaderID || state.LeaderEpoch == 0 {
		return ErrProxyUnavailable
	}
	base, ok := n.internalAPIURLForNode(status.LeaderID)
	if !ok {
		return ErrProxyUnavailable
	}
	client, err := n.internalHTTPClient()
	if err != nil {
		return err
	}
	barrierURL := strings.TrimRight(base, "/") + "/internal/v1/barrier"
	barrierReq, err := http.NewRequestWithContext(r.Context(), http.MethodPost, barrierURL, nil)
	if err != nil {
		return err
	}
	barrierResp, err := client.Do(barrierReq)
	if err != nil {
		return err
	}
	var leaderStatus Status
	decodeErr := json.NewDecoder(io.LimitReader(barrierResp.Body, 1<<20)).Decode(&leaderStatus)
	_ = barrierResp.Body.Close()
	if barrierResp.StatusCode != http.StatusOK || decodeErr != nil || leaderStatus.NodeID != status.LeaderID ||
		leaderStatus.LeaderEpoch != state.LeaderEpoch || leaderStatus.AppliedIndex < leaderStatus.CommitIndex {
		return ErrProxyUnavailable
	}
	if targetGeneration.IsZero() {
		targetGeneration = leaderStatus.CommittedGeneration
	}
	if targetGeneration.IsZero() {
		return ErrProxyUnavailable
	}
	proxyURL := strings.TrimRight(base, "/") + "/internal/v1/proxy" + r.URL.RequestURI()
	proxyReq, err := http.NewRequestWithContext(r.Context(), r.Method, proxyURL, r.Body)
	if err != nil {
		return err
	}
	copyProxyHeaders(proxyReq.Header, r.Header)
	proxyReq.Header.Set(HeaderForwardHop, "1")
	proxyReq.Header.Set(HeaderForwardedBy, n.cfg.NodeID)
	proxyReq.Header.Set(HeaderLeaderEpoch, strconv.FormatUint(state.LeaderEpoch, 10))
	if !targetGeneration.IsZero() {
		proxyReq.Header.Set(HeaderTargetGeneration, targetGeneration.String())
	}
	resp, err := client.Do(proxyReq)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	for key, values := range resp.Header {
		for _, value := range values {
			w.Header().Add(key, value)
		}
	}
	w.Header().Set("X-Meraki-Proxied", "true")
	w.WriteHeader(resp.StatusCode)
	_, err = io.Copy(w, resp.Body)
	return err
}

func copyProxyHeaders(dst, src http.Header) {
	for key, values := range src {
		if strings.HasPrefix(http.CanonicalHeaderKey(key), "X-Clusterha-") || strings.EqualFold(key, "Host") {
			continue
		}
		for _, value := range values {
			dst.Add(key, value)
		}
	}
}

func noteReplicationFailure(nodeID string, err error) {
	_, _ = fmt.Fprintf(os.Stderr, "clusterha snapshot replication to node=%s failed: %v\n", nodeID, err)
}

func (n *Node) VerifyBusinessLeadership(ctx context.Context) error {
	n.mu.RLock()
	r := n.raft
	fsm := n.fsm
	n.mu.RUnlock()
	if r == nil || fsm == nil {
		return ErrNodeNotStarted
	}
	if err := n.checkLocalBusinessLeadership(time.Now(), r, fsm); err != nil {
		return err
	}
	done := make(chan error, 1)
	go func() {
		if err := r.VerifyLeader().Error(); err != nil {
			done <- fmt.Errorf("verify raft leadership: %w", err)
			return
		}
		if err := r.Barrier(time.Duration(n.cfg.MaxQuorumVerificationAge)).Error(); err != nil {
			done <- fmt.Errorf("apply read barrier: %w", err)
			return
		}
		done <- nil
	}()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-done:
		if err != nil {
			n.updateCachedLeadership(false, n.Status().LeaderEpoch, err.Error())
			return err
		}
		n.updateCachedLeadership(true, fsm.State().LeaderEpoch, "")
		return nil
	}
}

func (n *Node) VerifyLocalRead(ctx context.Context, generation Generation) error {
	state := n.Metadata()
	manifest, ok := state.Snapshots[generation.ManifestHash]
	if !ok || !manifest.Generation.Equal(generation) || n.verifyManifestBlobs(manifest) != nil {
		return ErrProxyUnavailable
	}
	if state.ActiveSnapshot != generation.ManifestHash && time.Now().After(manifest.CreatedAt.Add(time.Duration(n.cfg.GenerationPinTTL))) {
		return fmt.Errorf("%w: generation pin expired", ErrProxyUnavailable)
	}
	if n.HasBusinessLeadership() {
		return n.VerifyBusinessLeadership(ctx)
	}
	status := n.Status()
	if status.LeaderID == "" || status.LeaderID == n.cfg.NodeID || state.LeaderOwner != status.LeaderID {
		if status.LeaderID == n.cfg.NodeID && n.HasBusinessLeadership() {
			return n.VerifyBusinessLeadership(ctx)
		}
		return ErrProxyUnavailable
	}
	base, ok := n.internalAPIURLForNode(status.LeaderID)
	if !ok {
		return ErrProxyUnavailable
	}
	client, err := n.internalHTTPClient()
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(base, "/")+"/internal/v1/barrier", nil)
	if err != nil {
		return err
	}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	var authoritative Status
	if resp.StatusCode != http.StatusOK || json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&authoritative) != nil {
		return ErrProxyUnavailable
	}
	n.mu.RLock()
	r := n.raft
	n.mu.RUnlock()
	if r == nil {
		return ErrNodeNotStarted
	}
	localApplied := parseUint(r.Stats()["applied_index"])
	if authoritative.NodeID != status.LeaderID || authoritative.LeaderEpoch != state.LeaderEpoch ||
		!containsGeneration(authoritative.AvailableGenerations, generation) || authoritative.AppliedIndex < authoritative.CommittedGenerationIndex ||
		localApplied < authoritative.CommittedGenerationIndex {
		return fmt.Errorf("%w: leader=%s/%s epoch=%d/%d generation=%s/%s local_applied=%d leader_applied=%d leader_commit=%d",
			ErrProxyUnavailable, authoritative.NodeID, status.LeaderID, authoritative.LeaderEpoch, state.LeaderEpoch,
			authoritative.CommittedGeneration.String(), generation.String(), localApplied, authoritative.AppliedIndex, authoritative.CommittedGenerationIndex)
	}
	return nil
}

func (n *Node) SubscribeLeadership(ctx context.Context) <-chan LeadershipEvent {
	ch := make(chan LeadershipEvent, 1)
	n.mu.Lock()
	n.subs[ch] = struct{}{}
	status := n.status
	n.mu.Unlock()
	ch <- LeadershipEvent{Active: status.Role == RoleLeader && status.HasQuorum, Epoch: status.LeaderEpoch, Reason: status.Reason}
	go func() {
		<-ctx.Done()
		n.mu.Lock()
		delete(n.subs, ch)
		close(ch)
		n.mu.Unlock()
	}()
	return ch
}

func (n *Node) setLeadership(active bool, epoch uint64, reason string) {
	n.mu.Lock()
	if n.leadership.Active == active && n.leadership.Epoch == epoch {
		n.leadership.Reason = reason
		n.mu.Unlock()
		return
	}
	n.leadership = LeadershipEvent{Active: active, Epoch: epoch, Reason: reason}
	for ch := range n.subs {
		event := n.leadership
		select {
		case ch <- event:
		default:
			select {
			case <-ch:
			default:
			}
			ch <- event
		}
	}
	n.mu.Unlock()
}

func (n *Node) HasBusinessLeadership() bool {
	n.mu.RLock()
	r := n.raft
	fsm := n.fsm
	n.mu.RUnlock()
	return n.checkLocalBusinessLeadership(time.Now(), r, fsm) == nil
}

func (n *Node) checkLocalBusinessLeadership(now time.Time, r *raft.Raft, fsm *metadataFSM) error {
	if r == nil || fsm == nil {
		return ErrNodeNotStarted
	}
	if r.State() != raft.Leader {
		return ErrNotBusinessLeader
	}
	state := fsm.State()
	if !SupportedNodeCapabilities().SupportsGate(state.CapabilityGate, true) {
		return ErrNotBusinessLeader
	}
	stats := r.Stats()
	if state.LeaderOwner != n.cfg.NodeID || state.LeaderEpoch == 0 || state.LastLeadershipTerm != parseUint(stats["term"]) {
		return ErrNotBusinessLeader
	}
	status := n.Status()
	if status.Role != RoleLeader || !status.HasQuorum || status.LeaderEpoch != state.LeaderEpoch ||
		status.QuorumVerifiedAt.IsZero() || now.Sub(status.QuorumVerifiedAt) > time.Duration(n.cfg.MaxQuorumVerificationAge) {
		return ErrNotBusinessLeader
	}
	return nil
}

func (n *Node) updateCachedLeadership(active bool, epoch uint64, reason string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if !active {
		n.status.HasQuorum = false
		n.status.Role = RoleFollower
		n.status.Ready = false
		n.status.Readiness = ReadinessNotReady
		if reason != "" {
			n.status.Reason = reason
		}
		return
	}
	n.status.HasQuorum = true
	n.status.QuorumVerifiedAt = time.Now()
	n.status.Role = RoleLeader
	n.status.LeaderEpoch = epoch
}

func (n *Node) SetServingReadiness(check func() (bool, string)) {
	n.mu.Lock()
	n.servingReady = check
	n.mu.Unlock()
}

func (n *Node) updateStatus(role string, leaderID raft.ServerID, leaderAddr raft.ServerAddress, state MetadataState, stats map[string]string, hasQuorum bool, verifiedAt time.Time, reason string, now time.Time) {
	n.mu.RLock()
	servingReady := n.servingReady
	n.mu.RUnlock()
	applicationReady := false
	applicationReason := "application serving readiness is not configured"
	if servingReady != nil {
		applicationReady, applicationReason = servingReady()
	}
	availableGenerations := make([]Generation, 0, len(state.Snapshots))
	for hash, snapshot := range state.Snapshots {
		if (hash == state.ActiveSnapshot || now.Before(snapshot.CreatedAt.Add(time.Duration(n.cfg.GenerationPinTTL)))) && n.verifyManifestBlobs(snapshot) == nil {
			availableGenerations = append(availableGenerations, snapshot.Generation)
		}
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	n.status.Role = role
	n.status.LeaderID = string(leaderID)
	n.status.LeaderAddress = string(leaderAddr)
	n.status.LeaderEpoch = state.LeaderEpoch
	n.status.Term = parseUint(stats["term"])
	n.status.CommitIndex = parseUint(stats["commit_index"])
	n.status.AppliedIndex = parseUint(stats["applied_index"])
	n.status.CommittedGeneration = Generation{}
	n.status.CommittedGenerationIndex = state.ActiveSnapshotIndex
	n.status.AvailableGenerations = availableGenerations
	n.status.ProtocolVersion = CurrentProtocolVersion
	n.status.CommandVersion = CurrentCommandVersion
	n.status.Capabilities = SupportedNodeCapabilities()
	n.status.ActiveCapabilityGate = normalizeCapabilityGate(state.CapabilityGate)
	n.status.LeaderEligible = SupportedNodeCapabilities().SupportsGate(state.CapabilityGate, true)
	if manifest, ok := state.Snapshots[state.ActiveSnapshot]; ok {
		n.status.CommittedGeneration = manifest.Generation
	}
	n.status.HasQuorum = hasQuorum
	if !verifiedAt.IsZero() {
		n.status.QuorumVerifiedAt = verifiedAt
	}
	contact := parseDuration(stats["last_contact"])
	if role == RoleFollower {
		if contact >= 0 {
			n.status.LastLeaderContactAt = now.Add(-contact)
		}
	}
	transportReady := false
	switch role {
	case RoleLeader:
		transportReady = hasQuorum && !verifiedAt.IsZero() && now.Sub(verifiedAt) <= time.Duration(n.cfg.MaxQuorumVerificationAge)
	case RoleFollower:
		transportReady = leaderID != "" && contact >= 0 && contact <= time.Duration(n.cfg.MaxLeaderContactAge)
	}
	n.status.Ready = transportReady && applicationReady && n.status.LeaderEligible
	n.status.Readiness = ReadinessNotReady
	if n.status.Ready {
		n.status.Readiness = ReadinessReady
		n.status.Reason = ""
		return
	}
	if reason == "" {
		if !transportReady {
			reason = "node cannot currently prove a safe cluster path"
		} else {
			reason = applicationReason
		}
	}
	n.status.Reason = reason
}

func waitContext(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return nil
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func containsGeneration(generations []Generation, wanted Generation) bool {
	for _, generation := range generations {
		if generation.Equal(wanted) {
			return true
		}
	}
	return false
}

func (n *Node) Metadata() MetadataState {
	n.mu.RLock()
	fsm := n.fsm
	n.mu.RUnlock()
	if fsm == nil {
		return MetadataState{ClusterID: n.cfg.ClusterID}
	}
	return fsm.State()
}

func (n *Node) Members() ([]MemberStatus, error) {
	if n.isUnjoined() {
		return []MemberStatus{{NodeID: n.cfg.NodeID, Address: n.cfg.RaftAdvertiseAddr, Suffrage: "unjoined"}}, nil
	}
	n.mu.RLock()
	r := n.raft
	n.mu.RUnlock()
	if r == nil {
		return nil, ErrNodeNotStarted
	}
	future := r.GetConfiguration()
	if err := future.Error(); err != nil {
		return nil, err
	}
	result := make([]MemberStatus, 0, len(future.Configuration().Servers))
	for _, server := range future.Configuration().Servers {
		result = append(result, MemberStatus{NodeID: string(server.ID), Address: string(server.Address), Suffrage: server.Suffrage.String()})
	}
	return result, nil
}

type MemberStatus struct {
	NodeID   string `json:"node_id"`
	Address  string `json:"address"`
	Suffrage string `json:"suffrage"`
}

func (n *Node) Close() error {
	n.mu.Lock()
	if n.cancel != nil {
		n.cancel()
	}
	started := n.started
	n.started = false
	n.mu.Unlock()
	if !started || !n.cfg.Enabled {
		return nil
	}
	select {
	case <-n.done:
	case <-time.After(3 * time.Second):
	}
	return n.closeRuntime()
}

func (n *Node) closeRuntime() error {
	n.mu.RLock()
	r := n.raft
	transport := n.transport
	store := n.store
	server := n.internalServer
	listener := n.internalListener
	n.mu.RUnlock()
	var errs []error
	if server != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		if err := server.Shutdown(ctx); err != nil {
			errs = append(errs, err)
		}
		cancel()
	}
	if listener != nil {
		_ = listener.Close()
	}
	if r != nil {
		if err := r.Shutdown().Error(); err != nil {
			errs = append(errs, err)
		}
	}
	if transport != nil {
		transport.Close()
	}
	if store != nil {
		if err := store.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

func (n *Node) Enabled() bool  { n.mu.RLock(); defer n.mu.RUnlock(); return n.status.Enabled }
func (n *Node) Ready() bool    { n.mu.RLock(); defer n.mu.RUnlock(); return n.status.Ready }
func (n *Node) Status() Status { n.mu.RLock(); defer n.mu.RUnlock(); return n.status }

func parseUint(value string) uint64 { parsed, _ := strconv.ParseUint(value, 10, 64); return parsed }
func parseDuration(value string) time.Duration {
	if value == "never" || value == "" {
		return -1
	}
	parsed, err := time.ParseDuration(value)
	if err != nil {
		return -1
	}
	return parsed
}
func minDuration(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}
