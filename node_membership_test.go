package clusterha

import (
	"encoding/json"
	"testing"
)

func TestValidateJoiningNodeStatusRequiresHealthyConsensusState(t *testing.T) {
	node := &Node{cfg: Config{ClusterID: "cluster"}, fsm: newMetadataFSM("cluster")}
	gate := LegacyCapabilityGate()
	node.fsm.state.Revision = 7
	member := Member{NodeID: "node-d"}
	status := Status{
		ClusterID: "cluster", NodeID: "node-d", FSMHealthy: true, FSMRevision: 7,
		CommitIndex: 10, AppliedIndex: 10, ActiveCapabilityGate: gate,
		Capabilities: SupportedNodeCapabilities(),
	}
	if caughtUp, err := node.validateJoiningNodeStatus(member, status, gate); err != nil || !caughtUp {
		t.Fatalf("healthy status rejected: caught_up=%t err=%v", caughtUp, err)
	}
	status.FSMHealthy = false
	status.FSMApplyError = "unsupported command version"
	status.FSMApplyErrorIndex = 9
	if _, err := node.validateJoiningNodeStatus(member, status, gate); err == nil {
		t.Fatal("FSM apply fault was accepted")
	}
	status.FSMHealthy = true
	status.FSMApplyError = ""
	status.FSMRevision = 6
	if caughtUp, err := node.validateJoiningNodeStatus(member, status, gate); err != nil || caughtUp {
		t.Fatalf("stale FSM revision should wait: caught_up=%t err=%v", caughtUp, err)
	}
	status.FSMRevision = 7
	status.ActiveCapabilityGate.CommandVersion = 2
	if _, err := node.validateJoiningNodeStatus(member, status, gate); err == nil {
		t.Fatal("mismatched capability gate was accepted")
	}
}

func TestValidateCapabilityActivationVotersRequiresLeaderEligibleVoter(t *testing.T) {
	gate := CapabilityGate{
		ProtocolVersion: 1, CommandVersion: 1, ManifestSchemaVersion: 1,
		DatasetSchemaVersions:  map[string]uint32{"*": 1},
		MinimumVoterCapability: 1, MinimumLeaderCapability: 2,
	}
	voters := map[string]NodeCapabilities{
		"node-a": SupportedNodeCapabilities(),
		"node-b": SupportedNodeCapabilities(),
		"node-c": SupportedNodeCapabilities(),
	}
	if err := validateCapabilityActivationVoters(voters, gate); err == nil {
		t.Fatal("capability gate with no leader-eligible voter was accepted")
	}
	voters["node-b"] = testV2Capabilities()
	if err := validateCapabilityActivationVoters(voters, gate); err != nil {
		t.Fatalf("capability gate with one leader-eligible voter was rejected: %v", err)
	}
}

func TestRefreshPeerRegistryRevokesRemovedOperationMember(t *testing.T) {
	fsm := newMetadataFSM("cluster")
	members := map[string]Member{
		"node-a": {NodeID: "node-a", Addr: "127.0.0.1:1"},
		"node-b": {NodeID: "node-b", Addr: "127.0.0.1:2"},
	}
	memberData, _ := json.Marshal(members)
	operationData, _ := json.Marshal(MembershipOperation{
		ID: "remove/node-b", Type: MembershipOperationRemove, Phase: MembershipPhaseRemoved, Member: members["node-b"],
	})
	fsm.state.Values["cluster/members"] = memberData
	fsm.state.Values[membershipOperationMetadataKey] = operationData
	node := &Node{
		cfg: Config{NodeID: "node-a"}, fsm: fsm, peers: newPeerRegistry(nil),
		pendingMembers: map[string]Member{"node-b": members["node-b"]},
	}
	node.refreshPeerRegistry()
	if _, trusted := node.peers.Member("node-b"); trusted {
		t.Fatal("removed node remained trusted before metadata cleanup")
	}
}
