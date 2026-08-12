package clusterha

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"io"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/hashicorp/raft"
)

func TestThreeNodeElectionReplicationAndFailover(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping three-node integration test in short mode")
	}
	serverName := "clusterha.test"
	caCert, caKey, caFile := createTestCA(t)
	nodeIDs := []string{"node-a", "node-b", "node-c"}
	raftAddresses := []string{freeAddress(t), freeAddress(t), freeAddress(t)}
	internalAddresses := []string{freeAddress(t), freeAddress(t), freeAddress(t)}
	initialMembers := make([]string, len(nodeIDs))
	for i := range nodeIDs {
		initialMembers[i] = fmt.Sprintf("%s=%s|https://%s", nodeIDs[i], raftAddresses[i], internalAddresses[i])
	}

	nodes := make([]*Node, 0, len(nodeIDs))
	for i, nodeID := range nodeIDs {
		certFile, keyFile := createNodeCertificate(t, caCert, caKey, nodeID, serverName)
		cfg := Config{
			Enabled: true, ClusterID: "integration", NodeID: nodeID,
			RaftBindAddr: raftAddresses[i], RaftAdvertiseAddr: raftAddresses[i],
			InternalAPIBindAddr: internalAddresses[i], InternalAPIAdvertiseAddr: "https://" + internalAddresses[i],
			DataDir: t.TempDir(), BootstrapExpect: 3, InitialMembers: initialMembers,
			MaxLeaderContactAge: Duration(2 * time.Second), MaxQuorumVerificationAge: Duration(time.Second),
			SnapshotDurablePolicy: SnapshotDurablePolicyVoterQuorum,
			InternalTLS:           TLSConfig{Enabled: true, CAFile: caFile, CertFile: certFile, KeyFile: keyFile, ServerName: serverName},
		}
		node, err := NewNode(cfg)
		if err != nil {
			t.Fatal(err)
		}
		if nodeID != "node-c" {
			node.capabilities = testV2Capabilities()
		}
		if err := node.Start(t.Context()); err != nil {
			t.Fatal(err)
		}
		nodes = append(nodes, node)
	}
	t.Cleanup(func() {
		for _, node := range nodes {
			_ = node.Close()
		}
	})

	leader := waitForSingleLeader(t, nodes, 20*time.Second)
	state := leader.Metadata()
	if _, ok := state.Values["cluster/members"]; !ok {
		t.Fatal("bootstrap membership was not committed to FSM")
	}
	revision, err := leader.PutMetadataBatch(context.Background(), "test-batch-command", map[string]any{
		"runtime/features": map[string]bool{"enabled": true},
		"ssid/state":       []string{"imported"},
	}, state.Revision)
	if err != nil {
		t.Fatal(err)
	}
	state = leader.Metadata()
	if _, ok := state.Values["ssid/state"]; !ok {
		t.Fatal("metadata batch did not commit every key")
	}
	revision, err = leader.PutMetadata(context.Background(), "test-command", "runtime/features", map[string]bool{"enabled": true}, revision)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := leader.PutMetadata(context.Background(), "test-command", "runtime/features", map[string]bool{"enabled": false}, revision); err == nil {
		t.Fatal("expected command id reuse with different payload to fail")
	}
	if err := leader.VerifyBusinessLeadership(context.Background()); err != nil {
		t.Fatal(err)
	}
	leaderStatus := leader.Status()
	if leaderStatus.AppliedIndex < leaderStatus.CommitIndex {
		t.Fatalf("barrier did not wait for FSM apply: applied=%d commit=%d", leaderStatus.AppliedIndex, leaderStatus.CommitIndex)
	}
	blob := []byte(`{"ok":true}`)
	blobHash, err := NewBlobStore(filepath.Join(leader.cfg.DataDir, "cas")).PutBytes(blob)
	if err != nil {
		t.Fatal(err)
	}
	manifest := Manifest{
		SchemaVersion: 1,
		Generation:    Generation{ClusterID: "integration", LeaderEpoch: leader.Status().LeaderEpoch, Sequence: 1},
		CreatedAt:     time.Unix(1, 0).UTC(),
		Datasets: map[string]DatasetRef{
			"test": {Name: "test", Scope: "global", BlobHash: blobHash, Encoding: "json", RecordCount: 1, CollectedAt: time.Unix(1, 0).UTC(), SourceSuccess: true},
		},
	}
	snapshotRevision, err := leader.PublishSnapshot(context.Background(), "snapshot-command", manifest, map[string]io.Reader{blobHash: bytes.NewReader(blob)})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := leader.Metadata().Values["test"]; ok {
		t.Fatal("snapshot data must not be stored in metadata values")
	}
	waitFor(t, 10*time.Second, func() bool {
		for _, node := range nodes {
			if node.Metadata().Revision < snapshotRevision {
				return false
			}
			reader, _, err := node.OpenSnapshotDataset("test")
			if err != nil {
				return false
			}
			data, err := io.ReadAll(reader)
			_ = reader.Close()
			if err != nil || string(data) != string(blob) {
				return false
			}
		}
		return true
	})
	waitFor(t, 10*time.Second, func() bool {
		for _, node := range nodes {
			active, ok := node.ActiveSnapshot()
			err := node.VerifyLocalRead(context.Background(), active.Generation)
			if !ok || err != nil {
				return false
			}
		}
		return true
	})
	waitFor(t, 5*time.Second, func() bool {
		for _, node := range nodes {
			if len(node.Status().AvailableGenerations) == 0 {
				return false
			}
		}
		return true
	})
	waitFor(t, 10*time.Second, func() bool {
		for _, node := range nodes {
			if node.Metadata().Revision < snapshotRevision {
				return false
			}
		}
		return true
	})
	joinNodeID := "node-d"
	joinRaftAddress := freeAddress(t)
	joinInternalAddress := freeAddress(t)
	joinCertFile, joinKeyFile := createNodeCertificate(t, caCert, caKey, joinNodeID, serverName)
	joinCfg := Config{
		Enabled: true, ClusterID: "integration", NodeID: joinNodeID,
		RaftBindAddr: joinRaftAddress, RaftAdvertiseAddr: joinRaftAddress,
		InternalAPIBindAddr: joinInternalAddress, InternalAPIAdvertiseAddr: "https://" + joinInternalAddress,
		DataDir: t.TempDir(), BootstrapExpect: 3, JoinExisting: true, InitialMembers: initialMembers,
		MaxLeaderContactAge: Duration(2 * time.Second), MaxQuorumVerificationAge: Duration(time.Second),
		SnapshotDurablePolicy: SnapshotDurablePolicyVoterQuorum,
		InternalTLS:           TLSConfig{Enabled: true, CAFile: caFile, CertFile: joinCertFile, KeyFile: joinKeyFile, ServerName: serverName},
	}
	joinNode, err := NewNode(joinCfg)
	if err != nil {
		t.Fatal(err)
	}
	joinNode.capabilities = testV2Capabilities()
	if err := joinNode.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	nodes = append(nodes, joinNode)
	joinMember := Member{NodeID: joinNodeID, Addr: joinRaftAddress, InternalAPIURL: "https://" + joinInternalAddress}
	leader.addPendingMember(joinMember)
	leader.refreshPeerRegistry()
	joinCapabilities, err := leader.prepareJoiningMember(context.Background(), joinMember)
	if err != nil {
		t.Fatal(err)
	}
	joinOperation := MembershipOperation{ID: "join/node-d/recovery-window", Type: MembershipOperationJoin,
		Phase: MembershipPhasePrepared, Member: joinMember, Capabilities: joinCapabilities,
		CreatedAt: time.Now().UTC(), UpdatedAt: time.Now().UTC()}
	if err := leader.beginMembershipOperation(context.Background(), joinOperation); err != nil {
		t.Fatal(err)
	}
	v2Gate := CapabilityGate{
		ProtocolVersion: 2, CommandVersion: 2, ManifestSchemaVersion: 1,
		DatasetSchemaVersions: map[string]uint32{"*": 1}, MinimumLeaderCapability: 2, MinimumVoterCapability: 2,
	}
	if _, err := leader.ActivateCapabilities(context.Background(), "activate-v2-before-replacement", v2Gate); err == nil {
		t.Fatal("v2 capability gate activated while a v1-only voter remained")
	}
	if _, present, err := leader.raftServerSuffrage(joinNodeID); err != nil || present {
		t.Fatalf("prepared join unexpectedly changed Raft configuration: present=%t err=%v", present, err)
	}
	if err := leader.JoinVoter(context.Background(), joinMember); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 10*time.Second, func() bool {
		members, err := leader.Members()
		if err != nil {
			return false
		}
		for _, member := range members {
			if member.NodeID == joinNodeID && member.Suffrage == raft.Voter.String() {
				return true
			}
		}
		return false
	})
	operation, ok, err := leader.membershipOperation()
	if err != nil || !ok || operation.Phase != MembershipPhaseCompleted {
		t.Fatalf("join operation was not completed: operation=%#v ok=%t err=%v", operation, ok, err)
	}
	// Simulate a leader crash after Raft promoted the voter but before the FSM
	// recorded completion. Reconciliation must derive the next step from Raft.
	operation.ID += "/recovery"
	operation.Phase = MembershipPhasePrepared
	operation.UpdatedAt = time.Now().UTC()
	state = leader.Metadata()
	if _, err := leader.PutMetadata(context.Background(), operation.ID+"/test-recovery", membershipOperationMetadataKey, operation, state.Revision); err != nil {
		t.Fatal(err)
	}
	if err := leader.reconcileMembershipOperation(context.Background()); err != nil {
		t.Fatal(err)
	}
	operation, ok, err = leader.membershipOperation()
	if err != nil || !ok || operation.Phase != MembershipPhaseCompleted {
		t.Fatalf("join recovery did not complete: operation=%#v ok=%t err=%v", operation, ok, err)
	}
	replacedNodeID := "node-c"
	if leader.NodeID() == replacedNodeID {
		transferTarget := "node-a"
		if transferTarget == replacedNodeID {
			transferTarget = "node-b"
		}
		if err := leader.TransferLeadership(context.Background(), transferTarget); err != nil {
			t.Fatal(err)
		}
		leader = waitForSingleLeader(t, nodes, 20*time.Second)
	}
	if err := leader.RemoveServer(context.Background(), replacedNodeID); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 10*time.Second, func() bool {
		members, err := leader.Members()
		if err != nil {
			return false
		}
		for _, member := range members {
			if member.NodeID == replacedNodeID {
				return false
			}
		}
		_, trusted := leader.peers.Member(replacedNodeID)
		return !trusted
	})
	if _, err := leader.ActivateCapabilities(context.Background(), "activate-v2-after-replacement", v2Gate); err != nil {
		t.Fatal(err)
	}
	state = leader.Metadata()
	if state.CapabilityGate.ProtocolVersion != 2 || state.CapabilityGate.CommandVersion != 2 {
		t.Fatalf("v2 gate was not activated: %#v", state.CapabilityGate)
	}
	if _, err := leader.PutMetadata(context.Background(), "v2-command", "test/v2", true, state.Revision); err != nil {
		t.Fatal(err)
	}

	oldEpoch := leader.Status().LeaderEpoch
	if err := leader.Close(); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 5*time.Second, func() bool { return !leader.HasBusinessLeadership() })
	remaining := make([]*Node, 0, 2)
	for _, node := range nodes {
		if node != leader {
			remaining = append(remaining, node)
		}
	}
	newLeader := waitForSingleLeader(t, remaining, 20*time.Second)
	if newLeader.Status().LeaderEpoch <= oldEpoch {
		t.Fatalf("new leader epoch=%d must be greater than old epoch=%d", newLeader.Status().LeaderEpoch, oldEpoch)
	}
	if _, ok := newLeader.Metadata().Values["runtime/features"]; !ok {
		t.Fatal("replicated metadata missing after failover")
	}
}

func testV2Capabilities() NodeCapabilities {
	return NodeCapabilities{
		CapabilityLevel: 2,
		Protocol:        VersionRange{Min: 1, Max: 2},
		Command:         VersionRange{Min: 1, Max: 2},
		ManifestSchema:  VersionRange{Min: 1, Max: 2},
		DatasetSchema:   VersionRange{Min: 1, Max: 2},
	}
}

func waitForSingleLeader(t *testing.T, nodes []*Node, timeout time.Duration) *Node {
	t.Helper()
	var leader *Node
	waitFor(t, timeout, func() bool {
		leader = nil
		for _, node := range nodes {
			status := node.Status()
			if status.Role == RoleLeader && status.HasQuorum {
				if leader != nil {
					return false
				}
				leader = node
			}
		}
		return leader != nil
	})
	return leader
}

func waitFor(t *testing.T, timeout time.Duration, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatal("condition did not become true before timeout")
}

func freeAddress(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	return address
}

func createTestCA(t *testing.T) (*x509.Certificate, *rsa.PrivateKey, string) {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "clusterha-test-ca"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "ca.pem")
	if err := os.WriteFile(path, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0600); err != nil {
		t.Fatal(err)
	}
	return cert, key, path
}

func createNodeCertificate(t *testing.T, ca *x509.Certificate, caKey *rsa.PrivateKey, nodeID, serverName string) (string, string) {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 120))
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{
		SerialNumber: serial, Subject: pkix.Name{CommonName: nodeID, Organization: []string{"integration"}}, DNSNames: []string{serverName},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		KeyUsage:    x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, template, ca, &key.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	certFile := filepath.Join(dir, "cert.pem")
	keyFile := filepath.Join(dir, "key.pem")
	if err := os.WriteFile(certFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(keyFile, pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)}), 0600); err != nil {
		t.Fatal(err)
	}
	return certFile, keyFile
}
