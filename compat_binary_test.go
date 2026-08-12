package clusterha

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

var (
	compatChildMode       = flag.Bool("clusterha-compat-child", false, "run clusterha compatibility child process")
	compatChildConfigPath = flag.String("clusterha-compat-config", "", "clusterha compatibility child config")
	compatCapabilityLevel = "1"
)

type compatChildConfig struct {
	Node      Config `json:"node"`
	AdminAddr string `json:"admin_addr"`
}

type compatProcess struct {
	nodeID string
	level  string
	admin  string
	cmd    *exec.Cmd
	logs   bytes.Buffer
}

func TestNNPlusOneIndependentBinaryCompatibility(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping independent binary compatibility test in short mode")
	}
	if *compatChildMode {
		t.Skip("parent test is disabled in child mode")
	}

	binDir := t.TempDir()
	oldBinary := buildCompatBinary(t, binDir, "clusterha-n.test", "1")
	newBinary := buildCompatBinary(t, binDir, "clusterha-nplus1.test", "2")

	serverName := "clusterha.test"
	caCert, caKey, caFile := createTestCA(t)
	nodeIDs := []string{"node-a", "node-b", "node-c"}
	levels := []string{"2", "2", "1"}
	binaries := []string{newBinary, newBinary, oldBinary}
	raftAddresses := []string{freeAddress(t), freeAddress(t), freeAddress(t)}
	internalAddresses := []string{freeAddress(t), freeAddress(t), freeAddress(t)}
	adminAddresses := []string{freeAddress(t), freeAddress(t), freeAddress(t)}
	initialMembers := make([]string, len(nodeIDs))
	for i := range nodeIDs {
		initialMembers[i] = fmt.Sprintf("%s=%s|https://%s", nodeIDs[i], raftAddresses[i], internalAddresses[i])
	}

	processes := make([]*compatProcess, 0, len(nodeIDs))
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
		processes = append(processes, startCompatChild(t, binaries[i], levels[i], compatChildConfig{
			Node: cfg, AdminAddr: adminAddresses[i],
		}))
	}

	leader := waitForCompatLeader(t, processes, 30*time.Second)
	state := compatMetadata(t, leader)
	if _, ok := state.Values["cluster/members"]; !ok {
		t.Fatal("bootstrap membership was not committed")
	}
	if _, err := compatPutMetadata(t, leader, "compat/put-before-gate", "compat/value", true, state.Revision); err != nil {
		t.Fatal(err)
	}

	gate := CapabilityGate{
		ProtocolVersion: 1, CommandVersion: 1, ManifestSchemaVersion: 1,
		DatasetSchemaVersions:  map[string]uint32{"*": 1},
		MinimumVoterCapability: 1, MinimumLeaderCapability: 2,
	}
	if _, err := compatActivateCapabilities(t, leader, "compat/leader-capability-gate", gate); err != nil {
		t.Fatalf("mixed-version leader capability gate was rejected: %v", err)
	}
	waitForCompatGate(t, processes, gate, 10*time.Second)

	leader = waitForCompatLeader(t, processes, 30*time.Second)
	if leader.level != "2" {
		t.Fatalf("business leadership stayed on N binary %s after leader-only gate", leader.nodeID)
	}
	status := compatStatus(t, leader)
	if !status.LeaderEligible || status.ActiveCapabilityGate.MinimumLeaderCapability != 2 {
		t.Fatalf("new leader status did not apply leader gate: %#v", status)
	}
	oldStatus := compatStatus(t, processes[2])
	if oldStatus.LeaderEligible {
		t.Fatalf("N binary remained leader-eligible after leader-only gate: %#v", oldStatus)
	}

	state = compatMetadata(t, leader)
	if _, err := compatPutMetadata(t, leader, "compat/put-after-gate", "compat/value2", true, state.Revision); err != nil {
		t.Fatal(err)
	}
}

func TestClusterHACompatChildProcess(t *testing.T) {
	if !*compatChildMode {
		t.Skip("not running as clusterha compatibility child process")
	}
	if *compatChildConfigPath == "" {
		t.Fatal("missing -clusterha-compat-config")
	}
	data, err := os.ReadFile(*compatChildConfigPath)
	if err != nil {
		t.Fatal(err)
	}
	var cfg compatChildConfig
	if err := json.Unmarshal(data, &cfg); err != nil {
		t.Fatal(err)
	}
	node, err := NewNode(cfg.Node)
	if err != nil {
		t.Fatal(err)
	}
	if compatCapabilityLevel == "2" {
		node.capabilities = testV2Capabilities()
	}
	node.SetServingReadiness(func() (bool, string) { return true, "" })
	if err := node.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer node.Close()

	mux := http.NewServeMux()
	mux.HandleFunc("/status", func(w http.ResponseWriter, _ *http.Request) {
		writeCompatJSON(w, http.StatusOK, node.Status())
	})
	mux.HandleFunc("/metadata", func(w http.ResponseWriter, _ *http.Request) {
		writeCompatJSON(w, http.StatusOK, node.Metadata())
	})
	mux.HandleFunc("/put", func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			ID               string          `json:"id"`
			Key              string          `json:"key"`
			Value            json.RawMessage `json:"value"`
			ExpectedRevision uint64          `json:"expected_revision"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeCompatError(w, http.StatusBadRequest, err)
			return
		}
		revision, err := node.PutMetadata(r.Context(), req.ID, req.Key, req.Value, req.ExpectedRevision)
		if err != nil {
			writeCompatError(w, http.StatusInternalServerError, err)
			return
		}
		writeCompatJSON(w, http.StatusOK, map[string]uint64{"revision": revision})
	})
	mux.HandleFunc("/activate", func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			ID   string         `json:"id"`
			Gate CapabilityGate `json:"gate"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeCompatError(w, http.StatusBadRequest, err)
			return
		}
		revision, err := node.ActivateCapabilities(r.Context(), req.ID, req.Gate)
		if err != nil {
			writeCompatError(w, http.StatusInternalServerError, err)
			return
		}
		writeCompatJSON(w, http.StatusOK, map[string]uint64{"revision": revision})
	})

	listener, err := net.Listen("tcp", cfg.AdminAddr)
	if err != nil {
		t.Fatal(err)
	}
	server := &http.Server{Handler: mux}
	go func() { _ = server.Serve(listener) }()
	defer server.Close()

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)
	<-signals
}

func buildCompatBinary(t *testing.T, dir, name, capabilityLevel string) string {
	t.Helper()
	path := filepath.Join(dir, name)
	cmd := exec.Command("go", "test", "-c", "-o", path, "-ldflags",
		fmt.Sprintf("-X github.com/kongfei605/go-clusterha.compatCapabilityLevel=%s", capabilityLevel), ".")
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("build compat binary level=%s: %v\n%s", capabilityLevel, err, output)
	}
	return path
}

func startCompatChild(t *testing.T, binary, level string, cfg compatChildConfig) *compatProcess {
	t.Helper()
	configPath := filepath.Join(t.TempDir(), cfg.Node.NodeID+".json")
	data, err := json.Marshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(configPath, data, 0600); err != nil {
		t.Fatal(err)
	}
	proc := &compatProcess{nodeID: cfg.Node.NodeID, level: level, admin: "http://" + cfg.AdminAddr}
	cmd := exec.Command(binary, "-test.run", "^TestClusterHACompatChildProcess$", "-clusterha-compat-child", "-clusterha-compat-config", configPath)
	cmd.Stdout = &proc.logs
	cmd.Stderr = &proc.logs
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	proc.cmd = cmd
	t.Cleanup(func() { stopCompatChild(t, proc) })
	waitForCompatAdmin(t, proc, 10*time.Second)
	return proc
}

func stopCompatChild(t *testing.T, proc *compatProcess) {
	t.Helper()
	if proc.cmd == nil || proc.cmd.Process == nil {
		return
	}
	_ = proc.cmd.Process.Signal(os.Interrupt)
	done := make(chan error, 1)
	go func() { done <- proc.cmd.Wait() }()
	select {
	case <-time.After(5 * time.Second):
		_ = proc.cmd.Process.Kill()
		t.Fatalf("compat child %s did not exit; logs:\n%s", proc.nodeID, proc.logs.String())
	case <-done:
	}
}

func waitForCompatAdmin(t *testing.T, proc *compatProcess, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		resp, err := http.Get(proc.admin + "/status")
		if err == nil {
			_, _ = io.Copy(io.Discard, resp.Body)
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("compat child %s admin did not become ready; logs:\n%s", proc.nodeID, proc.logs.String())
}

func waitForCompatLeader(t *testing.T, processes []*compatProcess, timeout time.Duration) *compatProcess {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		var leader *compatProcess
		for _, proc := range processes {
			status := compatStatus(t, proc)
			if status.Role == RoleLeader && status.HasQuorum {
				if leader != nil {
					leader = nil
					break
				}
				leader = proc
			}
		}
		if leader != nil {
			return leader
		}
		time.Sleep(100 * time.Millisecond)
	}
	for _, proc := range processes {
		t.Logf("compat child %s logs:\n%s", proc.nodeID, proc.logs.String())
	}
	t.Fatal("compat cluster did not elect a single business leader")
	return nil
}

func waitForCompatGate(t *testing.T, processes []*compatProcess, gate CapabilityGate, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		allApplied := true
		for _, proc := range processes {
			if !capabilityGatesEqual(compatStatus(t, proc).ActiveCapabilityGate, gate) {
				allApplied = false
				break
			}
		}
		if allApplied {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	for _, proc := range processes {
		t.Logf("status for %s: %#v", proc.nodeID, compatStatus(t, proc))
	}
	t.Fatal("compat cluster did not apply capability gate")
}

func compatStatus(t *testing.T, proc *compatProcess) Status {
	t.Helper()
	var status Status
	compatGetJSON(t, proc.admin+"/status", &status)
	return status
}

func compatMetadata(t *testing.T, proc *compatProcess) MetadataState {
	t.Helper()
	var state MetadataState
	compatGetJSON(t, proc.admin+"/metadata", &state)
	return state
}

func compatPutMetadata(t *testing.T, proc *compatProcess, id, key string, value any, expectedRevision uint64) (uint64, error) {
	t.Helper()
	var resp struct {
		Revision uint64 `json:"revision"`
	}
	err := compatPostJSON(proc.admin+"/put", map[string]any{
		"id": id, "key": key, "value": value, "expected_revision": expectedRevision,
	}, &resp)
	return resp.Revision, err
}

func compatActivateCapabilities(t *testing.T, proc *compatProcess, id string, gate CapabilityGate) (uint64, error) {
	t.Helper()
	var resp struct {
		Revision uint64 `json:"revision"`
	}
	err := compatPostJSON(proc.admin+"/activate", map[string]any{"id": id, "gate": gate}, &resp)
	return resp.Revision, err
}

func compatGetJSON(t *testing.T, endpoint string, target any) {
	t.Helper()
	client := http.Client{Timeout: 2 * time.Second}
	resp, err := client.Get(endpoint)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		t.Fatalf("GET %s returned %s: %s", endpoint, resp.Status, body)
	}
	if err := json.NewDecoder(resp.Body).Decode(target); err != nil {
		t.Fatal(err)
	}
}

func compatPostJSON(endpoint string, request any, target any) error {
	data, err := json.Marshal(request)
	if err != nil {
		return err
	}
	client := http.Client{Timeout: 10 * time.Second}
	resp, err := client.Post(endpoint, "application/json", bytes.NewReader(data))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("%s: %s", resp.Status, strings.TrimSpace(string(body)))
	}
	return json.NewDecoder(resp.Body).Decode(target)
}

func writeCompatJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func writeCompatError(w http.ResponseWriter, status int, err error) {
	writeCompatJSON(w, status, map[string]string{"error": err.Error()})
}
