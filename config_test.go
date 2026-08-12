package clusterha

import (
	"context"
	"strings"
	"testing"
	"time"
)

func TestNormalizeConfigDefaults(t *testing.T) {
	var cfg Config
	NormalizeConfig(&cfg)

	if cfg.ReadConsistency != DefaultReadConsistency {
		t.Fatalf("ReadConsistency=%q", cfg.ReadConsistency)
	}
	if time.Duration(cfg.MaxQuorumVerificationAge) != DefaultMaxQuorumVerificationAge {
		t.Fatalf("MaxQuorumVerificationAge=%s", time.Duration(cfg.MaxQuorumVerificationAge))
	}
	if cfg.SnapshotDurablePolicy != SnapshotDurablePolicyVoterQuorum {
		t.Fatalf("SnapshotDurablePolicy=%q", cfg.SnapshotDurablePolicy)
	}
	if cfg.GenerationRetention != DefaultGenerationRetention {
		t.Fatalf("GenerationRetention=%d", cfg.GenerationRetention)
	}
}

func TestValidateConfigDisabledAllowsEmptyConfig(t *testing.T) {
	if err := ValidateConfig(Config{}); err != nil {
		t.Fatal(err)
	}
}

func TestValidateConfigEnabledRequiresVoterQuorumAndMTLS(t *testing.T) {
	cfg := validEnabledConfig()
	cfg.SnapshotDurablePolicy = "2"
	if err := ValidateConfig(cfg); err == nil {
		t.Fatal("expected snapshot durable policy validation error")
	}

	cfg = validEnabledConfig()
	cfg.InternalTLS.Enabled = false
	if err := ValidateConfig(cfg); err == nil {
		t.Fatal("expected internal mTLS validation error")
	}
}

func TestValidateConfigRejectsUnspecifiedAdvertiseAddr(t *testing.T) {
	cfg := validEnabledConfig()
	cfg.RaftAdvertiseAddr = "0.0.0.0:9200"
	if err := ValidateConfig(cfg); err == nil {
		t.Fatal("expected unspecified raft advertise addr validation error")
	}

	cfg = validEnabledConfig()
	cfg.InternalAPIAdvertiseAddr = "https://0.0.0.0:9201"
	if err := ValidateConfig(cfg); err == nil {
		t.Fatal("expected unspecified internal advertise addr validation error")
	}
}

func TestNodeStartsNotReadyUntilRuntimeConnects(t *testing.T) {
	node, err := NewNode(validEnabledConfig())
	if err != nil {
		t.Fatal(err)
	}
	status := node.Status()
	if status.Ready {
		t.Fatal("cluster node must remain not ready before Start")
	}
	if status.Reason != "cluster runtime is starting" {
		t.Fatalf("reason=%q", status.Reason)
	}
}

func TestJoinExistingRefusesToBootstrapEmptyDirectory(t *testing.T) {
	cfg := validEnabledConfig()
	caCert, caKey, caFile := createTestCA(t)
	certFile, keyFile := createNodeCertificate(t, caCert, caKey, cfg.NodeID, "clusterha.test")
	cfg.JoinExisting = true
	cfg.RaftBindAddr = "127.0.0.1:0"
	cfg.RaftAdvertiseAddr = "127.0.0.1:9200"
	cfg.InternalAPIBindAddr = "127.0.0.1:0"
	cfg.InternalAPIAdvertiseAddr = "https://127.0.0.1:9201"
	cfg.DataDir = t.TempDir()
	cfg.InitialMembers[0] = "meraki-a=127.0.0.1:9200|https://127.0.0.1:9201"
	cfg.InternalTLS = TLSConfig{Enabled: true, CAFile: caFile, CertFile: certFile, KeyFile: keyFile, ServerName: "clusterha.test"}
	node, err := NewNode(cfg)
	if err != nil {
		t.Fatal(err)
	}
	err = node.Start(t.Context())
	if err == nil || !strings.Contains(err.Error(), "join_existing=true") {
		t.Fatalf("err=%v", err)
	}
}

func TestDynamicMembershipAPIsFailClosed(t *testing.T) {
	node, err := NewNode(validEnabledConfig())
	if err != nil {
		t.Fatal(err)
	}
	if err := node.AddVoter(context.Background(), Member{NodeID: "new", Addr: "127.0.0.1:1", InternalAPIURL: "https://127.0.0.1:2"}); err == nil {
		t.Fatal("AddVoter should fail closed until safe join workflow exists")
	}
	if err := node.RemoveServer(context.Background(), "old"); err == nil {
		t.Fatal("RemoveServer should fail closed until safe replacement workflow exists")
	}
}

func validEnabledConfig() Config {
	cfg := Config{
		Enabled:                  true,
		ClusterID:                "meraki-prod",
		NodeID:                   "meraki-a",
		RaftBindAddr:             "127.0.0.1:9200",
		RaftAdvertiseAddr:        "10.0.0.1:9200",
		InternalAPIBindAddr:      "127.0.0.1:9201",
		InternalAPIAdvertiseAddr: "https://10.0.0.1:9201",
		DataDir:                  "/tmp/meraki-clusterha-test",
		BootstrapExpect:          3,
		InitialMembers: []string{
			"meraki-a=10.0.0.1:9200",
			"meraki-b=10.0.0.2:9200",
			"meraki-c=10.0.0.3:9200",
		},
		SnapshotDurablePolicy: SnapshotDurablePolicyVoterQuorum,
		InternalTLS: TLSConfig{
			Enabled:  true,
			CAFile:   "/tmp/ca.pem",
			CertFile: "/tmp/node.pem",
			KeyFile:  "/tmp/node-key.pem",
		},
		MaxLeaderContactAge:      Duration(DefaultMaxLeaderContactAge),
		MaxQuorumVerificationAge: Duration(DefaultMaxQuorumVerificationAge),
	}
	NormalizeConfig(&cfg)
	return cfg
}
