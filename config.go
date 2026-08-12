package clusterha

import (
	"errors"
	"fmt"
	"net"
	"net/url"
	"strings"
	"time"
)

const (
	DefaultReadConsistency           = "bounded"
	DefaultMaxLeaderContactAge       = 2 * time.Second
	DefaultMaxQuorumVerificationAge  = 2 * time.Second
	DefaultEmergencyStaleReadMaxAge  = 15 * time.Minute
	DefaultSnapshotPublishInterval   = time.Minute
	DefaultSnapshotPublishRetryMin   = 5 * time.Second
	DefaultSnapshotPublishRetryMax   = time.Minute
	DefaultSnapshotFreshnessMaxAge   = 5 * time.Minute
	DefaultGenerationRetention       = 3
	DefaultGenerationPinTTL          = 5 * time.Minute
	DefaultMaxSnapshotBlobBytes      = int64(512 * 1024 * 1024)
	DefaultMaxSnapshotBlobPuts       = 2
	SnapshotDurablePolicyVoterQuorum = "voter_quorum"
)

type Config struct {
	Enabled                    bool      `toml:"enabled"`
	ClusterID                  string    `toml:"cluster_id"`
	NodeID                     string    `toml:"node_id"`
	ExternalAPIBindAddr        string    `toml:"external_api_bind_addr"`
	RaftBindAddr               string    `toml:"raft_bind_addr"`
	RaftAdvertiseAddr          string    `toml:"raft_advertise_addr"`
	InternalAPIBindAddr        string    `toml:"internal_api_bind_addr"`
	InternalAPIAdvertiseAddr   string    `toml:"internal_api_advertise_addr"`
	DataDir                    string    `toml:"data_dir"`
	BootstrapExpect            int       `toml:"bootstrap_expect"`
	JoinExisting               bool      `toml:"join_existing"`
	InitialMembers             []string  `toml:"initial_members"`
	MembershipAdminNodeIDs     []string  `toml:"membership_admin_node_ids"`
	ReadConsistency            string    `toml:"read_consistency"`
	MaxLeaderContactAge        Duration  `toml:"max_leader_contact_age"`
	MaxQuorumVerificationAge   Duration  `toml:"max_quorum_verification_age"`
	EmergencyStaleRead         bool      `toml:"emergency_stale_read"`
	EmergencyStaleReadMaxAge   Duration  `toml:"emergency_stale_read_max_age"`
	SnapshotPublishInterval    Duration  `toml:"snapshot_publish_interval"`
	SnapshotPublishRetryMin    Duration  `toml:"snapshot_publish_retry_min"`
	SnapshotPublishRetryMax    Duration  `toml:"snapshot_publish_retry_max"`
	SnapshotFreshnessMaxAge    Duration  `toml:"snapshot_freshness_max_age"`
	SnapshotDurablePolicy      string    `toml:"snapshot_durable_policy"`
	AdditionalNonVoterReplicas int       `toml:"additional_non_voter_replicas"`
	GenerationRetention        int       `toml:"generation_retention"`
	GenerationPinTTL           Duration  `toml:"generation_pin_ttl"`
	MaxSnapshotBlobBytes       int64     `toml:"max_snapshot_blob_bytes"`
	MaxSnapshotBlobPuts        int       `toml:"max_snapshot_blob_puts"`
	InternalTLS                TLSConfig `toml:"internal_tls"`
}

type TLSConfig struct {
	Enabled    bool   `toml:"enabled"`
	CAFile     string `toml:"ca_file"`
	CertFile   string `toml:"cert_file"`
	KeyFile    string `toml:"key_file"`
	ServerName string `toml:"server_name"`
}

type Member struct {
	NodeID         string `json:"node_id"`
	Addr           string `json:"addr"`
	InternalAPIURL string `json:"internal_api_url,omitempty"`
}

func NormalizeConfig(cfg *Config) {
	if cfg.ReadConsistency == "" {
		cfg.ReadConsistency = DefaultReadConsistency
	}
	if cfg.MaxLeaderContactAge <= 0 {
		cfg.MaxLeaderContactAge = Duration(DefaultMaxLeaderContactAge)
	}
	if cfg.MaxQuorumVerificationAge <= 0 {
		cfg.MaxQuorumVerificationAge = Duration(DefaultMaxQuorumVerificationAge)
	}
	if cfg.EmergencyStaleReadMaxAge <= 0 {
		cfg.EmergencyStaleReadMaxAge = Duration(DefaultEmergencyStaleReadMaxAge)
	}
	if cfg.SnapshotPublishInterval <= 0 {
		cfg.SnapshotPublishInterval = Duration(DefaultSnapshotPublishInterval)
	}
	if cfg.SnapshotPublishRetryMin <= 0 {
		cfg.SnapshotPublishRetryMin = Duration(DefaultSnapshotPublishRetryMin)
	}
	if cfg.SnapshotPublishRetryMax <= 0 {
		cfg.SnapshotPublishRetryMax = Duration(DefaultSnapshotPublishRetryMax)
	}
	if cfg.SnapshotFreshnessMaxAge <= 0 {
		cfg.SnapshotFreshnessMaxAge = Duration(DefaultSnapshotFreshnessMaxAge)
	}
	if cfg.SnapshotDurablePolicy == "" {
		cfg.SnapshotDurablePolicy = SnapshotDurablePolicyVoterQuorum
	}
	if cfg.GenerationRetention <= 0 {
		cfg.GenerationRetention = DefaultGenerationRetention
	}
	if cfg.GenerationPinTTL <= 0 {
		cfg.GenerationPinTTL = Duration(DefaultGenerationPinTTL)
	}
	if cfg.MaxSnapshotBlobBytes <= 0 {
		cfg.MaxSnapshotBlobBytes = DefaultMaxSnapshotBlobBytes
	}
	if cfg.MaxSnapshotBlobPuts <= 0 {
		cfg.MaxSnapshotBlobPuts = DefaultMaxSnapshotBlobPuts
	}
}

func ValidateConfig(cfg Config) error {
	if !cfg.Enabled {
		return nil
	}
	if cfg.ReadConsistency != "bounded" && cfg.ReadConsistency != "linearizable" {
		return fmt.Errorf("cluster.read_consistency must be %q or %q, got %q", "bounded", "linearizable", cfg.ReadConsistency)
	}
	if cfg.SnapshotPublishRetryMin > cfg.SnapshotPublishRetryMax {
		return errors.New("cluster.snapshot_publish_retry_min must not exceed cluster.snapshot_publish_retry_max")
	}
	if cfg.SnapshotFreshnessMaxAge > cfg.EmergencyStaleReadMaxAge && cfg.EmergencyStaleRead {
		return errors.New("cluster.emergency_stale_read_max_age must be at least cluster.snapshot_freshness_max_age")
	}
	if strings.TrimSpace(cfg.ClusterID) == "" {
		return errors.New("cluster.cluster_id is required when cluster.enabled=true")
	}
	if strings.TrimSpace(cfg.NodeID) == "" {
		return errors.New("cluster.node_id is required when cluster.enabled=true")
	}
	if strings.TrimSpace(cfg.DataDir) == "" {
		return errors.New("cluster.data_dir is required when cluster.enabled=true")
	}
	if err := validateHostPort(cfg.RaftBindAddr, "cluster.raft_bind_addr"); err != nil {
		return err
	}
	if cfg.BootstrapExpect != 3 {
		return fmt.Errorf("cluster.bootstrap_expect must be 3 for the initial production topology, got %d", cfg.BootstrapExpect)
	}
	if len(cfg.InitialMembers) != cfg.BootstrapExpect {
		return fmt.Errorf("cluster.initial_members must contain %d members, got %d", cfg.BootstrapExpect, len(cfg.InitialMembers))
	}
	members, err := ParseInitialMembers(cfg.InitialMembers)
	if err != nil {
		return err
	}
	seen := make(map[string]struct{}, len(members))
	foundSelf := false
	for _, member := range members {
		if _, ok := seen[member.NodeID]; ok {
			return fmt.Errorf("cluster.initial_members contains duplicate node_id %q", member.NodeID)
		}
		seen[member.NodeID] = struct{}{}
		if member.NodeID == cfg.NodeID {
			foundSelf = true
		}
		if err := validateHostPort(member.Addr, "cluster.initial_members"); err != nil {
			return err
		}
		if member.InternalAPIURL != "" {
			if err := validateInternalAdvertiseURL(member.InternalAPIURL); err != nil {
				return fmt.Errorf("cluster.initial_members internal API for node %q: %w", member.NodeID, err)
			}
		}
	}
	if !foundSelf && !cfg.JoinExisting {
		return fmt.Errorf("cluster.node_id %q is not present in cluster.initial_members", cfg.NodeID)
	}
	if err := validateHostPort(cfg.RaftAdvertiseAddr, "cluster.raft_advertise_addr"); err != nil {
		return err
	}
	if strings.HasPrefix(cfg.RaftAdvertiseAddr, "0.0.0.0:") || strings.HasPrefix(cfg.RaftAdvertiseAddr, "[::]:") {
		return errors.New("cluster.raft_advertise_addr must not use an unspecified address")
	}
	if err := validateInternalAdvertiseURL(cfg.InternalAPIAdvertiseAddr); err != nil {
		return err
	}
	if err := validateHostPort(cfg.InternalAPIBindAddr, "cluster.internal_api_bind_addr"); err != nil {
		return err
	}
	if cfg.SnapshotDurablePolicy != SnapshotDurablePolicyVoterQuorum {
		return fmt.Errorf("cluster.snapshot_durable_policy must be %q in production", SnapshotDurablePolicyVoterQuorum)
	}
	if cfg.AdditionalNonVoterReplicas < 0 {
		return errors.New("cluster.additional_non_voter_replicas must not be negative")
	}
	if !cfg.InternalTLS.Enabled {
		return errors.New("cluster.internal_tls.enabled must be true when cluster.enabled=true")
	}
	if strings.TrimSpace(cfg.InternalTLS.CAFile) == "" || strings.TrimSpace(cfg.InternalTLS.CertFile) == "" || strings.TrimSpace(cfg.InternalTLS.KeyFile) == "" {
		return errors.New("cluster.internal_tls ca_file, cert_file and key_file are required when cluster.enabled=true")
	}
	return nil
}

func ParseInitialMembers(raw []string) ([]Member, error) {
	members := make([]Member, 0, len(raw))
	for _, item := range raw {
		parts := strings.SplitN(item, "=", 2)
		if len(parts) != 2 || strings.TrimSpace(parts[0]) == "" || strings.TrimSpace(parts[1]) == "" {
			return nil, fmt.Errorf("cluster.initial_members entry %q must be node_id=host:port or node_id=host:port|https://internal-host:port", item)
		}
		addressParts := strings.SplitN(strings.TrimSpace(parts[1]), "|", 2)
		member := Member{NodeID: strings.TrimSpace(parts[0]), Addr: strings.TrimSpace(addressParts[0])}
		if len(addressParts) == 2 {
			member.InternalAPIURL = strings.TrimRight(strings.TrimSpace(addressParts[1]), "/")
		}
		members = append(members, member)
	}
	return members, nil
}

func validateHostPort(addr, field string) error {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return fmt.Errorf("%s must be host:port: %w", field, err)
	}
	if strings.TrimSpace(host) == "" || strings.TrimSpace(port) == "" {
		return fmt.Errorf("%s must include host and port", field)
	}
	return nil
}

func validateInternalAdvertiseURL(raw string) error {
	parsed, err := url.Parse(raw)
	if err != nil {
		return fmt.Errorf("cluster.internal_api_advertise_addr is invalid: %w", err)
	}
	if parsed.Scheme != "https" || parsed.Host == "" {
		return errors.New("cluster.internal_api_advertise_addr must be an absolute https URL")
	}
	host := parsed.Hostname()
	if host == "0.0.0.0" || host == "::" || host == "" {
		return errors.New("cluster.internal_api_advertise_addr must not use an unspecified address")
	}
	if parsed.RawQuery != "" || parsed.Fragment != "" {
		return errors.New("cluster.internal_api_advertise_addr must not include query or fragment")
	}
	return nil
}
