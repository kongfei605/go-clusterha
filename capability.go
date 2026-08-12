package clusterha

import (
	"fmt"
)

const (
	LegacyProtocolVersion       uint32 = 1
	LegacyCommandVersion        uint32 = 1
	LegacyManifestSchemaVersion uint32 = 1
	LegacyDatasetSchemaVersion  uint32 = 1

	CurrentProtocolVersion       uint32 = 1
	CurrentCommandVersion        uint32 = 1
	CurrentManifestSchemaVersion uint32 = 2
	CurrentDatasetSchemaVersion  uint32 = 2
	CurrentCapabilityLevel       uint32 = 2
)

type VersionRange struct {
	Min uint32 `json:"min"`
	Max uint32 `json:"max"`
}

func (r VersionRange) Supports(version uint32) bool {
	return r.Min != 0 && version >= r.Min && version <= r.Max
}

type NodeCapabilities struct {
	CapabilityLevel uint32       `json:"capability_level"`
	Protocol        VersionRange `json:"protocol"`
	Command         VersionRange `json:"command"`
	ManifestSchema  VersionRange `json:"manifest_schema"`
	DatasetSchema   VersionRange `json:"dataset_schema"`
}

type CapabilityGate struct {
	ProtocolVersion         uint32            `json:"protocol_version"`
	CommandVersion          uint32            `json:"command_version"`
	ManifestSchemaVersion   uint32            `json:"manifest_schema_version"`
	DatasetSchemaVersions   map[string]uint32 `json:"dataset_schema_versions"`
	MinimumLeaderCapability uint32            `json:"minimum_leader_capability"`
	MinimumVoterCapability  uint32            `json:"minimum_voter_capability"`
}

func SupportedNodeCapabilities() NodeCapabilities {
	return NodeCapabilities{
		CapabilityLevel: CurrentCapabilityLevel,
		Protocol:        VersionRange{Min: LegacyProtocolVersion, Max: CurrentProtocolVersion},
		Command:         VersionRange{Min: LegacyCommandVersion, Max: CurrentCommandVersion},
		ManifestSchema:  VersionRange{Min: LegacyManifestSchemaVersion, Max: CurrentManifestSchemaVersion},
		DatasetSchema:   VersionRange{Min: LegacyDatasetSchemaVersion, Max: CurrentDatasetSchemaVersion},
	}
}

func LegacyCapabilityGate() CapabilityGate {
	return CapabilityGate{
		ProtocolVersion:         LegacyProtocolVersion,
		CommandVersion:          LegacyCommandVersion,
		ManifestSchemaVersion:   LegacyManifestSchemaVersion,
		DatasetSchemaVersions:   map[string]uint32{"*": LegacyDatasetSchemaVersion},
		MinimumLeaderCapability: 1,
		MinimumVoterCapability:  1,
	}
}

func normalizeCapabilityGate(gate CapabilityGate) CapabilityGate {
	legacy := LegacyCapabilityGate()
	if gate.ProtocolVersion == 0 {
		gate.ProtocolVersion = legacy.ProtocolVersion
	}
	if gate.CommandVersion == 0 {
		gate.CommandVersion = legacy.CommandVersion
	}
	if gate.ManifestSchemaVersion == 0 {
		gate.ManifestSchemaVersion = legacy.ManifestSchemaVersion
	}
	if gate.MinimumLeaderCapability == 0 {
		gate.MinimumLeaderCapability = legacy.MinimumLeaderCapability
	}
	if gate.MinimumVoterCapability == 0 {
		gate.MinimumVoterCapability = legacy.MinimumVoterCapability
	}
	if len(gate.DatasetSchemaVersions) == 0 {
		gate.DatasetSchemaVersions = map[string]uint32{"*": LegacyDatasetSchemaVersion}
	} else {
		copyVersions := make(map[string]uint32, len(gate.DatasetSchemaVersions))
		for name, version := range gate.DatasetSchemaVersions {
			if version == 0 {
				version = LegacyDatasetSchemaVersion
			}
			copyVersions[name] = version
		}
		gate.DatasetSchemaVersions = copyVersions
	}
	return gate
}

func (g CapabilityGate) DatasetSchemaVersion(name string) uint32 {
	g = normalizeCapabilityGate(g)
	if version, ok := g.DatasetSchemaVersions[name]; ok {
		return version
	}
	if version, ok := g.DatasetSchemaVersions["*"]; ok {
		return version
	}
	return LegacyDatasetSchemaVersion
}

func capabilityGatesEqual(left, right CapabilityGate) bool {
	left = normalizeCapabilityGate(left)
	right = normalizeCapabilityGate(right)
	if left.ProtocolVersion != right.ProtocolVersion || left.CommandVersion != right.CommandVersion ||
		left.ManifestSchemaVersion != right.ManifestSchemaVersion ||
		left.MinimumLeaderCapability != right.MinimumLeaderCapability ||
		left.MinimumVoterCapability != right.MinimumVoterCapability ||
		len(left.DatasetSchemaVersions) != len(right.DatasetSchemaVersions) {
		return false
	}
	for name, version := range left.DatasetSchemaVersions {
		if right.DatasetSchemaVersions[name] != version {
			return false
		}
	}
	return true
}

func (c NodeCapabilities) SupportsGate(gate CapabilityGate, leader bool) bool {
	gate = normalizeCapabilityGate(gate)
	minimum := gate.MinimumVoterCapability
	if leader {
		minimum = gate.MinimumLeaderCapability
	}
	if c.CapabilityLevel < minimum || !c.Protocol.Supports(gate.ProtocolVersion) ||
		!c.Command.Supports(gate.CommandVersion) || !c.ManifestSchema.Supports(gate.ManifestSchemaVersion) {
		return false
	}
	for _, version := range gate.DatasetSchemaVersions {
		if !c.DatasetSchema.Supports(version) {
			return false
		}
	}
	return true
}

func validateCapabilityGate(gate CapabilityGate) error {
	gate = normalizeCapabilityGate(gate)
	if gate.ProtocolVersion == 0 || gate.CommandVersion == 0 || gate.ManifestSchemaVersion == 0 {
		return fmt.Errorf("capability gate versions must be non-zero")
	}
	if gate.MinimumLeaderCapability == 0 || gate.MinimumVoterCapability == 0 {
		return fmt.Errorf("capability gate minimum capability levels must be non-zero")
	}
	for name, version := range gate.DatasetSchemaVersions {
		if name == "" || version == 0 {
			return fmt.Errorf("dataset schema name and version must be non-zero")
		}
	}
	return nil
}
