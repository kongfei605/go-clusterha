package clusterha

import "testing"

func TestSupportedCapabilitiesFenceLevelThreeControllers(t *testing.T) {
	capabilities := SupportedNodeCapabilities()
	if capabilities.CapabilityLevel != 3 {
		t.Fatalf("capability level=%d", capabilities.CapabilityLevel)
	}
	if !capabilities.SupportsGate(LegacyCapabilityGate(), true) {
		t.Fatal("new binary must remain eligible under the legacy gate")
	}
	gate := LegacyCapabilityGate()
	gate.MinimumLeaderCapability = 3
	gate.MinimumVoterCapability = 3
	if !capabilities.SupportsGate(gate, true) || !capabilities.SupportsGate(gate, false) {
		t.Fatal("new binary must support the controller fencing gate")
	}
	old := capabilities
	old.CapabilityLevel = 2
	if old.SupportsGate(gate, true) || old.SupportsGate(gate, false) {
		t.Fatal("level-two binary was not fenced by the controller gate")
	}
}
