package clusterha

import (
	"encoding/json"
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

func mustJSON(t *testing.T, value any) []byte {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return data
}
