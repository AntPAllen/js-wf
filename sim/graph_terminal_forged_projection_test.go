package sim

import (
	"encoding/json"
	"js-wf/identity"
	"js-wf/wf"
	"os"
	"reflect"
	"testing"
)

func TestGraphTerminalForgedProjectionCurrentReplay(t *testing.T) {
	generated, err := runGraphTerminalWorker(3, nil)
	if err != nil {
		t.Fatal(err)
	}
	if generated.Decisions[0].Chosen != "forged_state" {
		t.Fatal("directed mode changed")
	}
	cut := false
	payload, _ := json.Marshal(wf.Outcome{InvSeq: 1, Result: []byte(`42`)})
	for _, event := range generated.Transport {
		if event.Operation == "kv_update" && event.Subject == identity.Key("test", integratedWorkerIDs(1)[0]) && event.DataSHA256 == digest(payload) && event.Outcome == "ok" {
			cut = true
		}
	}
	if !cut {
		t.Fatal("projection CAS not reached")
	}
	replayed, err := runGraphTerminalWorker(3, &generated)
	if err != nil || !reflect.DeepEqual(generated, replayed) {
		t.Fatal("corrected projection trace diverged", err)
	}
	if path := os.Getenv("SIM_GRAPH_FORGED_PROJECTION_TRACE"); path != "" {
		if err := generated.Save(path); err != nil {
			t.Fatal(err)
		}
	}
}
