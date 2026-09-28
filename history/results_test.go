package history

import (
	"encoding/json"
	"testing"
	"time"

	"js-wf/client"

	"github.com/anishathalye/porcupine"
)

func TestResultModel(t *testing.T) {
	base := time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC)
	operation := func(call, returned time.Duration, status string, generation uint64, hash string) client.Operation {
		args, _ := json.Marshal(resultInput{Type: "test", ID: "same"})
		result, _ := json.Marshal(resultOutput{Status: status, InvSeq: generation, ResultHash: hash})
		return client.Operation{InvokeTS: base.Add(call), ReturnTS: base.Add(returned), Op: "getResult", Args: args, Result: result}
	}
	first := operation(0, time.Millisecond, "completed", 7, "hash-a")
	second := operation(2*time.Millisecond, 3*time.Millisecond, "completed", 7, "hash-a")
	purged := operation(4*time.Millisecond, 5*time.Millisecond, "purged", 7, "")
	reused := operation(6*time.Millisecond, 7*time.Millisecond, "completed", 12, "hash-b")
	tests := []struct {
		name string
		ops  []client.Operation
		want porcupine.CheckResult
	}{
		{"stable and reused", []client.Operation{first, second, purged, reused}, porcupine.Ok},
		{"changed result", []client.Operation{first, operation(2*time.Millisecond, 3*time.Millisecond, "completed", 7, "hash-b")}, porcupine.Illegal},
		{"changed terminal status", []client.Operation{first, operation(2*time.Millisecond, 3*time.Millisecond, "failed", 7, "error-hash")}, porcupine.Illegal},
		{"completed after purge", []client.Operation{first, purged, operation(6*time.Millisecond, 7*time.Millisecond, "completed", 7, "hash-a")}, porcupine.Illegal},
		{"old generation after reuse", []client.Operation{reused, operation(8*time.Millisecond, 9*time.Millisecond, "completed", 7, "hash-a")}, porcupine.Illegal},
		{"overlap around purge", []client.Operation{operation(0, 6*time.Millisecond, "completed", 7, "hash-a"), purged}, porcupine.Ok},
		{"failed is stable", []client.Operation{operation(0, time.Millisecond, "failed", 7, "error-hash"), operation(2*time.Millisecond, 3*time.Millisecond, "failed", 7, "error-hash")}, porcupine.Ok},
		{"not found before start", []client.Operation{operation(0, time.Millisecond, "not_found", 0, ""), first}, porcupine.Ok},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := CheckResults(test.ops, time.Second)
			if err != nil || got != test.want {
				t.Fatalf("check=%s err=%v, want %s", got, err, test.want)
			}
		})
	}
}
