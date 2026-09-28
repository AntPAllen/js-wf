package history

import (
	"encoding/json"
	"testing"
	"time"

	"js-wf/client"

	"github.com/anishathalye/porcupine"
)

func TestSignalModel(t *testing.T) {
	base := time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC)
	operation := func(call, returned time.Duration, key, hash, status string, seq uint64) client.Operation {
		args, _ := json.Marshal(signalInput{Type: "test", ID: "same", Name: "go", PayloadHash: hash, IdempotencyKey: key, InvSeq: 7})
		result, _ := json.Marshal(signalOutput{Status: status, SignalSeq: seq})
		return client.Operation{InvokeTS: base.Add(call), ReturnTS: base.Add(returned), Op: "signal", Args: args, Result: result}
	}
	first := operation(0, time.Millisecond, "one", "hash-a", "signaled", 10)
	second := operation(2*time.Millisecond, 3*time.Millisecond, "two", "hash-b", "signaled", 12)
	duplicate := operation(4*time.Millisecond, 5*time.Millisecond, "one", "hash-a", "signaled", 10)
	mismatch := operation(6*time.Millisecond, 7*time.Millisecond, "one", "hash-c", "payload_mismatch", 0)
	unknown := operation(8*time.Millisecond, 9*time.Millisecond, "three", "hash-d", "unknown", 0)
	tests := []struct {
		name string
		ops  []client.Operation
		want porcupine.CheckResult
	}{
		{"ordered and idempotent", []client.Operation{first, second, duplicate, mismatch}, porcupine.Ok},
		{"reverse sequence", []client.Operation{operation(0, time.Millisecond, "one", "hash-a", "signaled", 12), operation(2*time.Millisecond, 3*time.Millisecond, "two", "hash-b", "signaled", 10)}, porcupine.Illegal},
		{"duplicate changed sequence", []client.Operation{first, operation(2*time.Millisecond, 3*time.Millisecond, "one", "hash-a", "signaled", 11)}, porcupine.Illegal},
		{"duplicate changed payload", []client.Operation{first, operation(2*time.Millisecond, 3*time.Millisecond, "one", "hash-b", "signaled", 10)}, porcupine.Illegal},
		{"mismatch without prior", []client.Operation{mismatch}, porcupine.Illegal},
		{"uncertain committed", []client.Operation{unknown, operation(10*time.Millisecond, 11*time.Millisecond, "three", "hash-d", "signaled", 14)}, porcupine.Ok},
		{"uncertain absent", []client.Operation{unknown, operation(10*time.Millisecond, 11*time.Millisecond, "three", "hash-e", "signaled", 14)}, porcupine.Ok},
		{"uncertain before later append", []client.Operation{unknown, operation(10*time.Millisecond, 11*time.Millisecond, "four", "hash-e", "signaled", 16), operation(12*time.Millisecond, 13*time.Millisecond, "three", "hash-d", "signaled", 14)}, porcupine.Ok},
		{"uncertain cannot precede lower sequence", []client.Operation{first, unknown, operation(10*time.Millisecond, 11*time.Millisecond, "three", "hash-d", "signaled", 9)}, porcupine.Illegal},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := CheckSignals(test.ops, time.Second)
			if err != nil || got != test.want {
				t.Fatalf("check=%s err=%v, want %s", got, err, test.want)
			}
		})
	}
	long := []client.Operation{first, operation(3*time.Minute, 3*time.Minute+time.Millisecond, "one", "hash-a", "signaled", 10)}
	if got, err := CheckSignals(long, time.Second); got != porcupine.Unknown || err == nil {
		t.Fatalf("expired dedup window: check=%s err=%v", got, err)
	}
}
