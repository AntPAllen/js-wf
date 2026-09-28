package history

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"js-wf/client"

	"github.com/anishathalye/porcupine"
)

func TestStartModelAcceptsOverlapAndRejectsDoubleStart(t *testing.T) {
	base := time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC)
	makeOperation := func(call, returned time.Duration, hash, status string) client.Operation {
		args, _ := json.Marshal(map[string]string{"type": "test", "id": "same", "input_hash": hash})
		result, _ := json.Marshal(map[string]any{"status": status, "inv_seq": 7})
		return client.Operation{InvokeTS: base.Add(call), ReturnTS: base.Add(returned), Op: "start", Args: args, Result: result}
	}
	good := []client.Operation{
		makeOperation(0, 10*time.Millisecond, "hash-a", "started"),
		makeOperation(5*time.Millisecond, 15*time.Millisecond, "hash-a", "already_started"),
		makeOperation(16*time.Millisecond, 20*time.Millisecond, "hash-b", "input_mismatch"),
	}
	if result, err := CheckStarts(good, time.Second); err != nil || result != porcupine.Ok {
		t.Fatalf("valid start history=%s err=%v", result, err)
	}
	broken := append([]client.Operation(nil), good...)
	broken[1] = makeOperation(5*time.Millisecond, 15*time.Millisecond, "hash-a", "started")
	if result, err := CheckStarts(broken, time.Second); err != nil || result != porcupine.Illegal {
		t.Fatalf("double start history=%s err=%v", result, err)
	}
	lostAck := []client.Operation{makeOperation(0, 10*time.Millisecond, "hash-a", "already_started"), makeOperation(11*time.Millisecond, 15*time.Millisecond, "hash-b", "input_mismatch")}
	if result, err := CheckStarts(lostAck, time.Second); err != nil || result != porcupine.Ok {
		t.Fatalf("self-observed lost ack history=%s err=%v", result, err)
	}
	priorAck := []client.Operation{makeOperation(0, 10*time.Millisecond, "hash-a", "already_started"), makeOperation(11*time.Millisecond, 15*time.Millisecond, "hash-a", "started")}
	if result, err := CheckStarts(priorAck, time.Second); err != nil || result != porcupine.Illegal {
		t.Fatalf("start after completed prior ack history=%s err=%v", result, err)
	}
	unknown := makeOperation(0, 10*time.Millisecond, "hash-a", "unknown")
	unknown.Result = json.RawMessage(`{"status":"unknown","inv_seq":0}`)
	committed := []client.Operation{unknown, makeOperation(11*time.Millisecond, 15*time.Millisecond, "hash-a", "already_started")}
	if result, err := CheckStarts(committed, time.Second); err != nil || result != porcupine.Ok {
		t.Fatalf("unknown committed history=%s err=%v", result, err)
	}
	notCommitted := []client.Operation{unknown, makeOperation(11*time.Millisecond, 15*time.Millisecond, "hash-b", "started")}
	if result, err := CheckStarts(notCommitted, time.Second); err != nil || result != porcupine.Ok {
		t.Fatalf("unknown absent history=%s err=%v", result, err)
	}
	impossible := []client.Operation{unknown, makeOperation(11*time.Millisecond, 15*time.Millisecond, "hash-b", "already_started"), makeOperation(16*time.Millisecond, 20*time.Millisecond, "hash-a", "already_started")}
	if result, err := CheckStarts(impossible, time.Second); err != nil || result != porcupine.Illegal {
		t.Fatalf("impossible unknown history=%s err=%v", result, err)
	}
}

func TestRecorderExportsJSONLines(t *testing.T) {
	var recorder Recorder
	operation := client.Operation{InvokeTS: time.Now(), ReturnTS: time.Now().Add(time.Millisecond), Op: "getResult", Args: json.RawMessage(`{"type":"test","id":"one"}`), Result: json.RawMessage(`{"status":"completed"}`)}
	recorder.Record(operation)
	operation.Args[0] = '['
	snapshot := recorder.Snapshot()
	if len(snapshot) != 1 || snapshot[0].Args[0] != '{' {
		t.Fatalf("recorder did not isolate operation bytes: %+v", snapshot)
	}
	var output bytes.Buffer
	if err := recorder.WriteJSONL(&output); err != nil {
		t.Fatal(err)
	}
	if strings.Count(output.String(), "\n") != 1 || !strings.Contains(output.String(), `"getResult"`) {
		t.Fatalf("history export=%q", output.String())
	}
}
