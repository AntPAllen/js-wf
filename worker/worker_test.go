package worker

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"js-wf/client"
	"js-wf/history"
	"js-wf/identity"
	"js-wf/integrity"
	"js-wf/journal"
	"js-wf/provision"
	"js-wf/testcluster"
	"js-wf/wf"

	"github.com/anishathalye/porcupine"
	"github.com/nats-io/nats.go/jetstream"
)

func TestJournalLimitRecordsTerminalFailure(t *testing.T) {
	cluster, err := testcluster.Start(t.TempDir(), 1)
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Close()
	js, err := jetstream.New(cluster.Clients[0])
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := provision.Ensure(ctx, js, 1); err != nil {
		t.Fatal(err)
	}
	var effects atomic.Int64
	w, err := New(ctx, js, "limit-worker", map[string]Handler{"test": func(c *wf.Context, _ json.RawMessage) (json.RawMessage, error) {
		for i := 0; i < 10; i++ {
			if _, err := wf.Run(c, "step", i, func(context.Context) (int, error) {
				effects.Add(1)
				return i, nil
			}); err != nil {
				return nil, err
			}
		}
		return json.RawMessage(`1`), nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	w.maxEntries = 8
	if _, err := client.New(js).Start(ctx, "test", "limit", []byte(`null`)); err != nil {
		t.Fatal(err)
	}
	workerCtx, stop := context.WithCancel(ctx)
	defer stop()
	done := make(chan error, 1)
	go func() { done <- w.RunPartition(workerCtx, identity.Partition("test", "limit", provision.Partitions)) }()
	recorder := &history.Recorder{}
	observed := client.NewObserved(js, recorder)
	_, err = observed.Await(ctx, "test", "limit")
	if err == nil || !strings.Contains(err.Error(), journal.ErrTooLong.Error()) {
		t.Fatalf("terminal error: %v", err)
	}
	_, err = observed.Await(ctx, "test", "limit")
	if err == nil || !strings.Contains(err.Error(), journal.ErrTooLong.Error()) {
		t.Fatalf("reread terminal error: %v", err)
	}
	if result, err := history.CheckResults(recorder.Snapshot(), time.Second); err != nil || result != porcupine.Ok {
		t.Fatalf("failed result history=%s err=%v", result, err)
	}
	stop()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	records, _, err := journal.New(js).Read(ctx, "test", "limit")
	if err != nil || len(records) != 8 || effects.Load() != 3 {
		t.Fatalf("records=%d effects=%d err=%v", len(records), effects.Load(), err)
	}
	if records[7].Kind != journal.Failed {
		t.Fatalf("terminal kind=%s", records[7].Kind)
	}
	var outcome wf.Outcome
	if err := json.Unmarshal(records[7].Payload, &outcome); err != nil {
		t.Fatal(err)
	}
	var attempted struct {
		Kind      string `json:"kind"`
		Name      string `json:"name"`
		InputHash string `json:"input_hash"`
	}
	if err := json.Unmarshal(outcome.LimitRequest, &attempted); err != nil {
		t.Fatal(err)
	}
	inputHash := sha256.Sum256([]byte(`3`))
	if outcome.Error != journal.ErrTooLong.Error() || attempted.Kind != "run" || attempted.Name != "step" || attempted.InputHash != hex.EncodeToString(inputHash[:]) {
		t.Fatalf("journal limit metadata: outcome=%+v attempted=%+v", outcome, attempted)
	}
	if _, err := integrity.Check(ctx, js); err != nil {
		t.Fatal(err)
	}
}
