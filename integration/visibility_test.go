package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"js-wf/client"
	"js-wf/identity"
	"js-wf/journal"
	"js-wf/provision"
	"js-wf/retention"
	"js-wf/visibility"
	"js-wf/wf"
	"js-wf/worker"
)

func TestProjectionRebuildLagAndPurge(t *testing.T) {
	for _, encoding := range []journal.Encoding{journal.JSON, journal.ProtobufV1} {
		t.Run(string(encoding), func(t *testing.T) { runProjectionRebuildLagAndPurge(t, encoding) })
	}
}

func runProjectionRebuildLagAndPurge(t *testing.T, encoding journal.Encoding) {
	all, _ := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 35*time.Second)
	defer cancel()
	const typ, id = "test", "visible"
	p, err := visibility.New(ctx, all[0])
	if err != nil {
		t.Fatal(err)
	}
	c := client.New(all[0])
	start, err := c.Start(ctx, typ, id, []byte(`7`))
	if err != nil {
		t.Fatal(err)
	}
	if err := p.Rebuild(ctx); err != nil {
		t.Fatal(err)
	}
	row, err := p.Get(ctx, typ, id)
	if err != nil || row.Status != "queued" || row.InvSeq != start.InvSeq {
		t.Fatalf("queued row=%+v err=%v", row, err)
	}
	w, err := worker.New(ctx, all[1], "projection-worker", map[string]worker.Handler{typ: func(c *wf.Context, input json.RawMessage) (json.RawMessage, error) {
		if err := c.SetSearchAttributes(map[string]string{"team": "wire-contract"}); err != nil {
			return nil, err
		}
		value, err := wf.Run(c, "echo", string(input), func(context.Context) (json.RawMessage, error) { return input, nil })
		return value, err
	}}, worker.WithJournalEncoding(encoding))
	if err != nil {
		t.Fatal(err)
	}
	workerCtx, stopWorker := context.WithCancel(ctx)
	defer stopWorker()
	workerDone := make(chan error, 1)
	go func() { workerDone <- w.RunPartition(workerCtx, identity.Partition(typ, id, provision.Partitions)) }()
	projectionCtx, stopProjection := context.WithCancel(ctx)
	projectionDone := make(chan error, 1)
	go func() { projectionDone <- p.Run(projectionCtx) }()
	if value, err := c.Await(ctx, typ, id); err != nil || string(value) != "7" {
		t.Fatalf("workflow result=%s err=%v", value, err)
	}
	for ctx.Err() == nil {
		row, err = p.Get(ctx, typ, id)
		if err == nil && row.Status == "completed" {
			if lag, lagErr := p.Lag(ctx); lagErr == nil && lag == 0 {
				break
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	if ctx.Err() != nil {
		t.Fatalf("projection did not catch up: row=%+v err=%v", row, err)
	}
	stream, err := all[2].Stream(ctx, "WF_JRN")
	if err != nil {
		t.Fatal(err)
	}
	raw, err := stream.GetLastMsgForSubject(ctx, identity.JournalSubject(typ, id))
	if err != nil {
		t.Fatal(err)
	}
	if bytes.HasPrefix(raw.Data, []byte{'W', 'F', 'J', 0}) != (encoding == journal.ProtobufV1) {
		t.Fatal("wrong physical encoding", encoding)
	}
	if row.Attributes["team"] != "wire-contract" {
		t.Fatal("missing projected attributes", row)
	}
	selected, err := p.ListByAttribute(ctx, "team", "wire-contract", "completed")
	if err != nil || len(selected) != 1 || selected[0].ID != id {
		t.Fatal("attribute index", selected, err)
	}
	if row.Started.IsZero() || row.Updated.IsZero() || row.JournalSeq == 0 {
		t.Fatalf("missing projected timestamps or sequence: %+v", row)
	}
	if _, err := journal.New(all[0]).SnapshotPrefix(ctx, typ, id, 1); err != nil {
		t.Fatal(err)
	}
	view, err := all[0].KeyValue(ctx, "WF_VIEW")
	if err != nil {
		t.Fatal(err)
	}
	before, err := view.Get(ctx, "row."+identity.Key(typ, id))
	if err != nil {
		t.Fatal(err)
	}
	if err := p.Rebuild(ctx); err != nil {
		t.Fatal(err)
	}
	after, err := view.Get(ctx, "row."+identity.Key(typ, id))
	if err != nil || !bytes.Equal(before.Value(), after.Value()) {
		t.Fatalf("rebuild changed row: err=%v", err)
	}
	rows, err := p.List(ctx, "completed")
	if err != nil || len(rows) != 1 || rows[0].ID != id {
		t.Fatalf("completed list=%+v err=%v", rows, err)
	}
	rows, err = p.List(ctx, "queued")
	if err != nil || len(rows) != 0 {
		t.Fatalf("queued index after completion=%+v err=%v", rows, err)
	}
	stopProjection()
	if err := <-projectionDone; err != nil && !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	stopWorker()
	if err := <-workerDone; err != nil {
		t.Fatal(err)
	}
	if err := retention.Purge(ctx, all[0], typ, id, time.Minute); err != nil {
		t.Fatal(err)
	}
	restarted, err := visibility.New(ctx, all[2])
	if err != nil {
		t.Fatal(err)
	}
	if err := restarted.Rebuild(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := restarted.Get(ctx, typ, id); !errors.Is(err, visibility.ErrNotFound) {
		t.Fatalf("purged projection row: %v", err)
	}
	rows, err = restarted.List(ctx, "completed")
	if err != nil || len(rows) != 0 {
		t.Fatalf("list after purge=%+v err=%v", rows, err)
	}
}
