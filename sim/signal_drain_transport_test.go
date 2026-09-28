package sim

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"

	"js-wf/client"
	"js-wf/identity"
	"js-wf/journal"
	"js-wf/worker"

	"github.com/nats-io/nats.go"
)

func TestWorkerSignalDrainModelSkipsGapsAndReplays(t *testing.T) {
	ctx := context.Background()
	model := NewSignalTransport(NewScheduler(61))
	c := client.NewWithSignalPorts(model, model)
	const typ, id = "test", "drain"
	generation, err := model.PublishInvocation(ctx, &nats.Msg{Subject: identity.InvocationSubject(typ, id), Data: []byte("input")})
	if err != nil {
		t.Fatal(err)
	}
	first, err := c.Signal(ctx, typ, id, "go", []byte("first"), "first")
	if err != nil {
		t.Fatal(err)
	}
	model.CommitSignal(&nats.Msg{Subject: "wf.sig.test.other.go", Data: []byte("other")})
	stale := &nats.Msg{Subject: "wf.sig.test.drain.go", Data: []byte("stale"), Header: nats.Header{}}
	stale.Header.Set("Wf-Inv-Seq", "999")
	model.CommitSignal(stale)
	hole := &nats.Msg{Subject: "wf.sig.test.drain.go", Data: []byte("deleted"), Header: nats.Header{}}
	hole.Header.Set("Wf-Inv-Seq", "1")
	model.PurgeSignal(model.CommitSignal(hole))
	large := bytes.Repeat([]byte("z"), client.MaxInlineSignal+1)
	second, err := c.Signal(ctx, typ, id, "big", large, "second")
	if err != nil || second <= first {
		t.Fatalf("second signal=%d first=%d err=%v", second, first, err)
	}
	var records []journal.Record
	appendEntry := func(kind journal.Kind, payload json.RawMessage) error {
		records = append(records, journal.Record{Entry: journal.Entry{Kind: kind, Index: uint64(len(records)), Payload: append([]byte(nil), payload...)}})
		return nil
	}
	signals, err := worker.DrainSignalsWithPort(ctx, model, typ, id, generation, nil, appendEntry)
	if err != nil || len(signals) != 2 || len(records) != 2 || signals[0].Sequence != first || signals[1].Sequence != second || signals[0].Name != "go" || signals[1].Name != "big" || string(signals[0].Payload) != "first" || !bytes.Equal(signals[1].Payload, large) {
		t.Fatalf("drain signals=%+v records=%d err=%v", signals, len(records), err)
	}
	replayed, err := worker.DrainSignalsWithPort(ctx, model, typ, id, generation, records, appendEntry)
	if err != nil || len(replayed) != 2 || len(records) != 2 || !bytes.Equal(replayed[1].Payload, large) {
		t.Fatalf("replayed signals=%+v records=%d err=%v", replayed, len(records), err)
	}
	corrupt := &nats.Msg{Subject: "wf.sig.test.drain.bad", Data: []byte("wrong"), Header: nats.Header{}}
	corrupt.Header.Set("Wf-Inv-Seq", "1")
	corrupt.Header.Set("Wf-Input-SHA256", strings.Repeat("0", 64))
	model.CommitSignal(corrupt)
	if _, err := worker.DrainSignalsWithPort(ctx, model, typ, id, generation, records, appendEntry); err == nil || !strings.Contains(err.Error(), "hash mismatch") || len(records) != 2 {
		t.Fatalf("corrupt signal: err=%v records=%d", err, len(records))
	}
}
