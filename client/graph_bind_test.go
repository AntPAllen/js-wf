package client_test

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"

	"js-wf/client"
	"js-wf/identity"
	"js-wf/journal"
	"js-wf/sim"
	"js-wf/wf"
)

func TestGraphClientBindingKeepsOriginalModeAndUsesProvidedTransports(t *testing.T) {
	ctx := context.Background()
	schedule := sim.NewScheduler(1)
	transport := sim.NewSignalTransport(schedule)
	graph, err := journal.NewGraphStore(journal.GraphConfig{Protocol: sim.NewGraphPublicationTransport(schedule).Protocol()})
	if err != nil {
		t.Fatal(err)
	}
	original := client.NewWithSignalPorts(transport, transport)
	bound, err := original.WithGraphJournal(graph)
	if err != nil {
		t.Fatal(err)
	}
	handle, err := bound.Start(ctx, "test", "binding", []byte(`7`))
	if err != nil {
		t.Fatal(err)
	}
	tail, err := graph.Begin(ctx, "test", "binding", handle.InvSeq)
	if err != nil {
		t.Fatal(err)
	}
	tail, err = graph.Append(ctx, "test", "binding", handle.InvSeq, journal.Entry{Kind: journal.Started}, tail, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	transport.SetState(identity.Key("test", "binding"), []byte(`invalid mirror`))
	if seq, err := original.Signal(ctx, "test", "binding", "go", []byte(`42`), "original"); err == nil || seq != 0 {
		t.Fatal("binding mutated original legacy mode", seq, err)
	}
	if seq, err := bound.Signal(ctx, "test", "binding", "go", []byte(`42`), "bound"); err != nil || seq == 0 {
		t.Fatal("bound client did not use graph mode/transports", seq, err)
	}
	body, _ := json.Marshal(wf.Outcome{InvSeq: handle.InvSeq, Result: []byte(`42`)})
	if _, err = graph.Append(ctx, "test", "binding", handle.InvSeq, journal.Entry{Kind: journal.Completed, Index: 1, Payload: body}, tail, nil, nil); err != nil {
		t.Fatal(err)
	}
	transport.SetState(identity.Key("test", "binding"), []byte(`{"inv_seq":1,"error":"forged mirror"}`))
	if result, err := bound.Await(ctx, "test", "binding"); err != nil || !bytes.Equal(result, []byte(`42`)) {
		t.Fatal("bound result transport", string(result), err)
	}
	var absent *client.Client
	if _, err = absent.WithGraphJournal(graph); err == nil {
		t.Fatal("accepted nil client")
	}
	if _, err = original.WithGraphJournal(nil); err == nil {
		t.Fatal("accepted nil graph")
	}
	if _, err = client.NewWithStartPort(transport).WithGraphJournal(graph); err == nil {
		t.Fatal("accepted missing signal transport")
	}
}
