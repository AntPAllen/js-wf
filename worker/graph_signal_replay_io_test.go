package worker_test

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"js-wf/client"
	"js-wf/identity"
	"js-wf/internal/graphpublication"
	"js-wf/internal/retainedgraph"
	"js-wf/journal"
	"js-wf/lease"
	"js-wf/provision"
	"js-wf/sim"
	"js-wf/wf"
	"js-wf/worker"
)

type signalBodyReadCounter struct {
	*sim.GraphPublicationTransport
	hash  string
	reads int
	deny  bool
}

func (p *signalBodyReadCounter) Get(ctx context.Context, link retainedgraph.Link, limit int) ([]byte, error) {
	if link.Hash == p.hash {
		p.reads++
		if p.deny {
			return nil, context.DeadlineExceeded
		}
	}
	return p.GraphPublicationTransport.Get(ctx, link, limit)
}

var _ graphpublication.Port = (*signalBodyReadCounter)(nil)

// Replay must fetch the journal-owned input exactly once. An unreadable owned
// body must NAK before handler admission rather than trust queue metadata.
func TestGraphCanonicalSignalReplayReadsJournalOwnedBodyOnce(t *testing.T) {
	for _, deny := range []bool{false, true} {
		t.Run(fmt.Sprint(deny), func(t *testing.T) {
			ctx := context.Background()
			schedule := sim.NewScheduler(1)
			model := sim.NewGraphPublicationTransport(schedule)
			counter := &signalBodyReadCounter{GraphPublicationTransport: model}
			protocol := model.Protocol()
			protocol.Port = counter
			now := time.Unix(1000, 0)
			store, err := journal.NewGraphStore(journal.GraphConfig{Protocol: protocol, CanonicalStarts: true, CanonicalSignals: true, Now: func() time.Time { return now }})
			if err != nil {
				t.Fatal(err)
			}
			transport := sim.NewWorkerTransport(schedule, 3*time.Second)
			c, err := client.NewWithSignalPorts(transport.SignalTransport, transport.SignalTransport).WithGraphJournal(store)
			if err != nil {
				t.Fatal(err)
			}
			h, err := c.Start(ctx, "test", "integrated0002", []byte(`7`))
			if err != nil {
				t.Fatal(err)
			}
			input, err := store.ReserveSignal(ctx, journal.GraphSignalRequest{Type: h.Type, ID: h.ID, Invocation: h.InvSeq, Name: "signal", Key: "key"}, []byte(`true`), false)
			if err != nil {
				t.Fatal(err)
			}
			msg := &nats.Msg{Subject: "wf.sig." + h.Type + "." + h.ID + ".signal", Data: input.PointerBytes(), Header: nats.Header{}}
			msg.Header.Set(journal.GraphSignalTokenHeader, input.Token)
			msg.Header.Set("Wf-Input-SHA256", input.InputSHA256)
			msg.Header.Set("Wf-Inv-Seq", strconv.FormatUint(h.InvSeq, 10))
			seq := transport.CommitSignal(msg)
			if progress, e := store.BindNextSignal(ctx, h.Type, h.ID, h.InvSeq, seq, transport.SignalTransport); e != nil || !progress {
				t.Fatal(progress, e)
			}
			tail, err := store.Begin(ctx, h.Type, h.ID, h.InvSeq)
			if err != nil {
				t.Fatal(err)
			}
			state, err := store.InspectStart(ctx, h.Type, h.ID)
			if err != nil {
				t.Fatal(err)
			}
			started, _ := json.Marshal(map[string]any{"input_sha256": state.State.Start.InputSHA256})
			tail, err = store.Append(ctx, h.Type, h.ID, h.InvSeq, journal.Entry{Kind: journal.Started, Payload: started}, tail, [][]byte{[]byte(`7`)}, nil)
			if err != nil {
				t.Fatal(err)
			}
			payload, _ := json.Marshal(map[string]any{"sig_seq": seq, "name": "signal", "ref": "graph-signal-" + input.InputSHA256, "hash": input.InputSHA256, "canonical_signal": map[string]any{"index": 0, "token": input.Token}})
			if _, err = store.Append(ctx, h.Type, h.ID, h.InvSeq, journal.Entry{Index: 1, Kind: journal.SignalConsumed, Payload: payload}, tail, [][]byte{[]byte(`true`)}, nil); err != nil {
				t.Fatal(err)
			}
			transport.PurgeSignal(seq)
			legacy := sim.NewJournalTransport(schedule)
			legacyStore := journal.NewWithPorts(legacy, legacy)
			entered := false
			w, err := worker.NewWithPorts("replay", map[string]worker.Handler{h.Type: func(c *wf.Context, _ json.RawMessage) (json.RawMessage, error) {
				entered = true
				body, e := wf.AwaitSignal(c, "signal")
				if e != nil {
					return nil, e
				}
				if string(body) != "true" {
					return nil, fmt.Errorf("replay body changed")
				}
				return json.RawMessage(`42`), nil
			}}, worker.ModeledWorkerPorts{Journal: legacyStore, Leases: lease.NewWithKVPort(sim.NewKVTransport(schedule, 30*time.Second)), Outcome: sim.NewKVTransport(schedule, 0), Invocation: transport.SignalTransport, Signals: transport.SignalTransport, Client: c, HeartbeatTicks: make(chan time.Time), OperationNow: func() time.Time { return now }, OperationObserver: func(e worker.OperationEvent) {
				if e.Error != "" {
					t.Logf("operation: %+v", e)
				}
			}}, worker.WithGraphJournal(store))
			if err != nil {
				t.Fatal(err)
			}
			counter.hash = input.InputSHA256
			counter.reads = 0
			counter.deny = deny
			runCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()
			if deny {
				transport.Dispatch.StopAfterNextNak(cancel)
			} else {
				transport.Dispatch.StopWhenDrained(cancel)
			}
			if err = w.RunPartitionWithTransport(runCtx, identity.Partition(h.Type, h.ID, provision.Partitions), transport.Dispatch); err != nil {
				t.Fatal(err)
			}
			if counter.reads != 1 {
				events := schedule.Trace().Transport
				start := len(events) - 25
				if start < 0 {
					start = 0
				}
				for _, event := range events[start:] {
					t.Logf("trace: %+v", event)
				}
				t.Fatal("input body reads", counter.reads, "want one journal-owned fetch")
			}
			if entered == deny {
				t.Fatal("incorrect handler admission", entered, deny)
			}
			status, err := store.InspectStart(ctx, h.Type, h.ID)
			if err != nil || status.SignalConsumed != 1 || (status.Kind == journal.Completed) == deny {
				t.Fatal(status, err)
			}
			if err = model.CheckReferences(); err != nil {
				t.Fatal(err)
			}
		})
	}
}
