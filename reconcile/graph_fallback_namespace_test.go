package reconcile_test

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"js-wf/client"
	"js-wf/identity"
	"js-wf/journal"
	"js-wf/provision"
	"js-wf/reconcile"
	"js-wf/sim"
)

type namespaceFallbackPort struct {
	graphFallbackPort
	timers  map[uint64]*jetstream.RawStreamMsg
	wakeups []*nats.Msg
	deleted []uint64
}

func (p *namespaceFallbackPort) LastTimerSequence(context.Context) (uint64, error) { return 2, nil }
func (p *namespaceFallbackPort) GetTimer(_ context.Context, seq uint64) (*jetstream.RawStreamMsg, error) {
	if timer := p.timers[seq]; timer != nil {
		return timer, nil
	}
	return nil, jetstream.ErrMsgNotFound
}
func (p *namespaceFallbackPort) PublishWakeup(_ context.Context, msg *nats.Msg, _ string) error {
	p.wakeups = append(p.wakeups, msg)
	return nil
}
func (p *namespaceFallbackPort) DeleteTimer(_ context.Context, seq uint64) error {
	p.deleted = append(p.deleted, seq)
	delete(p.timers, seq)
	return nil
}

func TestGraphFallbackTimerSkipsForeignNamespace(t *testing.T) {
	for _, mode := range []string{"healthy", "authority-unknown", "local-pending"} {
		t.Run(mode, func(t *testing.T) {
			const seed = 17
			t.Logf("FAULT_SEED=%d mode=%s", seed, mode)
			ctx := context.Background()
			scheduler := sim.NewScheduler(seed)
			modelA := sim.NewGraphPublicationTransport(scheduler)
			modelB := sim.NewGraphPublicationTransport(scheduler)
			open := func(model *sim.GraphPublicationTransport) *journal.GraphStore {
				g, err := journal.NewGraphStore(journal.GraphConfig{Protocol: model.Protocol(), CanonicalStarts: true, CanonicalSignals: true})
				if err != nil {
					t.Fatal(err)
				}
				return g
			}
			a, b := open(modelA), open(modelB)
			source := sim.NewSignalTransport(scheduler)
			fire := time.Unix(1000, 0).UTC()
			makeTimer := func(graph *journal.GraphStore, id string, seq uint64) *jetstream.RawStreamMsg {
				c, err := client.NewWithSignalPorts(source, source).WithGraphJournal(graph)
				if err != nil {
					t.Fatal(err)
				}
				h, err := c.Start(ctx, "test", id, []byte(`7`))
				if err != nil {
					t.Fatal(err)
				}
				status, err := graph.InspectStart(ctx, h.Type, h.ID)
				if err != nil {
					t.Fatal(err)
				}
				started, _ := json.Marshal(map[string]string{"input_sha256": status.State.Start.InputSHA256})
				request, _ := json.Marshal(map[string]any{"kind": "timer", "name": "sleep", "fire_at": fire, "clock_domain": "domain"})
				tail, err := graph.Begin(ctx, h.Type, h.ID, h.InvSeq)
				if err != nil {
					t.Fatal(err)
				}
				for i, entry := range []journal.Entry{{Kind: journal.Started, Payload: started}, {Kind: journal.StepRequested, Payload: request}, {Kind: journal.Suspended, Payload: []byte(`{"waiting_on":"timer"}`)}} {
					entry.Index = uint64(i)
					var payloads [][]byte
					if i == 0 {
						payloads = [][]byte{[]byte(`7`)}
					}
					tail, err = graph.Append(ctx, h.Type, h.ID, h.InvSeq, entry, tail, payloads, nil)
					if err != nil {
						t.Fatal(err)
					}
				}
				data, _ := json.Marshal(map[string]any{"fire_at": fire, "clock_domain": "domain"})
				return &jetstream.RawStreamMsg{Sequence: seq, Subject: identity.TimerSubject(h.Type, h.ID, h.InvSeq, 0), Data: data}
			}
			port := &namespaceFallbackPort{graphFallbackPort: graphFallbackPort{source: source}, timers: map[uint64]*jetstream.RawStreamMsg{}}
			port.timers[1] = makeTimer(b, "foreign", 1)
			port.timers[2] = makeTimer(a, "owned", 2)
			if mode == "local-pending" {
				if _, err := a.ReserveStart(ctx, journal.GraphStartRequest{Type: "test", ID: "foreign"}, []byte(`7`)); err != nil {
					t.Fatal(err)
				}
			}
			if mode == "authority-unknown" {
				if err := modelA.QueueFault("read_root", sim.DropBeforeCommit); err != nil {
					t.Fatal(err)
				}
			}
			newScan := func(graph *journal.GraphStore) *reconcile.FallbackTimerScan {
				s, err := reconcile.NewFallbackTimerScanWithGraphJournalPort(port, graph, func(context.Context) (time.Time, error) { panic("legacy clock used") })
				if err != nil {
					t.Fatal(err)
				}
				s.DomainNow = func(context.Context, string) (time.Time, error) { return fire.Add(time.Second), nil }
				return s
			}
			result, err := newScan(a).Scan(ctx, 1, 2, false)
			if mode != "healthy" {
				if err == nil || len(port.wakeups) != 0 || len(port.deleted) != 0 || result.RetrySequence != 0 {
					t.Fatal(result, err, port)
				}
				return
			}
			if err != nil || result.Inspected != 2 || result.Reenqueued != 1 || result.Removed != 0 || result.NextSequence != 3 || len(port.deleted) != 1 || port.deleted[0] != 2 || port.timers[1] == nil || len(port.wakeups) != 1 || string(port.wakeups[0].Data) != identity.Key("test", "owned") || port.wakeups[0].Subject != identity.RunSubject("test", "owned", provision.Partitions) {
				t.Fatalf("FAULT_SEED=%d foreign hint blocked or mutated: %+v %v %+v", seed, result, err, port)
			}
			result, err = newScan(b).Scan(ctx, 1, 2, false)
			if err != nil || result.Reenqueued != 1 || len(port.deleted) != 2 || port.deleted[1] != 1 || len(port.wakeups) != 2 || string(port.wakeups[1].Data) != identity.Key("test", "foreign") || port.wakeups[1].Subject != identity.RunSubject("test", "foreign", provision.Partitions) {
				t.Fatal(fmt.Sprint(result), err, port)
			}
		})
	}
}
