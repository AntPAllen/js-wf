package worker_test

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"js-wf/client"
	"js-wf/identity"
	"js-wf/internal/graphpublication"
	"js-wf/internal/retainedgraph"
	"js-wf/journal"
	"js-wf/sim"
	"js-wf/wf"
	"js-wf/worker"
)

type replayRangePort struct {
	*sim.GraphPublicationTransport
	clock            *time.Time
	gets, pinWrites  int
	advance          bool
	acquisitionDelay time.Duration
}

func (p *replayRangePort) Get(ctx context.Context, link retainedgraph.Link, limit int) ([]byte, error) {
	p.gets++
	if p.advance {
		*p.clock = p.clock.Add(200 * time.Millisecond)
	}
	return p.GraphPublicationTransport.Get(ctx, link, limit)
}

func (p *replayRangePort) CASRoot(ctx context.Context, key string, head uint64, root graphpublication.Root) (graphpublication.Root, error) {
	result, err := p.GraphPublicationTransport.CASRoot(ctx, key, head, root)
	if err == nil && len(root.Readers) != 0 {
		p.pinWrites++
		*p.clock = p.clock.Add(p.acquisitionDelay)
		p.acquisitionDelay = 0
	}
	return result, err
}

func TestGraphReplaySnapshotRangeAndSlowAcquisition(t *testing.T) {
	for _, slow := range []bool{false, true} {
		t.Run(map[bool]string{false: "census", true: "slow-acquisition"}[slow], func(t *testing.T) {
			ctx := context.Background()
			now := time.Unix(1000, 0).UTC()
			schedule := sim.NewScheduler(75)
			port := &replayRangePort{GraphPublicationTransport: sim.NewGraphPublicationTransport(schedule), clock: &now}
			store, err := journal.NewGraphStore(journal.GraphConfig{Protocol: graphpublication.Protocol{Port: port}, CanonicalStarts: true, CanonicalSignals: true, Now: func() time.Time { return now }, PinTTL: 4 * time.Second})
			if err != nil {
				t.Fatal(err)
			}
			transport := sim.NewWorkerTransport(schedule, 3*time.Second)
			c, err := client.NewWithSignalPorts(transport.SignalTransport, transport.SignalTransport).WithGraphJournal(store)
			if err != nil {
				t.Fatal(err)
			}
			h, err := c.Start(ctx, "test", "range-export", []byte(`7`))
			if err != nil {
				t.Fatal(err)
			}
			status, err := store.InspectStart(ctx, h.Type, h.ID)
			if err != nil {
				t.Fatal(err)
			}
			tail, err := store.Begin(ctx, h.Type, h.ID, h.InvSeq)
			if err != nil {
				t.Fatal(err)
			}
			const count = 33
			for i := uint64(0); i < count; i++ {
				kind := journal.Suspended
				payload := []byte(`{"waiting_on":"signal:gate"}`)
				var objects [][]byte
				if i == 0 {
					kind = journal.Started
					payload, _ = json.Marshal(map[string]string{"input_sha256": status.State.Start.InputSHA256})
					objects = [][]byte{[]byte(`7`)}
				} else if i == count-1 {
					kind = journal.Completed
					payload, _ = json.Marshal(wf.Outcome{InvSeq: h.InvSeq, Result: []byte(`43`)})
				}
				tail, err = store.Append(ctx, h.Type, h.ID, h.InvSeq, journal.Entry{Index: i, Epoch: 3, Kind: kind, Payload: payload}, tail, objects, nil)
				if err != nil {
					t.Fatal(i, err)
				}
			}
			source, err := transport.LastInvocation(ctx, identity.InvocationSubject(h.Type, h.ID))
			if err != nil {
				t.Fatal(err)
			}
			view, err := store.OpenExisting(ctx, h.Type, h.ID, h.InvSeq)
			if err != nil {
				t.Fatal(err)
			}
			port.gets = 0
			if err = view.ValidateStartInvocation(ctx, source); err != nil {
				t.Fatal(err)
			}
			input, err := view.StartInput(ctx)
			if err != nil {
				t.Fatal(err)
			}
			var records []journal.Record
			for i := uint64(0); i < count; i++ {
				record, err := view.Read(ctx, i)
				if err != nil {
					t.Fatal(i, err)
				}
				records = append(records, record.Record)
				if i == 0 {
					for _, link := range record.Blobs {
						if link.Hash == status.State.Start.InputSHA256 {
							if _, err := view.Payload(ctx, 0, link, store.PayloadReadLimit()); err != nil {
								t.Fatal(err)
							}
						}
					}
				}
			}
			pointGets := port.gets
			if err = view.Close(ctx); err != nil {
				t.Fatal(err)
			}
			port.gets, port.pinWrites = 0, 0
			port.advance = slow
			if slow {
				port.acquisitionDelay = 3 * time.Second
			}
			start := now
			snapshot, err := worker.ReadGraphReplaySnapshot(ctx, store, h.Type, h.ID, source)
			if err != nil || !reflect.DeepEqual(snapshot.Records, records) || string(snapshot.Input) != string(input) || string(snapshot.Objects["input:"+status.State.Start.InputSHA256]) != "7" {
				t.Fatal("range export differs from point history or owned payload", err, len(snapshot.Records))
			}
			if port.gets*3 >= pointGets*2 {
				t.Fatal("range export did not reduce object reads by a third", port.gets, pointGets)
			}
			if slow && (now.Sub(start) <= 4*time.Second || port.pinWrites < 3) {
				t.Fatal("slow read did not require repeated renewal", now.Sub(start), port.pinWrites)
			}
			t.Logf("REPLAY_RANGE records=33 range_gets=%d point_gets=%d elapsed=%s pin_ttl=4s pin_writes=%d", port.gets, pointGets, now.Sub(start), port.pinWrites)
		})
	}
}
