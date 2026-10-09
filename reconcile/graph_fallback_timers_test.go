package reconcile_test

import (
	"context"
	"encoding/json"
	"errors"
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
	"js-wf/testcluster"
	"js-wf/wf"
)

type graphFallbackPort struct {
	source                        *sim.SignalTransport
	timer                         *jetstream.RawStreamMsg
	held, unknownLease, forged    bool
	publishUnknown, deleteUnknown bool
	publications, deletions       int
}

func (p *graphFallbackPort) LastTimerSequence(context.Context) (uint64, error) { return 1, nil }
func (p *graphFallbackPort) GetTimer(context.Context, uint64) (*jetstream.RawStreamMsg, error) {
	return p.timer, nil
}
func (p *graphFallbackPort) StateValue(context.Context, string) ([]byte, error) {
	panic("graph fallback consulted legacy state")
}
func (p *graphFallbackPort) LastInvocation(ctx context.Context, subject string) (*jetstream.RawStreamMsg, error) {
	input, err := p.source.LastInvocation(ctx, subject)
	if err == nil && p.forged {
		input.Header.Set(journal.GraphStartTokenHeader, "forged")
	}
	return input, err
}
func (p *graphFallbackPort) GraphRepairBlocked(context.Context, string, string) (bool, error) {
	if p.unknownLease {
		return false, context.DeadlineExceeded
	}
	return p.held, nil
}
func (p *graphFallbackPort) PublishWakeup(context.Context, *nats.Msg, string) error {
	p.publications++
	if p.publishUnknown {
		return context.DeadlineExceeded
	}
	return nil
}
func (p *graphFallbackPort) DeleteTimer(context.Context, uint64) error {
	if p.deleteUnknown {
		return context.DeadlineExceeded
	}
	p.deletions++
	return nil
}

func TestGraphFallbackTimerCanonicalDecisions(t *testing.T) {
	for _, mode := range []string{"due", "held", "lease-unknown", "source-forged", "hint-forged", "publish-unknown", "delete-unknown", "authority-unknown", "terminal"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			schedule := sim.NewScheduler(7)
			model := sim.NewGraphPublicationTransport(schedule)
			graph, err := journal.NewGraphStore(journal.GraphConfig{Protocol: model.Protocol(), CanonicalStarts: true, CanonicalSignals: true})
			if err != nil {
				t.Fatal(err)
			}
			transport := sim.NewSignalTransport(schedule)
			c, err := client.NewWithSignalPorts(transport, transport).WithGraphJournal(graph)
			if err != nil {
				t.Fatal(err)
			}
			h, err := c.Start(ctx, "test", "fallback", []byte(`7`))
			if err != nil {
				t.Fatal(err)
			}
			status, err := graph.InspectStart(ctx, h.Type, h.ID)
			if err != nil {
				t.Fatal(err)
			}
			fire := time.Unix(1000, 0).UTC()
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
			if mode == "terminal" {
				outcome, _ := json.Marshal(wf.Outcome{InvSeq: h.InvSeq, Result: []byte(`42`)})
				_, err = graph.Append(ctx, h.Type, h.ID, h.InvSeq, journal.Entry{Index: 3, Kind: journal.Completed, Payload: outcome}, tail, nil, nil)
				if err != nil {
					t.Fatal(err)
				}
			}
			hintFire := fire
			if mode == "hint-forged" {
				hintFire = fire.Add(-time.Hour)
			}
			data, _ := json.Marshal(map[string]any{"fire_at": hintFire, "clock_domain": "domain"})
			port := &graphFallbackPort{source: transport, timer: &jetstream.RawStreamMsg{Sequence: 1, Subject: identity.TimerSubject(h.Type, h.ID, h.InvSeq, 0), Data: data}, held: mode == "held", unknownLease: mode == "lease-unknown", forged: mode == "source-forged", publishUnknown: mode == "publish-unknown", deleteUnknown: mode == "delete-unknown"}
			scan, err := reconcile.NewFallbackTimerScanWithGraphJournalPort(port, graph, func(context.Context) (time.Time, error) {
				t.Fatal("domain timer read legacy clock")
				return time.Time{}, nil
			})
			if err != nil {
				t.Fatal(err)
			}
			scan.DomainNow = func(context.Context, string) (time.Time, error) { return fire.Add(time.Second), nil }
			if mode == "authority-unknown" {
				if err := model.QueueFault("read_root", sim.DropBeforeCommit); err != nil {
					t.Fatal(err)
				}
			}
			if mode == "due" || mode == "terminal" {
				if _, err := scan.Scan(ctx, 1, 1, true); err != nil || port.publications != 0 || port.deletions != 0 {
					t.Fatal("dry run mutated hints", err)
				}
			}
			result, err := scan.Scan(ctx, 1, 1, false)
			switch mode {
			case "due":
				if err != nil || port.publications != 1 || port.deletions != 1 {
					t.Fatal(result, err, port)
				}
			case "held":
				if err != nil || port.publications != 0 || port.deletions != 0 {
					t.Fatal(result, err, port)
				}
			case "terminal":
				if err != nil || port.publications != 0 || port.deletions != 1 {
					t.Fatal(result, err, port)
				}
			case "lease-unknown", "source-forged", "hint-forged", "authority-unknown":
				if err == nil || port.publications != 0 || port.deletions != 0 {
					t.Fatal(result, err, port)
				}
			case "publish-unknown", "delete-unknown":
				if !errors.Is(err, context.DeadlineExceeded) || port.deletions != 0 {
					t.Fatal(result, err, port)
				}
			}
			t.Log(fmt.Sprintf("mode=%s publications=%d deletions=%d error=%v", mode, port.publications, port.deletions, err))
		})
	}
}

func TestNativeGraphFallbackTimerIgnoresForgedState(t *testing.T) {
	cluster, err := testcluster.Start(t.TempDir(), 1)
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	js, err := jetstream.New(cluster.Clients[0])
	if err != nil {
		t.Fatal(err)
	}
	if err := provision.EnsureFallback(ctx, js, 1); err != nil {
		t.Fatal(err)
	}
	cfg := journal.NativeGraphConfig{AuthorityStream: "FALLBACK_AUTH", AuthorityPrefix: "wf.graph.fallback", ObjectBucket: "FALLBACK_OBJECTS", ExpectedReplicas: 1, CanonicalStarts: true, CanonicalSignals: true}
	configs, err := journal.NativeGraphStreamConfigs(cfg, 1)
	if err != nil {
		t.Fatal(err)
	}
	for _, config := range configs {
		if _, err := js.CreateStream(ctx, config); err != nil {
			t.Fatal(err)
		}
	}
	graph, err := journal.OpenNativeGraphStore(ctx, js, cfg)
	if err != nil {
		t.Fatal(err)
	}
	c, err := client.New(js).WithGraphJournal(graph)
	if err != nil {
		t.Fatal(err)
	}
	h, err := c.Start(ctx, "test", "fallback-native", []byte(`7`))
	if err != nil {
		t.Fatal(err)
	}
	status, err := graph.InspectStart(ctx, h.Type, h.ID)
	if err != nil {
		t.Fatal(err)
	}
	fire := time.Now().UTC().Add(-time.Second)
	started, _ := json.Marshal(map[string]string{"input_sha256": status.State.Start.InputSHA256})
	request, _ := json.Marshal(map[string]any{"kind": "timer", "name": "sleep", "fire_at": fire})
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
	state, err := js.KeyValue(ctx, "WF_STATE")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := state.Put(ctx, "purging."+identity.Key(h.Type, h.ID), []byte(fmt.Sprint(h.InvSeq))); err != nil {
		t.Fatal(err)
	}
	if _, err := state.Put(ctx, identity.Key(h.Type, h.ID), []byte(`invalid legacy state`)); err != nil {
		t.Fatal(err)
	}
	data, _ := json.Marshal(map[string]any{"fire_at": fire})
	subject := identity.TimerSubject(h.Type, h.ID, h.InvSeq, 0)
	hint, err := js.Publish(ctx, subject, data)
	if err != nil {
		t.Fatal(err)
	}
	runs, err := js.Stream(ctx, "WF_RUN")
	if err != nil {
		t.Fatal(err)
	}
	if err := runs.Purge(ctx); err != nil {
		t.Fatal(err)
	}
	scanner, err := reconcile.NewFallbackTimerScanWithGraphJournal(js, graph)
	if err != nil {
		t.Fatal(err)
	}
	result, err := scanner.Scan(ctx, 1, 10, false)
	if err != nil || result.Reenqueued != 1 || result.Removed != 0 {
		t.Fatal(result, err)
	}
	wakeup, err := runs.GetLastMsgForSubject(ctx, identity.RunSubject(h.Type, h.ID, provision.Partitions))
	if err != nil || wakeup.Header.Get(identity.TimerInvSeqHeader) != fmt.Sprint(h.InvSeq) || wakeup.Header.Get(identity.TimerStepHeader) != "0" {
		t.Fatal("canonical due wakeup missing", wakeup, err)
	}
	timers, err := js.Stream(ctx, "WF_TIMER")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := timers.GetMsg(ctx, hint.Sequence); !errors.Is(err, jetstream.ErrMsgNotFound) {
		t.Fatal("acknowledged hint retained", err)
	}
	// Once the canonical generation terminates, physical hints are disposable
	// even when the legacy mirror remains forged.
	outcome, _ := json.Marshal(wf.Outcome{InvSeq: h.InvSeq, Result: []byte(`42`)})
	if _, err := graph.Append(ctx, h.Type, h.ID, h.InvSeq, journal.Entry{Index: 3, Kind: journal.Completed, Payload: outcome}, tail, nil, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := js.Publish(ctx, subject, data); err != nil {
		t.Fatal(err)
	}
	result, err = scanner.Scan(ctx, 1, 10, false)
	if err != nil || result.Removed != 1 || result.Reenqueued != 0 {
		t.Fatal("canonical terminal hint retirement", result, err)
	}
}
