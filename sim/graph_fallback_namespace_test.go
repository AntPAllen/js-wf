package sim

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"js-wf/client"
	"js-wf/identity"
	"js-wf/journal"
	"js-wf/reconcile"
)

var fallbackNamespaceModes = []string{"healthy", "read_unknown", "publish_drop", "publish_ack", "delete_drop", "delete_ack"}

type graphFallbackNamespacePort struct {
	*SignalTransport
	s         *Scheduler
	timers    map[uint64]*jetstream.RawStreamMsg
	owner     string
	mode      string
	faultUsed bool
	wakes     map[string]string
	deleted   map[uint64]string
}

func (p *graphFallbackNamespacePort) event(op string, seq uint64, outcome string) {
	p.s.RecordTransport(TransportEvent{Operation: "graph_fallback_namespace_" + op, Subject: p.owner, Sequence: seq, Outcome: outcome, AtMillis: p.s.NowMillis()})
}
func (p *graphFallbackNamespacePort) LastTimerSequence(context.Context) (uint64, error) {
	p.event("last", 2, "ok")
	return 2, nil
}
func (p *graphFallbackNamespacePort) GetTimer(_ context.Context, seq uint64) (*jetstream.RawStreamMsg, error) {
	if timer := p.timers[seq]; timer != nil {
		p.event("get", seq, "ok")
		return timer, nil
	}
	p.event("get", seq, "missing")
	return nil, jetstream.ErrMsgNotFound
}
func (*graphFallbackNamespacePort) StateValue(context.Context, string) ([]byte, error) {
	return nil, fmt.Errorf("graph namespace consulted legacy state")
}
func (p *graphFallbackNamespacePort) GraphRepairBlocked(context.Context, string, string) (bool, error) {
	p.event("lease", 0, "free")
	return false, nil
}
func (p *graphFallbackNamespacePort) PublishWakeup(_ context.Context, msg *nats.Msg, messageID string) error {
	if string(msg.Data) != identity.Key("test", p.owner) {
		return fmt.Errorf("foreign namespace wakeup")
	}
	if p.owner == "a" && !p.faultUsed && p.mode == "publish_drop" {
		p.faultUsed = true
		p.event("publish", 0, "drop")
		return nats.ErrTimeout
	}
	encoded, _ := json.Marshal(msg)
	hash := digest(encoded)
	if old, ok := p.wakes[messageID]; ok && old != hash {
		return fmt.Errorf("dedup payload changed")
	}
	outcome := "committed"
	if _, ok := p.wakes[messageID]; ok {
		outcome = "dedup"
	}
	p.wakes[messageID] = hash
	if p.owner == "a" && !p.faultUsed && p.mode == "publish_ack" {
		p.faultUsed = true
		p.event("publish", 0, "lost_ack")
		return nats.ErrTimeout
	}
	p.event("publish", 0, outcome)
	return nil
}
func (p *graphFallbackNamespacePort) DeleteTimer(_ context.Context, seq uint64) error {
	timer := p.timers[seq]
	if timer == nil {
		return jetstream.ErrMsgNotFound
	}
	// A wakeup/deletion must be attributable to this store's own invocation.
	if !strings.HasPrefix(timer.Subject, "wf.timer.test."+p.owner+".") {
		return fmt.Errorf("foreign namespace deletion")
	}
	if p.owner == "a" && !p.faultUsed && p.mode == "delete_drop" {
		p.faultUsed = true
		p.event("delete", seq, "drop")
		return nats.ErrTimeout
	}
	p.deleted[seq] = p.owner
	delete(p.timers, seq)
	if p.owner == "a" && !p.faultUsed && p.mode == "delete_ack" {
		p.faultUsed = true
		p.event("delete", seq, "lost_ack")
		return nats.ErrTimeout
	}
	p.event("delete", seq, "committed")
	return nil
}

func runGraphFallbackNamespace(seed int64, replay *Trace) (trace Trace, runErr error) {
	s := NewScheduler(seed)
	if replay != nil {
		var err error
		s, err = ReplayScheduler(*replay)
		if err != nil {
			return trace, err
		}
	}
	if err := s.SetWorkload("graph_fallback_namespace"); err != nil {
		return trace, err
	}
	defer func() { trace = s.Trace() }()
	mode, err := s.Choose(fallbackNamespaceModes)
	if err != nil {
		return trace, err
	}
	order, err := s.Choose([]string{"foreign_first", "owned_first"})
	if err != nil {
		return trace, err
	}
	batch, err := s.Choose([]string{"1", "2"})
	if err != nil {
		return trace, err
	}
	budget, _ := strconv.Atoi(batch)
	ctx := context.Background()
	models := []*GraphPublicationTransport{NewGraphPublicationTransport(s), NewGraphPublicationTransport(s)}
	source := NewSignalTransport(s)
	stores := make([]*journal.GraphStore, 2)
	port := &graphFallbackNamespacePort{SignalTransport: source, s: s, timers: map[uint64]*jetstream.RawStreamMsg{}, owner: "a", mode: mode, wakes: map[string]string{}, deleted: map[uint64]string{}}
	fire := time.Unix(1000, 0).UTC()
	ownedSeq := uint64(2)
	if order == "owned_first" {
		ownedSeq = 1
	}
	for i, id := range []string{"a", "b"} {
		g, e := journal.NewGraphStore(journal.GraphConfig{Protocol: models[i].Protocol(), CanonicalStarts: true, CanonicalSignals: true, Now: func() time.Time { return fire.Add(time.Duration(s.NowMillis()) * time.Millisecond) }})
		if e != nil {
			return trace, e
		}
		stores[i] = g
		c, e := client.NewWithSignalPorts(source, source).WithGraphJournal(g)
		if e != nil {
			return trace, e
		}
		h, e := c.Start(ctx, "test", id, []byte(`7`))
		if e != nil {
			return trace, e
		}
		status, e := g.InspectStart(ctx, h.Type, h.ID)
		if e != nil {
			return trace, e
		}
		started, _ := json.Marshal(map[string]string{"input_sha256": status.State.Start.InputSHA256})
		request, _ := json.Marshal(map[string]any{"kind": "timer", "name": "sleep", "fire_at": fire, "clock_domain": "domain"})
		tail, e := g.Begin(ctx, h.Type, h.ID, h.InvSeq)
		if e != nil {
			return trace, e
		}
		for index, entry := range []journal.Entry{{Kind: journal.Started, Payload: started}, {Kind: journal.StepRequested, Payload: request}, {Kind: journal.Suspended, Payload: []byte(`{"waiting_on":"timer"}`)}} {
			entry.Index = uint64(index)
			var payloads [][]byte
			if index == 0 {
				payloads = [][]byte{[]byte(`7`)}
			}
			tail, e = g.Append(ctx, h.Type, h.ID, h.InvSeq, entry, tail, payloads, nil)
			if e != nil {
				return trace, e
			}
		}
		seq := ownedSeq
		if id == "b" {
			seq = 3 - ownedSeq
		}
		data, _ := json.Marshal(map[string]any{"fire_at": fire, "clock_domain": "domain"})
		port.timers[seq] = &jetstream.RawStreamMsg{Sequence: seq, Subject: identity.TimerSubject(h.Type, h.ID, h.InvSeq, 0), Data: data}
	}
	objects := []map[string][]byte{models[0].objects, models[1].objects}
	// Capture independent bytes before fault injection and scanner activity.
	before, _ := json.Marshal(objects)
	beforeCanonical := fallbackNamespaceCanonical(models)
	if mode == "read_unknown" {
		if err := models[0].QueueFault("read_root", DropBeforeCommit); err != nil {
			return trace, err
		}
	}
	for i, id := range []string{"a", "b"} {
		port.owner = id
		seq := ownedSeq
		if i == 1 {
			seq = 3 - ownedSeq
		}
		cursor := uint64(1)
		for attempt := 0; attempt < 16; attempt++ {
			scan, e := reconcile.NewFallbackTimerScanWithGraphJournalPort(port, stores[i], func(context.Context) (time.Time, error) { return time.Time{}, fmt.Errorf("legacy clock used") })
			if e != nil {
				return trace, e
			}
			scan.DomainNow = func(context.Context, string) (time.Time, error) { return fire.Add(time.Second), nil }
			r, e := scan.Scan(ctx, cursor, budget, false)
			if e != nil {
				if !errors.Is(e, nats.ErrTimeout) && !errors.Is(e, journal.ErrUnknown) {
					return trace, e
				}
				if r.RetrySequence != 0 {
					cursor = r.RetrySequence
				}
			} else {
				cursor = r.NextSequence
			}
			if i == 0 && port.timers[3-ownedSeq] == nil {
				return trace, fmt.Errorf("foreign hint removed by first namespace")
			}
			if _, done := port.deleted[seq]; done && e == nil {
				break
			}
			if e = s.AdvanceMillis(1); e != nil {
				return trace, e
			}
		}
		if port.deleted[seq] != id {
			return trace, fmt.Errorf("owned hint not repaired by replacement scanner")
		}
	}
	if mode == "read_unknown" {
		if len(models[0].faults["read_root"]) != 0 {
			return trace, fmt.Errorf("authority fault unused")
		}
	} else if mode != "healthy" && !port.faultUsed {
		return trace, fmt.Errorf("selected fault unused")
	}
	if len(port.timers) != 0 || len(port.wakes) != 2 {
		return trace, fmt.Errorf("hint/wakeup drain mismatch")
	}
	if beforeCanonical != fallbackNamespaceCanonical(models) {
		return trace, fmt.Errorf("scanner changed canonical graph/application")
	}
	after, _ := json.Marshal(objects)
	if string(before) != string(after) {
		return trace, fmt.Errorf("scanner changed graph objects")
	}
	for _, m := range models {
		if err := m.CheckReferences(); err != nil {
			return trace, err
		}
		for _, root := range m.roots {
			if len(root.Readers) != 0 {
				return trace, fmt.Errorf("scanner leaked reader")
			}
		}
	}
	s.RecordTransport(TransportEvent{Operation: "check_graph_fallback_namespace", Outcome: mode + "/" + order + "/" + batch, AtMillis: s.NowMillis()})
	if err := s.Finish(); err != nil {
		return trace, err
	}
	return s.Trace(), nil
}

func TestSeededGraphFallbackNamespaceReplay(t *testing.T) {
	observed := map[string]int{}
	for seed := range seededSchedules(t) {
		generated, err := runGraphFallbackNamespace(seed, nil)
		if err != nil {
			path, _ := saveSeedFailureTrace(t.Name(), seed, generated)
			t.Fatalf("FAULT_SEED=%d FAULT_TRACE=%s: %v", seed, path, err)
		}
		replayed, err := runGraphFallbackNamespace(seed, &generated)
		if err != nil || !reflect.DeepEqual(generated, replayed) {
			path, _ := saveSeedFailureTrace(t.Name(), seed, generated)
			t.Fatalf("FAULT_SEED=%d FAULT_TRACE=%s replay: %v", seed, path, err)
		}
		key := generated.Decisions[0].Chosen + "/" + generated.Decisions[1].Chosen + "/" + generated.Decisions[2].Chosen
		if observed[key] == 0 {
			if dir := os.Getenv("SIM_GRAPH_FALLBACK_NAMESPACE_ROOT"); dir != "" {
				if err := generated.Save(filepath.Join(dir, fmt.Sprintf("seed-%d.json", seed))); err != nil {
					t.Fatal(err)
				}
			}
		}
		observed[key]++
	}
	if len(observed) != 24 {
		t.Fatal("incomplete fallback namespace mode/order/batch coverage", observed)
	}
	t.Logf("24 fallback namespace mode/order/batch combinations; exact replay, selected fault consumption, no foreign mutations, unchanged objects, no reader leaks: %v", observed)
}

// Root heads and reader pins can change during inspection; canonical histories,
// Start/Signal lifecycle bytes and owned graph roots must remain unchanged.
func fallbackNamespaceCanonical(models []*GraphPublicationTransport) string {
	snapshots := []map[string]string{}
	for _, model := range models {
		roots := map[string]string{}
		for key, root := range model.roots {
			value, _ := json.Marshal(map[string]any{"graph": root.Graph, "streams": root.Streams, "application": root.Application, "token": root.Token})
			roots[key] = string(value)
		}
		snapshots = append(snapshots, roots)
	}
	encoded, _ := json.Marshal(snapshots)
	return string(encoded)
}
