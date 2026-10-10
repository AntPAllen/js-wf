package sim

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"js-wf/client"
	"js-wf/identity"
	"js-wf/internal/graphpublication"
	"js-wf/journal"
	"js-wf/wf"
	"js-wf/worker"
)

var graphReplayLifecycleModes = []string{"healthy", "before_pin_retire", "before_pin_replace", "after_pin_retire", "after_pin_replace"}

type graphReplayLifecycleCut struct {
	*GraphPublicationTransport
	hook  func() error
	after bool
	fired bool
}

func (p *graphReplayLifecycleCut) CASRoot(ctx context.Context, key string, head uint64, root graphpublication.Root) (graphpublication.Root, error) {
	hook := p.hook
	cut := hook != nil && len(root.Readers) > 0
	if cut {
		p.hook = nil
		p.fired = true
		if !p.after {
			if err := hook(); err != nil {
				return graphpublication.Root{}, err
			}
		}
	}
	result, err := p.GraphPublicationTransport.CASRoot(ctx, key, head, root)
	if err == nil && cut && p.after {
		if e := hook(); e != nil {
			return graphpublication.Root{}, e
		}
	}
	return result, err
}

func runGraphReplayLifecycle(seed int64, replay *Trace) (trace Trace, runErr error) {
	schedule := NewScheduler(seed)
	if replay != nil {
		var err error
		schedule, err = ReplayScheduler(*replay)
		if err != nil {
			return trace, err
		}
	}
	if err := schedule.SetWorkload("graph_replay_lifecycle"); err != nil {
		return trace, err
	}
	defer func() { trace = schedule.Trace() }()
	mode, err := schedule.Choose(graphReplayLifecycleModes)
	if err != nil {
		return trace, err
	}
	size, err := schedule.Choose([]string{"small", "large"})
	if err != nil {
		return trace, err
	}
	padding := 16
	if size == "large" {
		padding = 4096
	}
	oldInput := fmt.Sprintf(`{"value":7,"padding":"%s"}`, strings.Repeat("x", padding))
	ctx := context.Background()
	model := NewGraphPublicationTransport(schedule)
	cut := &graphReplayLifecycleCut{GraphPublicationTransport: model, after: strings.HasPrefix(mode, "after_")}
	protocol := model.Protocol()
	protocol.Port = cut
	now := func() time.Time { return time.UnixMilli(schedule.NowMillis()).UTC() }
	store, err := journal.NewGraphStore(journal.GraphConfig{Protocol: protocol, CanonicalStarts: true, CanonicalSignals: true, Now: now})
	if err != nil {
		return trace, err
	}
	transport := NewWorkerTransport(schedule, 3*time.Second)
	c, err := client.NewWithSignalPorts(transport.SignalTransport, transport.SignalTransport).WithGraphJournal(store)
	if err != nil {
		return trace, err
	}
	appendHistory := func(h client.Handle, input, result string, epoch uint64) error {
		status, e := store.InspectStart(ctx, h.Type, h.ID)
		if e != nil {
			return e
		}
		tail, e := store.Begin(ctx, h.Type, h.ID, h.InvSeq)
		if e != nil {
			return e
		}
		started, _ := json.Marshal(map[string]string{"input_sha256": status.State.Start.InputSHA256})
		tail, e = store.Append(ctx, h.Type, h.ID, h.InvSeq, journal.Entry{Kind: journal.Started, Epoch: epoch, Payload: started}, tail, [][]byte{[]byte(input)}, nil)
		if e != nil {
			return e
		}
		terminal, _ := json.Marshal(wf.Outcome{InvSeq: h.InvSeq, Result: []byte(result)})
		_, e = store.Append(ctx, h.Type, h.ID, h.InvSeq, journal.Entry{Kind: journal.Completed, Epoch: epoch, Index: 1, Payload: terminal}, tail, nil, nil)
		return e
	}
	h, err := c.Start(ctx, "test", "replaylifecycle", []byte(oldInput))
	if err != nil {
		return trace, err
	}
	if err = appendHistory(h, oldInput, "42", 3); err != nil {
		return trace, err
	}
	source, err := transport.LastInvocation(ctx, identity.InvocationSubject(h.Type, h.ID))
	if err != nil {
		return trace, err
	}
	var replacement client.Handle
	if mode != "healthy" {
		cut.hook = func() error {
			if e := store.Retire(ctx, h.Type, h.ID, h.InvSeq, 2); e != nil {
				return e
			}
			if strings.HasSuffix(mode, "replace") {
				transport.PurgeInvocation(identity.InvocationSubject(h.Type, h.ID))
				var e error
				replacement, e = c.Start(ctx, h.Type, h.ID, []byte(`8`))
				if e != nil {
					return e
				}
				return appendHistory(replacement, "8", "99", 0)
			}
			return nil
		}
	}
	checkEmpty := func(s worker.GraphReplaySnapshot) bool {
		return s.Format == "" && s.InputHash == "" && s.Input == nil && s.Records == nil && s.Objects == nil && s.PendingSignal == nil
	}
	check := func(s worker.GraphReplaySnapshot, input, result string, inv, epoch uint64) error {
		if s.Format != wf.ReplayFormatGraphV1 || string(s.Input) != input || len(s.Records) != 2 || s.Records[0].Index != 0 || s.Records[0].Epoch != epoch {
			return fmt.Errorf("mode=%s mixed snapshot", mode)
		}
		var out wf.Outcome
		if json.Unmarshal(s.Records[1].Payload, &out) != nil || out.InvSeq != inv || string(out.Result) != result {
			return fmt.Errorf("mode=%s mixed terminal", mode)
		}
		return nil
	}
	snapshot, err := worker.ReadGraphReplaySnapshot(ctx, store, h.Type, h.ID, source)
	if mode == "healthy" || cut.after {
		if err != nil {
			return trace, err
		}
		if err = check(snapshot, oldInput, "42", h.InvSeq, 3); err != nil {
			return trace, err
		}
	} else if !errors.Is(err, journal.ErrStale) || !checkEmpty(snapshot) {
		return trace, fmt.Errorf("mode=%s old admission err=%v empty=%t", mode, err, checkEmpty(snapshot))
	}
	if mode != "healthy" {
		if !cut.fired {
			return trace, fmt.Errorf("mode=%s cut absent", mode)
		}
		stale, e := worker.ReadGraphReplaySnapshot(ctx, store, h.Type, h.ID, source)
		if !errors.Is(e, journal.ErrStale) || !checkEmpty(stale) {
			return trace, fmt.Errorf("mode=%s stale reacquisition err=%v", mode, e)
		}
	}
	if replacement.InvSeq != 0 {
		if replacement.InvSeq == h.InvSeq {
			return trace, fmt.Errorf("generation unchanged")
		}
		current, e := transport.LastInvocation(ctx, identity.InvocationSubject(h.Type, h.ID))
		if e != nil {
			return trace, e
		}
		fresh, e := worker.ReadGraphReplaySnapshot(ctx, store, h.Type, h.ID, current)
		if e != nil {
			return trace, e
		}
		if e = check(fresh, "8", "99", replacement.InvSeq, 0); e != nil {
			return trace, e
		}
	}
	if err = schedule.Finish(); err != nil {
		return trace, err
	}
	return schedule.Trace(), nil
}

func TestSeededGraphReplayLifecycleReplay(t *testing.T) {
	covered := map[string]bool{}
	counts := map[string]int{}
	dimensions := map[string]int{}
	fail := func(seed int64, trace Trace, err error) {
		t.Helper()
		path := os.Getenv("FAULT_TRACE_OUT")
		if path == "" {
			root, e := os.MkdirTemp("", "js-wf-graph-replay-lifecycle-failure-")
			if e != nil {
				t.Fatalf("seed=%d: %v; trace directory: %v", seed, err, e)
			}
			path = filepath.Join(root, "trace.json")
		}
		if e := trace.Save(path); e != nil {
			t.Fatalf("seed=%d: %v; save trace: %v", seed, err, e)
		}
		t.Fatalf("FAULT_SEED=%d FAULT_TRACE=%s: %v", seed, path, err)
	}
	for seed := range seededSchedules(t) {
		generated, err := runGraphReplayLifecycle(seed, nil)
		if err != nil {
			fail(seed, generated, err)
		}
		replayed, err := runGraphReplayLifecycle(seed, &generated)
		if err != nil || !reflect.DeepEqual(generated, replayed) {
			fail(seed, generated, fmt.Errorf("replay mismatch: %v", err))
		}
		mode := generated.Decisions[0].Chosen
		counts[mode]++
		dimensions[mode+"/"+generated.Decisions[1].Chosen]++
		if !covered[mode] && os.Getenv("SIM_GRAPH_REPLAY_LIFECYCLE_ROOT") != "" {
			root := os.Getenv("SIM_GRAPH_REPLAY_LIFECYCLE_ROOT")
			if err := os.MkdirAll(root, 0700); err != nil {
				t.Fatal(err)
			}
			if err := generated.Save(filepath.Join(root, mode+".json")); err != nil {
				t.Fatal(err)
			}
		}
		covered[mode] = true
	}
	if len(covered) != len(graphReplayLifecycleModes) {
		t.Fatalf("mode coverage=%v", covered)
	}
	if len(dimensions) != 2*len(graphReplayLifecycleModes) {
		t.Fatalf("size/mode coverage=%v", dimensions)
	}
	body, _ := json.Marshal(counts)
	t.Logf("GRAPH_REPLAY_LIFECYCLE modes=%s exact_replay=true", body)
	body, _ = json.Marshal(dimensions)
	t.Logf("GRAPH_REPLAY_LIFECYCLE dimensions=%s", body)
}
