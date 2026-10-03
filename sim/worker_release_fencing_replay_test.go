package sim

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"js-wf/client"
	"js-wf/identity"
	"js-wf/journal"
	"js-wf/lease"
	"js-wf/provision"
	"js-wf/wf"
	"js-wf/worker"
)

// Model the ownership boundary observed in R5 pause run37058644370: an
// execution fails before its heartbeat observes loss, Release sees an expired
// key, and Cleanup can safely return nil. No NATS failure cause is assumed.
func runWorkerReleaseFencing(seed int64, replay *Trace) (trace Trace, runErr error) {
	var s *Scheduler
	if replay == nil {
		s = NewScheduler(seed)
	} else {
		var err error
		s, err = ReplayScheduler(*replay)
		if err != nil {
			return trace, err
		}
	}
	if err := s.SetWorkload("worker_release_fencing"); err != nil {
		return trace, err
	}
	defer func() { trace = s.Trace() }()
	mode, err := s.Choose([]string{"retry_missing", "retry_successor", "completed_missing", "completed_successor", "already_fenced"})
	if err != nil {
		return trace, err
	}
	ctx, stop := context.WithTimeout(context.Background(), 5*time.Second)
	defer stop()
	const typ = "test"
	id := integratedWorkerIDs(1)[0]
	key := identity.Key(typ, id)
	transport := NewWorkerTransport(s, 13*time.Second)
	jt := NewJournalTransport(s)
	store := journal.NewWithPorts(jt, jt)
	kv := NewKVTransport(s, provision.LeaseTTL)
	leasing := lease.NewWithKVPort(kv)
	c := client.NewWithSignalPorts(transport.SignalTransport, transport.SignalTransport)
	var successor *lease.Lease
	var fences []worker.FencingEvent
	var dispatch []worker.DispatchEvent
	handler := func(_ *wf.Context, _ json.RawMessage) (json.RawMessage, error) {
		if err := s.AdvanceMillis(provision.LeaseTTL.Milliseconds() + 1); err != nil {
			return nil, err
		}
		if strings.HasSuffix(mode, "successor") {
			var err error
			successor, err = leasing.Acquire(ctx, typ, id, "successor")
			if err != nil {
				return nil, err
			}
		}
		if mode == "already_fenced" {
			return nil, lease.ErrLost
		}
		if strings.HasPrefix(mode, "retry") {
			return nil, context.Canceled
		}
		return json.RawMessage(`42`), nil
	}
	w, err := worker.NewWithPorts("old", map[string]worker.Handler{typ: handler}, worker.ModeledWorkerPorts{
		Journal: store, Leases: leasing, Outcome: NewKVTransport(s, 0), Invocation: transport.SignalTransport,
		Signals: transport.SignalTransport, Client: c, HeartbeatTicks: make(chan time.Time),
	}, worker.WithFencingObserver(func(e worker.FencingEvent) { fences = append(fences, e) }), worker.WithDispatchObserver(func(e worker.DispatchEvent) { dispatch = append(dispatch, e) }))
	if err != nil {
		return trace, err
	}
	if _, err := c.Start(ctx, typ, id, []byte(`null`)); err != nil {
		return trace, err
	}
	actor, stopActor := context.WithCancel(ctx)
	defer stopActor()
	transport.Dispatch.StopAfterNextNak(stopActor)
	transport.Dispatch.StopWhenDrained(stopActor)
	if err := w.RunPartitionWithTransport(actor, 0, transport.Dispatch); err != nil {
		return trace, err
	}
	if w.Metrics().FencingEvents != 1 || len(fences) != 1 {
		return trace, fmt.Errorf("lost release accounting mode=%s counter=%d events=%d want=1", mode, w.Metrics().FencingEvents, len(fences))
	}
	e := fences[0]
	reason := "lease_release_lost"
	if mode == "already_fenced" || strings.HasPrefix(mode, "completed") {
		reason = "lease_execution_lost"
	}
	if e.Worker != "old" || e.Type != typ || e.ID != id || e.Epoch == 0 || e.RunSequence == 0 || e.Delivery != 1 || e.Reason != reason || !strings.Contains(e.Error, lease.ErrLost.Error()) {
		return trace, fmt.Errorf("lost release identity mode=%s event=%+v", mode, e)
	}
	// Every mode reaches a failed ownership check; Cleanup's final result must
	// neither erase it nor cause a duplicate event during deferred release.
	lost := false
	cleaned := false
	for _, d := range dispatch {
		if d.Stage == "release_initial_error" && strings.Contains(d.Error, lease.ErrLost.Error()) {
			lost = true
		}
		if d.Stage == "release_cleanup_done" {
			cleaned = true
		}
	}
	if !lost || !cleaned {
		return trace, fmt.Errorf("mode=%s missing release/cleanup boundary", mode)
	}
	if successor != nil {
		entry, err := kv.Get(ctx, key)
		if err != nil {
			return trace, err
		}
		var value lease.Value
		if err := json.Unmarshal(entry.Value, &value); err != nil || value.Worker != "successor" || value.Epoch != successor.Epoch() || value.Epoch <= e.Epoch {
			return trace, fmt.Errorf("cleanup changed successor: %+v err=%v", value, err)
		}
	} else {
		if _, err := kv.Get(ctx, key); !errors.Is(err, jetstream.ErrKeyNotFound) {
			return trace, fmt.Errorf("expired lease retained: %v", err)
		}
	}
	records, _, err := store.Read(ctx, typ, id)
	if err != nil || len(records) != 1 || records[0].Kind != journal.Started || records[0].Epoch != e.Epoch {
		return trace, fmt.Errorf("lost delivery changed retained journal: records=%+v err=%v", records, err)
	}
	if transport.Dispatch.Pending() != 1 {
		return trace, fmt.Errorf("lost delivery acknowledged: pending=%d", transport.Dispatch.Pending())
	}
	for _, d := range dispatch {
		if d.Stage == "ack" {
			return trace, fmt.Errorf("lost delivery acknowledged despite failed ownership")
		}
	}
	s.RecordTransport(TransportEvent{Operation: "check_worker_release_fencing", Subject: key, Outcome: mode, AtMillis: s.NowMillis()})
	if err := s.Finish(); err != nil {
		return trace, err
	}
	return s.Trace(), nil
}

func TestSeededWorkerReleaseFencingReplay(t *testing.T) {
	if os.Getenv("SIM_RELEASE_FENCING_HELPER") == "1" {
		seed := int64(42)
		if raw := os.Getenv("FAULT_SEED"); raw != "" {
			var err error
			seed, err = strconv.ParseInt(raw, 10, 64)
			if err != nil {
				t.Fatal(err)
			}
		}
		trace, err := runWorkerReleaseFencing(seed, nil)
		if saveErr := trace.Save(os.Getenv("SIM_RELEASE_FENCING_OUT")); saveErr != nil {
			t.Fatal(saveErr)
		}
		if err != nil {
			t.Fatal(err)
		}
		return
	}
	covered := map[string]bool{}
	for seed := range seededSchedules(t) {
		trace, err := runWorkerReleaseFencing(seed, nil)
		if err != nil {
			path := os.Getenv("FAULT_TRACE_OUT")
			if path == "" {
				path = filepath.Join(t.TempDir(), "trace.json")
			}
			_ = trace.Save(path)
			t.Fatalf("FAULT_SEED=%d FAULT_TRACE=%s: %v", seed, path, err)
		}
		covered[trace.Decisions[0].Chosen] = true
		if seed <= 10 {
			again, err := runWorkerReleaseFencing(seed, &trace)
			if err != nil || !reflect.DeepEqual(trace, again) {
				t.Fatalf("seed=%d replay=%v", seed, err)
			}
		}
	}
	if len(covered) != 5 {
		t.Fatalf("covered %d/5 release modes", len(covered))
	}
	var data [2][]byte
	for i := range data {
		path := filepath.Join(t.TempDir(), "trace.json")
		cmd := exec.Command(os.Args[0], "-test.run=^TestSeededWorkerReleaseFencingReplay$")
		cmd.Env = append(os.Environ(), "SIM_RELEASE_FENCING_HELPER=1", "SIM_RELEASE_FENCING_OUT="+path)
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("child=%d err=%v out=%s", i, err, out)
		}
		var err error
		data[i], err = os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
	}
	if !bytes.Equal(data[0], data[1]) {
		t.Fatal("release fencing trace changed across fresh processes")
	}
}
