package sim

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"js-wf/client"
	"js-wf/identity"
	"js-wf/journal"
	"js-wf/retention"
	"js-wf/wf"
)

type graphResultTransport struct {
	source               *SignalTransport
	state                *KVTransport
	wait                 func() error
	invocationHook       func() error
	stateHook            func() error
	invReads, stateReads int
}

func (p *graphResultTransport) LastInvocation(ctx context.Context, subject string) (*jetstream.RawStreamMsg, error) {
	p.invReads++
	if p.invReads == 2 && p.invocationHook != nil {
		if err := p.invocationHook(); err != nil {
			return nil, err
		}
	}
	return p.source.LastInvocation(ctx, subject)
}
func (p *graphResultTransport) State(ctx context.Context, key string) ([]byte, error) {
	p.stateReads++
	if p.stateReads == 2 && p.stateHook != nil {
		if err := p.stateHook(); err != nil {
			return nil, err
		}
	}
	entry, err := p.state.Get(ctx, key)
	return entry.Value, err
}
func (p *graphResultTransport) Wait(ctx context.Context, d time.Duration) error {
	if err := p.source.Wait(ctx, d); err != nil {
		return err
	}
	if p.wait != nil {
		f := p.wait
		p.wait = nil
		return f()
	}
	return nil
}

var graphResultModes = []string{"inline", "external", "failed", "cancelled", "absent_state", "poisoned_state", "pending", "replaced_invocation", "purge_marker", "retired", "unknown_root", "unknown_pin", "foreign_payload"}

func runGraphResult(seed int64, replay *Trace) (trace Trace, runErr error) {
	schedule := NewScheduler(seed)
	if replay != nil {
		var err error
		schedule, err = ReplayScheduler(*replay)
		if err != nil {
			return trace, err
		}
	}
	if err := schedule.SetWorkload("graph_terminal_client"); err != nil {
		return trace, err
	}
	defer func() { trace = schedule.Trace() }()
	mode, err := schedule.Choose(graphResultModes)
	if err != nil {
		return trace, err
	}
	ctx, stop := context.WithTimeout(context.Background(), 5*time.Second)
	defer stop()
	m := NewGraphPublicationTransport(schedule)
	now := func() time.Time { return time.UnixMilli(schedule.NowMillis()).UTC() }
	store, err := journal.NewGraphStore(journal.GraphConfig{Protocol: m.Protocol(), Now: now, PinTTL: 30 * time.Second, IntentTTL: time.Second})
	if err != nil {
		return trace, err
	}
	source := NewSignalTransport(schedule)
	state := NewKVTransport(schedule, 0)
	port := &graphResultTransport{source: source, state: state}
	c, err := client.NewWithGraphJournalPorts(source, source, port, store)
	if err != nil {
		return trace, err
	}
	const typ, id = "test", "result"
	handle, err := c.Start(ctx, typ, id, []byte(`7`))
	if err != nil {
		return trace, err
	}
	tail, err := store.Begin(ctx, typ, id, handle.InvSeq)
	if err != nil {
		return trace, err
	}
	tail, err = store.Append(ctx, typ, id, handle.InvSeq, journal.Entry{Kind: journal.Started}, tail, nil, nil)
	if err != nil {
		return trace, err
	}
	want := []byte(`42`)
	terminal := wf.Outcome{InvSeq: handle.InvSeq, Result: want}
	kind := journal.Completed
	var payloads [][]byte
	if mode == "external" || mode == "foreign_payload" {
		want = bytes.Repeat([]byte("result"), 100)
		sum := sha256.Sum256(want)
		hash := hex.EncodeToString(sum[:])
		terminal = wf.Outcome{InvSeq: handle.InvSeq, ResultRef: "terminal-result-" + hash, ResultHash: hash}
		if mode == "external" {
			payloads = [][]byte{want}
		}
	}
	if mode == "failed" || mode == "cancelled" {
		kind = journal.Failed
		reason := "handler failed"
		if mode == "cancelled" {
			reason = client.ErrCancelled.Error()
		}
		terminal = wf.Outcome{InvSeq: handle.InvSeq, Error: reason}
		want = nil
	}
	body, _ := json.Marshal(terminal)
	finish := func() error {
		var e error
		tail, e = store.Append(ctx, typ, id, handle.InvSeq, journal.Entry{Kind: kind, Index: 1, Payload: body}, tail, payloads, nil)
		return e
	}
	if mode == "pending" {
		port.wait = finish
	} else if err = finish(); err != nil {
		return trace, err
	}
	mirrored := body
	if mode == "poisoned_state" {
		mirrored = []byte(fmt.Sprintf(`{"inv_seq":%d,"error":"forged failure"}`, handle.InvSeq))
	}
	if mode != "absent_state" {
		if _, err = state.Create(ctx, identity.Key(typ, id), mirrored); err != nil {
			return trace, err
		}
	}
	if mode == "replaced_invocation" {
		port.invocationHook = func() error {
			source.PurgeInvocation(identity.InvocationSubject(typ, id))
			_, e := c.Start(ctx, typ, id, []byte(`7`))
			return e
		}
	}
	if mode == "purge_marker" {
		port.stateHook = func() error {
			old, e := state.Get(ctx, identity.Key(typ, id))
			if e != nil {
				return e
			}
			tomb, _ := json.Marshal(retention.Tombstone{Tombstone: true, InvSeq: handle.InvSeq, PurgedAt: now(), ExpiresAt: now().Add(time.Hour)})
			_, e = state.Update(ctx, identity.Key(typ, id), tomb, old.Revision)
			return e
		}
	}
	if mode == "retired" {
		if err = store.Retire(ctx, typ, id, handle.InvSeq, tail); err != nil {
			return trace, err
		}
	}
	if mode == "unknown_root" {
		if err = m.QueueFault("read_root", DropBeforeCommit); err != nil {
			return trace, err
		}
	}
	if mode == "unknown_pin" {
		if err = m.QueueFault("cas_root", LoseAckAfterCommit); err != nil {
			return trace, err
		}
		m.PauseBefore("cas_root", func() error { return m.QueueFault("read_root", DropBeforeCommit) })
	}
	got, err := c.Await(ctx, typ, id)
	switch mode {
	case "retired", "purge_marker", "replaced_invocation":
		if !errors.Is(err, client.ErrPurged) {
			return trace, fmt.Errorf("%s admitted retired/replaced result: %v", mode, err)
		}
	case "unknown_root", "unknown_pin":
		if !errors.Is(err, journal.ErrUnknown) {
			return trace, fmt.Errorf("uncertain graph read: %v", err)
		}
	case "foreign_payload":
		if !errors.Is(err, wf.ErrCorruptJournal) {
			return trace, fmt.Errorf("unowned terminal payload: %v", err)
		}
	case "failed", "cancelled":
		if err == nil || err.Error() != terminal.Error {
			return trace, fmt.Errorf("terminal failure differs: %v", err)
		}
		_, again := c.Await(ctx, typ, id)
		if again == nil || again.Error() != err.Error() {
			return trace, fmt.Errorf("failure changed: %v", again)
		}
	default:
		if err != nil || !bytes.Equal(got, want) {
			return trace, fmt.Errorf("canonical terminal differs: %v", err)
		}
		again, e := c.Await(ctx, typ, id)
		if e != nil || !bytes.Equal(again, want) {
			return trace, fmt.Errorf("repeated terminal differs: %v", e)
		}
	}
	if mode == "pending" && port.wait != nil {
		return trace, fmt.Errorf("client trusted terminal state before graph completion")
	}
	if old, e := state.Get(ctx, identity.Key(typ, id)); e == nil {
		if err = state.Delete(ctx, identity.Key(typ, id), old.Revision); err != nil {
			return trace, err
		}
	}
	source.PurgeInvocation(identity.InvocationSubject(typ, id))
	if mode != "retired" {
		if err = store.Retire(ctx, typ, id, handle.InvSeq, tail); err != nil {
			return trace, err
		}
	}
	if err = schedule.AdvanceMillis(60000); err != nil {
		return trace, err
	}
	if _, err = m.Protocol().SweepWithReaders(ctx, now()); err != nil {
		return trace, err
	}
	objects, err := m.Objects(ctx)
	if err != nil || len(objects) != 0 {
		return trace, fmt.Errorf("terminal graph did not drain: %v", err)
	}
	if err = m.CheckReferences(); err != nil {
		return trace, err
	}
	schedule.RecordTransport(TransportEvent{Operation: "check_graph_terminal_client", Outcome: mode})
	if err = schedule.Finish(); err != nil {
		return trace, err
	}
	return trace, nil
}
func TestSeededGraphResultReplay(t *testing.T) {
	observed := map[string]int{}
	fail := func(seed int64, trace Trace, cause error) {
		path := os.Getenv("FAULT_TRACE_OUT")
		if path == "" {
			dir, e := os.MkdirTemp("", "js-wf-graph-result-failure-")
			if e != nil {
				t.Fatal(e)
			}
			path = filepath.Join(dir, "trace.json")
		}
		if e := trace.Save(path); e != nil {
			t.Fatal(e)
		}
		t.Fatalf("FAULT_SEED=%d FAULT_TRACE=%s: %v", seed, path, cause)
	}
	for seed := range seededSchedules(t) {
		generated, e := runGraphResult(seed, nil)
		if e != nil {
			fail(seed, generated, e)
		}
		mode := generated.Decisions[0].Chosen
		observed[mode]++
		replayed, e := runGraphResult(seed, &generated)
		if e != nil || !reflect.DeepEqual(generated, replayed) {
			fail(seed, generated, fmt.Errorf("result replay differs: %v", e))
		}
		if dir := os.Getenv("SIM_GRAPH_RESULT_ROOT"); dir != "" && observed[mode] == 1 {
			if e = generated.Save(filepath.Join(dir, mode+".json")); e != nil {
				t.Fatal(e)
			}
		}
	}
	if len(observed) != len(graphResultModes) {
		t.Fatalf("result coverage=%v", observed)
	}
	t.Logf("graph results: modes=%v; canonical inline/external/failure, identity/purge fencing, uncertainty and complete drain", observed)
}
