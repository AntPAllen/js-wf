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
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"js-wf/client"
	"js-wf/identity"
	"js-wf/integrity"
	"js-wf/journal"
	"js-wf/provision"
	"js-wf/retention"
	"js-wf/wf"
	"js-wf/worker"
)

// This signal adapter shares the retirement model's physical maps. Dedup is
// bounded by the production two-minute window and retains sequence holes.
type promiseRetirementPort struct {
	*workerRetirementPort
	signals       map[string]runDedupEntry
	notifyFault   AppendFault
	terminalFault AppendFault
	childObject   string
	childReads    int
	unavailable   int
}

var _ client.SignalPort = (*promiseRetirementPort)(nil)

func (p *promiseRetirementPort) StateValue(ctx context.Context, key string) ([]byte, error) {
	entry, err := p.Blobs.State().Get(ctx, key)
	return entry.Value, err
}
func (p *promiseRetirementPort) LastJournal(ctx context.Context, subject string) (*jetstream.RawStreamMsg, error) {
	tail, err := p.Last(ctx, subject)
	if err != nil {
		return nil, err
	}
	return &jetstream.RawStreamMsg{Subject: subject, Sequence: tail.Sequence, Data: tail.Data}, nil
}
func (p *promiseRetirementPort) ReadJournal(ctx context.Context, typ, id string) ([]journal.Record, error) {
	return p.Journal(ctx, typ, id)
}
func (p *promiseRetirementPort) PutSignalBlob(ctx context.Context, key string, raw []byte) error {
	return p.PutObject(ctx, key, raw)
}
func (p *promiseRetirementPort) PublishSignal(ctx context.Context, m *nats.Msg, id string) (client.SignalPublishAck, error) {
	if err := ctx.Err(); err != nil {
		return client.SignalPublishAck{}, err
	}
	p.Blobs.mu.Lock()
	defer p.Blobs.mu.Unlock()
	fault := p.notifyFault
	p.notifyFault = ""
	event := TransportEvent{Operation: "retirement_signal_publish", Subject: m.Subject, DataSHA256: digest(m.Data)}
	if fault == DropBeforeCommit {
		event.Outcome = string(fault)
		p.Blobs.event(event)
		return client.SignalPublishAck{}, ErrTransportLost
	}
	if prior, ok := p.signals[id]; id != "" && ok && p.schedule.NowMillis()-prior.atMillis < (2*time.Minute).Milliseconds() {
		event.Outcome = "duplicate"
		event.Sequence = prior.sequence
		if fault == LoseAckAfterCommit {
			event.Outcome = string(fault)
			p.Blobs.event(event)
			return client.SignalPublishAck{}, ErrTransportLost
		}
		p.Blobs.event(event)
		return client.SignalPublishAck{Sequence: prior.sequence, Duplicate: true}, nil
	}
	stream := p.Blobs.streams["WF_SIG"]
	stream.last++
	stream.messages[stream.last] = retention.BlobSweepMessage{Subject: m.Subject, Header: cloneHeader(m.Header), Data: bytes.Clone(m.Data)}
	if id != "" {
		p.signals[id] = runDedupEntry{sequence: stream.last, atMillis: p.schedule.NowMillis()}
	}
	event.Sequence = stream.last
	event.Outcome = "ok"
	if fault == LoseAckAfterCommit {
		event.Outcome = string(fault)
		p.Blobs.event(event)
		return client.SignalPublishAck{}, ErrTransportLost
	}
	p.Blobs.event(event)
	return client.SignalPublishAck{Sequence: stream.last}, nil
}
func (p *promiseRetirementPort) SignalBySequence(ctx context.Context, seq uint64) (*jetstream.RawStreamMsg, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	p.Blobs.mu.Lock()
	defer p.Blobs.mu.Unlock()
	m, ok := p.Blobs.streams["WF_SIG"].messages[seq]
	event := TransportEvent{Operation: "retirement_signal_get", Sequence: seq, Outcome: "not_found"}
	if !ok {
		p.Blobs.event(event)
		return nil, jetstream.ErrMsgNotFound
	}
	event.Outcome = "ok"
	event.Subject = m.Subject
	p.Blobs.event(event)
	return &jetstream.RawStreamMsg{Sequence: seq, Subject: m.Subject, Header: cloneHeader(m.Header), Data: bytes.Clone(m.Data)}, nil
}
func (p *promiseRetirementPort) PutBytes(ctx context.Context, name string, raw []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	fault := AppendFault("")
	if strings.HasPrefix(name, "terminal-result-") {
		fault = p.terminalFault
		p.terminalFault = ""
	}
	if fault == DropBeforeCommit {
		p.schedule.RecordTransport(TransportEvent{Operation: "retirement_terminal_put", Subject: name, Outcome: string(fault), AtMillis: p.schedule.NowMillis()})
		return fmt.Errorf("%w: %w", worker.ErrResultBlobUnknown, ErrTransportLost)
	}
	if err := p.PutObject(ctx, name, raw); err != nil {
		return err
	}
	if fault == LoseAckAfterCommit {
		p.schedule.RecordTransport(TransportEvent{Operation: "retirement_terminal_put", Subject: name, Outcome: string(fault), AtMillis: p.schedule.NowMillis()})
		return fmt.Errorf("%w: %w", worker.ErrResultBlobUnknown, ErrTransportLost)
	}
	return nil
}
func (p *promiseRetirementPort) GetBytes(ctx context.Context, name string) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if name == p.childObject {
		p.childReads++
		if p.unavailable > 0 {
			p.unavailable--
			p.schedule.RecordTransport(TransportEvent{Operation: "retirement_child_read", Subject: name, Outcome: "unavailable", AtMillis: p.schedule.NowMillis()})
			return nil, fmt.Errorf("%w: %w", worker.ErrResultBlobUnavailable, ErrTransportLost)
		}
	}
	return p.GetObject(ctx, name)
}

func runSeededWorkerPromiseRetirement(seed int64, replay *Trace) (trace Trace, runErr error) {
	schedule := NewScheduler(seed)
	if replay != nil {
		var err error
		schedule, err = ReplayScheduler(*replay)
		if err != nil {
			return trace, err
		}
	}
	if err := schedule.SetWorkload("worker_promise_retirement"); err != nil {
		return trace, err
	}
	defer func() { trace = schedule.Trace() }()
	mode, err := schedule.Choose([]string{"clean", "notify_drop", "notify_ack_lost", "terminal_drop", "terminal_ack_lost", "resume_read_lost", "child_purge_ack_lost", "parent_purge_drop"})
	if err != nil {
		return trace, err
	}
	ctx, stop := context.WithTimeout(context.Background(), 10*time.Second)
	defer stop()
	const typ, id, childType = "parent", "shared-promise", "child"
	delivery := NewWorkerTransport(schedule, 3*time.Second)
	p := &promiseRetirementPort{workerRetirementPort: &workerRetirementPort{PurgeTransport: NewPurgeTransport(schedule), delivery: delivery}, signals: map[string]runDedupEntry{}}
	store := journal.NewWithSnapshotPort(p, p, p)
	c := client.NewWithSignalPorts(p, p)
	payload, _ := json.Marshal(strings.Repeat("x", wf.MaxInlineTerminal))
	var promise wf.Promise
	var initialCalls, childCalls int
	initial := map[string]worker.Handler{
		typ: func(c *wf.Context, _ json.RawMessage) (json.RawMessage, error) {
			initialCalls++
			var err error
			promise, err = wf.CallAsync(c, childType, []byte(`null`))
			if err != nil {
				return nil, err
			}
			got, err := wf.AwaitPromise(c, promise)
			if err != nil {
				return nil, err
			}
			if !bytes.Equal(got, payload) {
				return nil, fmt.Errorf("initial child result changed")
			}
			return nil, wf.Continue(c, "finish_v1", promise)
		},
		childType: func(_ *wf.Context, _ json.RawMessage) (json.RawMessage, error) { childCalls++; return payload, nil },
	}
	stages := map[string]worker.ContinuationHandler{"finish_v1": func(c *wf.Context, _, locals json.RawMessage) (json.RawMessage, error) {
		if _, err := wf.AwaitSignal(c, "gate"); err != nil {
			return nil, err
		}
		var saved wf.Promise
		if err := json.Unmarshal(locals, &saved); err != nil {
			return nil, err
		}
		if saved != promise {
			return nil, fmt.Errorf("promise identity changed")
		}
		got, err := wf.AwaitPromise(c, saved)
		if err != nil {
			return nil, err
		}
		if !bytes.Equal(got, payload) {
			return nil, fmt.Errorf("restored child result changed")
		}
		got[0] = '!'
		again, err := wf.AwaitPromise(c, saved)
		if err != nil {
			return nil, err
		}
		if !bytes.Equal(again, payload) {
			return nil, fmt.Errorf("cached promise alias")
		}
		return json.RawMessage(strconv.Itoa(len(again))), nil
	}}
	ports := worker.ModeledWorkerPorts{Journal: store, Leases: p.leasing, Outcome: p.Blobs.State(), Invocation: p, Signals: p, ResultBlobs: p, Client: c, HeartbeatTicks: make(chan time.Time)}
	makeWorker := func(name string) (*worker.Worker, error) {
		return worker.NewWithPorts(name, initial, ports, worker.WithContinuations(typ, stages))
	}
	first, err := makeWorker("promise-first")
	if err != nil {
		return trace, err
	}
	runOne := func(w *worker.Worker, part uint32) error {
		runCtx, cancel := context.WithCancel(ctx)
		defer cancel()
		delivery.Dispatch.StopAfterNextAck(cancel)
		return w.RunPartitionWithTransport(runCtx, part, delivery.Dispatch)
	}
	handle, err := c.Start(ctx, typ, id, []byte(`null`))
	if err != nil {
		return trace, err
	}
	parentPart := identity.Partition(typ, id, provision.Partitions)
	if err := runOne(first, parentPart); err != nil {
		return trace, err
	}
	if promise.ChildID == "" {
		return trace, fmt.Errorf("missing child")
	}
	switch mode {
	case "notify_drop":
		p.notifyFault = DropBeforeCommit
	case "notify_ack_lost":
		p.notifyFault = LoseAckAfterCommit
	case "terminal_drop":
		p.terminalFault = DropBeforeCommit
	case "terminal_ack_lost":
		p.terminalFault = LoseAckAfterCommit
	}
	if err := runOne(first, identity.Partition(childType, promise.ChildID, provision.Partitions)); err != nil {
		return trace, err
	}
	if err := runOne(first, parentPart); err != nil {
		return trace, err
	}
	if err := runOne(first, parentPart); err != nil {
		return trace, err
	}
	before, _, err := store.Read(ctx, typ, id)
	if err != nil || len(before) == 0 || before[len(before)-1].Kind != journal.Suspended || string(before[len(before)-1].Payload) != `{"waiting_on":"signal:gate"}` {
		return trace, fmt.Errorf("parent not waiting at gate: err=%v", err)
	}
	view, err := store.ReadCheckpoint(ctx, typ, id, handle.InvSeq)
	if err != nil || view == nil {
		return trace, fmt.Errorf("checkpoint=%+v err=%v", view, err)
	}
	var frame struct {
		PromiseOutcomes map[string]json.RawMessage `json:"promise_outcomes"`
	}
	if err := json.Unmarshal(view.Frame, &frame); err != nil {
		return trace, err
	}
	var childOutcome wf.Outcome
	if err := json.Unmarshal(frame.PromiseOutcomes[promise.SignalName], &childOutcome); err != nil || childOutcome.ResultRef == "" || childOutcome.ResultHash == "" || len(childOutcome.Result) != 0 || len(view.Frame) >= len(payload) {
		return trace, fmt.Errorf("frame promise metadata invalid: %+v err=%v", childOutcome, err)
	}
	p.childObject = childOutcome.ResultRef
	if err := retention.PurgeWithPort(ctx, p, childType, promise.ChildID, time.Hour); !errors.Is(err, retention.ErrNotTerminal) {
		return trace, fmt.Errorf("child retired before parent terminal: %v", err)
	}
	liveGC, err := retention.SweepBlobsQuiescentWithPort(ctx, p.Blobs, 0, p.Now().Add(time.Hour))
	if err != nil || liveGC.Referenced != 3 || liveGC.Deleted != 0 || !p.Blobs.HasObject(p.childObject) {
		return trace, fmt.Errorf("live GC=%+v err=%v", liveGC, err)
	}
	baselineInitial := initialCalls
	p.childReads = 0
	if mode == "resume_read_lost" {
		p.unavailable = 1
	}
	second, err := makeWorker("promise-replacement")
	if err != nil {
		return trace, err
	}
	if _, err := c.Signal(ctx, typ, id, "gate", []byte(`true`), "gate-key"); err != nil {
		return trace, err
	}
	archiveBefore := p.archiveReads
	runCtx, stopRun := context.WithCancel(ctx)
	delivery.Dispatch.StopWhenDrained(stopRun)
	err = second.RunPartitionWithTransport(runCtx, parentPart, delivery.Dispatch)
	stopRun()
	if err != nil || delivery.Dispatch.Pending() != 0 || p.archiveReads != archiveBefore {
		return trace, fmt.Errorf("resume pending=%d archive_reads=%d err=%v", delivery.Dispatch.Pending(), p.archiveReads-archiveBefore, err)
	}
	wantReads := 1
	if mode == "resume_read_lost" {
		wantReads++
	}
	wantChild := 1
	if strings.HasPrefix(mode, "terminal_") {
		wantChild++
	}
	if initialCalls != baselineInitial || childCalls != wantChild || p.childReads != wantReads {
		return trace, fmt.Errorf("replay counts initial=%d/%d child=%d want=%d result_reads=%d want=%d", initialCalls, baselineInitial, childCalls, wantChild, p.childReads, wantReads)
	}
	parentRecords, _, err := store.Read(ctx, typ, id)
	if err != nil {
		return trace, err
	}
	calls, childSignals, promiseRequests := 0, 0, 0
	for _, record := range parentRecords {
		if record.Kind == journal.SignalConsumed {
			var consumed struct{ Name string }
			if err := json.Unmarshal(record.Payload, &consumed); err != nil {
				return trace, err
			}
			if consumed.Name == promise.SignalName {
				childSignals++
			}
		}
		if record.Kind == journal.StepRequested {
			var request struct{ Kind, Name string }
			if err := json.Unmarshal(record.Payload, &request); err != nil {
				return trace, err
			}
			if request.Kind == "call_async" {
				calls++
			}
			if request.Name == promise.SignalName {
				promiseRequests++
			}
		}
	}
	if calls != 1 || childSignals != 1 || promiseRequests != 2 {
		return trace, fmt.Errorf("promise declarations/consumptions: calls=%d child_signals=%d requests=%d", calls, childSignals, promiseRequests)
	}
	parentState, err := p.Blobs.State().Get(ctx, identity.Key(typ, id))
	if err != nil {
		return trace, err
	}
	var parentOutcome wf.Outcome
	if json.Unmarshal(parentState.Value, &parentOutcome) != nil || parentOutcome.InvSeq != handle.InvSeq || string(parentOutcome.Result) != strconv.Itoa(len(payload)) {
		return trace, fmt.Errorf("wrong parent outcome: %s", parentState.Value)
	}
	raw := integrity.Snapshot{Journals: map[string][]journal.Record{}, TerminalState: map[string][]byte{}}
	p.Blobs.mu.Lock()
	var subjects []string
	for _, m := range p.Blobs.streams["WF_INV"].messages {
		subjects = append(subjects, m.Subject)
	}
	p.Blobs.mu.Unlock()
	sort.Strings(subjects)
	for _, subject := range subjects {
		key := strings.TrimPrefix(subject, "wf.inv.")
		parts := strings.SplitN(key, ".", 2)
		records, _, err := store.Read(ctx, parts[0], parts[1])
		if err != nil {
			return trace, err
		}
		state, err := p.Blobs.State().Get(ctx, key)
		if err != nil {
			return trace, err
		}
		raw.Invocations = append(raw.Invocations, subject)
		raw.Journals[identity.JournalSubject(parts[0], parts[1])] = records
		raw.TerminalState[key] = state.Value
	}
	if report, err := integrity.CheckSnapshot(raw); err != nil || report.Invocations != 2 || report.Terminal != 2 {
		return trace, fmt.Errorf("raw integrity=%+v err=%v", report, err)
	}
	if mode == "child_purge_ack_lost" {
		if err := p.QueueFault(PurgeFault{"purge_WF_INV", LoseAckAfterCommit}); err != nil {
			return trace, err
		}
	}
	purgeErr := retention.PurgeWithPort(ctx, p, childType, promise.ChildID, time.Hour)
	if (purgeErr != nil) != (mode == "child_purge_ack_lost") {
		return trace, fmt.Errorf("child purge err=%v mode=%s", purgeErr, mode)
	}
	childGC, err := retention.SweepBlobsQuiescentWithPort(ctx, p.Blobs, 0, p.Now().Add(time.Hour))
	if err != nil || childGC.Referenced != 3 || childGC.Deleted != 0 || !p.Blobs.HasObject(p.childObject) {
		return trace, fmt.Errorf("retired child GC lost frame-held outcome: %+v err=%v", childGC, err)
	}
	if err := retention.PurgeWithPort(ctx, p, childType, promise.ChildID, time.Hour); err != nil {
		return trace, err
	}
	data, err := p.Blobs.ObjectBytes(ctx, p.childObject)
	if err != nil || !bytes.Equal(data, payload) {
		return trace, fmt.Errorf("child bytes changed after retirement: %v", err)
	}
	// Offline replay uses retained objects after child retirement and executes no
	// child handler or object-store archive reads. It audits initial declarations
	// and named stages from the exported logical journal.
	history, err := json.Marshal(parentRecords)
	if err != nil {
		return trace, err
	}
	replayed, err := wf.ReplayWithContinuations(history, func(c *wf.Context) (json.RawMessage, error) { return initial[typ](c, json.RawMessage(`null`)) }, map[string]wf.ReplayContinuation[json.RawMessage]{"finish_v1": func(c *wf.Context, locals json.RawMessage) (json.RawMessage, error) {
		return stages["finish_v1"](c, json.RawMessage(`null`), locals)
	}}, wf.ReplayOptions{Type: typ, ID: id, InvSeq: handle.InvSeq, Objects: map[string][]byte{p.childObject: data, view.Snapshot.Runtime.Object: view.Frame}})
	if err != nil || string(replayed) != strconv.Itoa(len(payload)) || childCalls != wantChild || initialCalls != baselineInitial+1 {
		return trace, fmt.Errorf("offline=%s child_calls=%d err=%v", replayed, childCalls, err)
	}
	if mode == "parent_purge_drop" {
		if err := p.QueueFault(PurgeFault{"purge_WF_JRN", DropBeforeCommit}); err != nil {
			return trace, err
		}
	}
	parentPurgeErr := retention.PurgeWithPort(ctx, p, typ, id, time.Hour)
	if (parentPurgeErr != nil) != (mode == "parent_purge_drop") {
		return trace, fmt.Errorf("parent purge err=%v", parentPurgeErr)
	}
	interrupted, err := retention.SweepBlobsQuiescentWithPort(ctx, p.Blobs, 0, p.Now().Add(time.Hour))
	if err != nil {
		return trace, err
	}
	if err := retention.PurgeWithPort(ctx, p, typ, id, time.Hour); err != nil {
		return trace, err
	}
	final, err := retention.SweepBlobsQuiescentWithPort(ctx, p.Blobs, 0, p.Now().Add(time.Hour))
	if err != nil || interrupted.Deleted+final.Deleted != 3 || final.Referenced != 0 || p.Blobs.HasObject(p.childObject) || p.PurgeEventCount() != 2 {
		return trace, fmt.Errorf("final GC=%+v intermediate=%+v events=%d err=%v", final, interrupted, p.PurgeEventCount(), err)
	}
	for _, v := range []struct {
		typ, id    string
		generation uint64
	}{{typ, id, handle.InvSeq}, {childType, promise.ChildID, childOutcome.InvSeq}} {
		state, err := p.Blobs.State().Get(ctx, identity.Key(v.typ, v.id))
		if err != nil {
			return trace, err
		}
		tomb, ok, err := retention.Decode(state.Value)
		if err != nil || !ok || tomb.InvSeq != v.generation {
			return trace, fmt.Errorf("wrong tombstone %s generation=%d got=%+v err=%v", v.typ, v.generation, tomb, err)
		}
		if _, err := p.LastInvocation(ctx, identity.InvocationSubject(v.typ, v.id)); !errors.Is(err, jetstream.ErrMsgNotFound) {
			return trace, fmt.Errorf("retained retired invocation %s: %v", v.typ, err)
		}
		for _, key := range []string{"snap." + identity.Key(v.typ, v.id), "purging." + identity.Key(v.typ, v.id)} {
			if _, err := p.Blobs.State().Get(ctx, key); !errors.Is(err, jetstream.ErrKeyNotFound) {
				return trace, fmt.Errorf("retained retirement metadata %s: %v", key, err)
			}
		}
	}
	schedule.RecordTransport(TransportEvent{Operation: "check_worker_promise_retirement", Subject: identity.Key(typ, id), Outcome: mode, Sequence: uint64(final.Deleted + interrupted.Deleted), AtMillis: schedule.NowMillis()})
	if err := schedule.Finish(); err != nil {
		return trace, err
	}
	return schedule.Trace(), nil
}

func TestSeededWorkerPromiseRetirementReplay(t *testing.T) {
	if os.Getenv("SIM_WORKER_PROMISE_RETIREMENT_HELPER") == "1" {
		seed, err := strconv.ParseInt(os.Getenv("FAULT_SEED"), 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		trace, err := runSeededWorkerPromiseRetirement(seed, nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := trace.Save(os.Getenv("SIM_WORKER_PROMISE_RETIREMENT_OUT")); err != nil {
			t.Fatal(err)
		}
		return
	}
	observed := map[string]int{}
	for seed := range seededSchedules(t) {
		generated, err := runSeededWorkerPromiseRetirement(seed, nil)
		if err != nil {
			path := os.Getenv("FAULT_TRACE_OUT")
			if path == "" {
				path = filepath.Join(t.TempDir(), "worker-promise-retirement-failure.json")
			}
			_ = generated.Save(path)
			t.Fatalf("FAULT_SEED=%d FAULT_TRACE=%s: %v", seed, path, err)
		}
		observed[generated.Decisions[0].Chosen]++
		if seed <= 10 {
			replayed, err := runSeededWorkerPromiseRetirement(seed, &generated)
			if err != nil || !reflect.DeepEqual(generated, replayed) {
				t.Fatalf("FAULT_SEED=%d replay: %v", seed, err)
			}
		}
	}
	if len(observed) != 8 {
		t.Fatalf("covered %d/8 worker promise retirement modes", len(observed))
	}
	var files [2]string
	for i := range files {
		files[i] = filepath.Join(t.TempDir(), fmt.Sprintf("worker-promise-retirement-%d.json", i))
		cmd := exec.Command(os.Args[0], "-test.run=^TestSeededWorkerPromiseRetirementReplay$")
		cmd.Env = append(os.Environ(), "SIM_WORKER_PROMISE_RETIREMENT_HELPER=1", "SIM_WORKER_PROMISE_RETIREMENT_OUT="+files[i], "FAULT_SEED=42")
		if output, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("child %d: %v: %s", i, err, output)
		}
	}
	first, err := os.ReadFile(files[0])
	if err != nil {
		t.Fatal(err)
	}
	second, err := os.ReadFile(files[1])
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(first, second) {
		t.Fatal("worker retirement trace changed across processes")
	}
}
