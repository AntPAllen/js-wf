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

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"js-wf/client"
	"js-wf/identity"
	"js-wf/integrity"
	"js-wf/journal"
	"js-wf/retention"
	"js-wf/wf"
	"js-wf/worker"
)

// Worker, retirement and GC use the same retained streams, KV and objects.
// Only run delivery/dedup uses the existing dispatch transport. No post-hoc
// copying of invocation, journal, state or snapshot data connects these paths.
type workerRetirementPort struct {
	*PurgeTransport
	delivery     *WorkerTransport
	archiveReads int
	startAckLost bool
}

func (p *workerRetirementPort) LastInvocation(ctx context.Context, subject string) (*jetstream.RawStreamMsg, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	p.Blobs.mu.Lock()
	defer p.Blobs.mu.Unlock()
	var latest uint64
	for seq, m := range p.Blobs.streams["WF_INV"].messages {
		if m.Subject == subject && seq > latest {
			latest = seq
		}
	}
	if latest == 0 {
		p.Blobs.event(TransportEvent{Operation: "retirement_worker_invocation", Subject: subject, Outcome: "not_found"})
		return nil, jetstream.ErrMsgNotFound
	}
	m := p.Blobs.streams["WF_INV"].messages[latest]
	p.Blobs.event(TransportEvent{Operation: "retirement_worker_invocation", Subject: subject, Sequence: latest, Outcome: "ok"})
	return &jetstream.RawStreamMsg{Sequence: latest, Subject: subject, Header: cloneHeader(m.Header), Data: bytes.Clone(m.Data)}, nil
}
func (p *workerRetirementPort) PublishInvocation(ctx context.Context, m *nats.Msg) (uint64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	p.Blobs.mu.Lock()
	defer p.Blobs.mu.Unlock()
	if m.Header.Get(jetstream.ExpectedLastSubjSeqHeader) != "0" {
		p.Blobs.event(TransportEvent{Operation: "retirement_start_publish", Subject: m.Subject, Outcome: "missing_cas"})
		return 0, fmt.Errorf("missing start CAS")
	}
	stream := p.Blobs.streams["WF_INV"]
	for _, message := range stream.messages {
		if message.Subject == m.Subject {
			p.Blobs.event(TransportEvent{Operation: "retirement_start_publish", Subject: m.Subject, Outcome: "wrong_last_sequence"})
			return 0, wrongLastSequence()
		}
	}
	stream.last++
	stream.messages[stream.last] = retention.BlobSweepMessage{Subject: m.Subject, Header: cloneHeader(m.Header), Data: bytes.Clone(m.Data)}
	p.Blobs.event(TransportEvent{Operation: "retirement_start_publish", Subject: m.Subject, Sequence: stream.last, DataSHA256: digest(m.Data), Outcome: "ok"})
	if p.startAckLost {
		p.startAckLost = false
		p.Blobs.event(TransportEvent{Operation: "retirement_start_ack_lost", Subject: m.Subject, Sequence: stream.last, Outcome: "unknown"})
		return 0, ErrTransportLost
	}
	return stream.last, nil
}
func (p *workerRetirementPort) EnqueueRun(ctx context.Context, subject string, data []byte, id string) error {
	return p.delivery.EnqueueRun(ctx, subject, data, id)
}
func (p *workerRetirementPort) PutInput(ctx context.Context, name string, data []byte) error {
	return p.PutObject(ctx, name, data)
}
func (p *workerRetirementPort) InputBlob(ctx context.Context, name string) ([]byte, error) {
	return p.GetObject(ctx, name)
}
func (p *workerRetirementPort) SignalBlob(ctx context.Context, name string) ([]byte, error) {
	return p.GetObject(ctx, name)
}
func (p *workerRetirementPort) PutBytes(ctx context.Context, name string, data []byte) error {
	return p.PutObject(ctx, name, data)
}
func (p *workerRetirementPort) GetBytes(ctx context.Context, name string) ([]byte, error) {
	return p.GetObject(ctx, name)
}
func (p *workerRetirementPort) GetObject(ctx context.Context, name string) ([]byte, error) {
	if strings.HasPrefix(name, "snapshot-") {
		p.archiveReads++
	}
	return p.PurgeTransport.GetObject(ctx, name)
}
func (p *workerRetirementPort) LastSignalSequence(ctx context.Context) (uint64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	p.Blobs.mu.Lock()
	defer p.Blobs.mu.Unlock()
	sequence := p.Blobs.streams["WF_SIG"].last
	p.Blobs.event(TransportEvent{Operation: "retirement_signal_info", Sequence: sequence, Outcome: "ok"})
	return sequence, nil
}
func (p *workerRetirementPort) NextSignal(ctx context.Context, from uint64, subject string) (*jetstream.RawStreamMsg, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	p.Blobs.mu.Lock()
	defer p.Blobs.mu.Unlock()
	stream := p.Blobs.streams["WF_SIG"]
	prefix := strings.TrimSuffix(subject, "*")
	for seq := from; seq != 0 && seq <= stream.last; seq++ {
		m, ok := stream.messages[seq]
		if !ok || !strings.HasPrefix(m.Subject, prefix) {
			continue
		}
		suffix := strings.TrimPrefix(m.Subject, prefix)
		if suffix == "" || strings.Contains(suffix, ".") {
			continue
		}
		p.Blobs.event(TransportEvent{Operation: "retirement_worker_signal", Subject: subject, Sequence: seq, Outcome: "ok"})
		return &jetstream.RawStreamMsg{Sequence: seq, Subject: m.Subject, Header: cloneHeader(m.Header), Data: bytes.Clone(m.Data)}, nil
	}
	p.Blobs.event(TransportEvent{Operation: "retirement_worker_signal", Subject: subject, Expected: from, Outcome: "not_found"})
	return nil, jetstream.ErrMsgNotFound
}

func runSeededWorkerRetirement(seed int64, replay *Trace) (trace Trace, runErr error) {
	schedule := NewScheduler(seed)
	if replay != nil {
		var err error
		schedule, err = ReplayScheduler(*replay)
		if err != nil {
			return trace, err
		}
	}
	if err := schedule.SetWorkload("worker_continuation_retirement"); err != nil {
		return trace, err
	}
	defer func() { trace = schedule.Trace() }()
	mode, err := schedule.Choose([]string{"clean", "journal_drop", "snapshot_delete_drop", "tombstone_ack_lost", "purge_event_ack_lost", "invocation_ack_lost", "reuse_start_ack_lost"})
	if err != nil {
		return trace, err
	}
	ctx, stop := context.WithTimeout(context.Background(), 10*time.Second)
	defer stop()
	const typ = "test"
	id := integratedWorkerIDs(1)[0]
	delivery := NewWorkerTransport(schedule, 3*time.Second)
	p := &workerRetirementPort{PurgeTransport: NewPurgeTransport(schedule), delivery: delivery}
	store := journal.NewWithSnapshotPort(p, p, p)
	c := client.NewWithStartPort(p)
	var effects int
	var keys []string
	initial := map[string]worker.Handler{typ: func(c *wf.Context, input json.RawMessage) (json.RawMessage, error) {
		var value int
		if err := json.Unmarshal(input, &value); err != nil {
			return nil, err
		}
		if err := c.SetState("value", value); err != nil {
			return nil, err
		}
		if _, err := wf.RunOnce(c, "prefix", value, func(_ context.Context, key string) (int, error) {
			effects++
			keys = append(keys, key)
			return value, nil
		}); err != nil {
			return nil, err
		}
		return nil, wf.Continue(c, "finish_v1", value)
	}}
	stages := map[string]worker.ContinuationHandler{"finish_v1": func(c *wf.Context, input, locals json.RawMessage) (json.RawMessage, error) {
		var value, local int
		if err := json.Unmarshal(input, &value); err != nil {
			return nil, err
		}
		if err := json.Unmarshal(locals, &local); err != nil {
			return nil, err
		}
		var state int
		found, err := c.GetState("value", &state)
		if err != nil || !found || state != value || local != value {
			return nil, fmt.Errorf("cross-generation state/locals: %d/%d/%d", value, local, state)
		}
		signal, err := wf.AwaitSignal(c, "finish")
		if err != nil {
			return nil, err
		}
		if string(signal) != string(input) {
			return nil, fmt.Errorf("stale signal consumed: %s input=%s", signal, input)
		}
		result, err := wf.RunOnce(c, "suffix", value, func(_ context.Context, key string) (int, error) {
			effects++
			keys = append(keys, key)
			return value * 2, nil
		})
		if err != nil {
			return nil, err
		}
		return json.Marshal(result)
	}}
	runWorker := func(name string) error {
		w, err := worker.NewWithPorts(name, initial, worker.ModeledWorkerPorts{Journal: store, Leases: p.leasing, Outcome: p.Blobs.State(), Invocation: p, Signals: p, ResultBlobs: p, Client: c, HeartbeatTicks: make(chan time.Time)}, worker.WithContinuations(typ, stages))
		if err != nil {
			return err
		}
		runCtx, stop := context.WithCancel(ctx)
		defer stop()
		delivery.Dispatch.StopWhenDrained(stop)
		before := p.archiveReads
		if err := w.RunPartitionWithTransport(runCtx, 0, delivery.Dispatch); err != nil {
			return err
		}
		if delivery.Dispatch.Pending() != 0 || strings.HasSuffix(name, "replacement") && p.archiveReads != before {
			return fmt.Errorf("dispatch/prefix read: pending=%d archive_reads=%d", delivery.Dispatch.Pending(), p.archiveReads-before)
		}
		return nil
	}
	signal := func(generation uint64, data string) error {
		headers := nats.Header{}
		headers.Set("Wf-Inv-Seq", strconv.FormatUint(generation, 10))
		_, err := p.Blobs.PublishSubject("WF_SIG", "wf.sig."+typ+"."+id+".finish", headers, []byte(data))
		if err != nil {
			return err
		}
		return c.Enqueue(ctx, typ, id, "finish:"+strconv.FormatUint(generation, 10)+":"+data)
	}
	checkTerminal := func(generation uint64, want string) error {
		records, _, err := store.Read(ctx, typ, id)
		if err != nil {
			return err
		}
		if len(records) == 0 || records[0].Kind != journal.Started || records[0].Index != 0 || records[len(records)-1].Kind != journal.Completed {
			return fmt.Errorf("invalid generation journal")
		}
		state, err := p.Blobs.State().Get(ctx, identity.Key(typ, id))
		if err != nil {
			return err
		}
		var outcome wf.Outcome
		if json.Unmarshal(state.Value, &outcome) != nil || outcome.InvSeq != generation || string(outcome.Result) != want {
			return fmt.Errorf("wrong terminal: %s", state.Value)
		}
		snapshot := integrity.Snapshot{Journals: map[string][]journal.Record{identity.JournalSubject(typ, id): records}, TerminalState: map[string][]byte{identity.Key(typ, id): state.Value}}
		p.Blobs.mu.Lock()
		for _, m := range p.Blobs.streams["WF_INV"].messages {
			snapshot.Invocations = append(snapshot.Invocations, m.Subject)
		}
		p.Blobs.mu.Unlock()
		report, err := integrity.CheckSnapshot(snapshot)
		if err != nil || report.Invocations != 1 || report.Terminal != 1 {
			return fmt.Errorf("raw integrity=%+v err=%v", report, err)
		}
		return nil
	}
	old, err := c.Start(ctx, typ, id, []byte(`23`))
	if err != nil {
		return trace, err
	}
	if err := runWorker("old-first"); err != nil {
		return trace, err
	}
	if err := retention.PurgeWithPort(ctx, p, typ, id, time.Hour); !errors.Is(err, retention.ErrNotTerminal) {
		return trace, fmt.Errorf("purged suspended worker: %v", err)
	}
	before, err := retention.SweepBlobsQuiescentWithPort(ctx, p.Blobs, 0, p.Now().Add(time.Hour))
	if err != nil || before.Referenced != 2 || before.Deleted != 0 {
		return trace, fmt.Errorf("live sweep=%+v err=%v", before, err)
	}
	if err := signal(old.InvSeq, "23"); err != nil {
		return trace, err
	}
	if err := runWorker("old-replacement"); err != nil {
		return trace, err
	}
	if err := checkTerminal(old.InvSeq, "46"); err != nil {
		return trace, err
	}
	switch mode {
	case "journal_drop":
		err = p.QueueFault(PurgeFault{"purge_WF_JRN", DropBeforeCommit})
	case "snapshot_delete_drop":
		err = p.Blobs.State().QueueFault(KVFault{"delete", KVDropBeforeCommit})
	case "tombstone_ack_lost":
		err = p.Blobs.State().QueueFault(KVFault{"update", KVLoseAckAfterCommit})
	case "purge_event_ack_lost":
		err = p.QueueFault(PurgeFault{"publish_purge", LoseAckAfterCommit})
	case "invocation_ack_lost":
		err = p.QueueFault(PurgeFault{"purge_WF_INV", LoseAckAfterCommit})
	}
	if err != nil {
		return trace, err
	}
	firstErr := retention.PurgeWithPort(ctx, p, typ, id, time.Hour)
	expectedError := mode != "clean" && mode != "tombstone_ack_lost" && mode != "reuse_start_ack_lost"
	if (firstErr != nil) != expectedError {
		return trace, fmt.Errorf("purge mode=%s err=%v", mode, firstErr)
	}
	interrupted, err := retention.SweepBlobsQuiescentWithPort(ctx, p.Blobs, 0, p.Now().Add(time.Hour))
	if err != nil {
		return trace, fmt.Errorf("interrupted retirement sweep: %w", err)
	}
	if err := retention.PurgeWithPort(ctx, p, typ, id, time.Hour); err != nil {
		return trace, err
	}
	tomb, err := p.Blobs.State().Get(ctx, identity.Key(typ, id))
	if err != nil {
		return trace, err
	}
	marker, isTomb, err := retention.Decode(tomb.Value)
	if err != nil || !isTomb || marker.InvSeq != old.InvSeq {
		return trace, fmt.Errorf("wrong retired generation")
	}
	retired, err := retention.SweepBlobsQuiescentWithPort(ctx, p.Blobs, 0, p.Now().Add(time.Hour))
	if err != nil || retired.Deleted+interrupted.Deleted != 2 || retired.Referenced != 0 {
		return trace, fmt.Errorf("retired sweep=%+v err=%v", retired, err)
	}
	if mode == "reuse_start_ack_lost" {
		p.startAckLost = true
	}
	fresh, err := c.Start(ctx, typ, id, []byte(`77`))
	if (err != nil && !(mode == "reuse_start_ack_lost" && errors.Is(err, client.ErrAlreadyStarted))) || fresh.InvSeq <= old.InvSeq {
		return trace, fmt.Errorf("reuse generation=%d old=%d err=%v", fresh.InvSeq, old.InvSeq, err)
	}
	// An old-generation signal must not enable the fresh continuation.
	if err := signal(old.InvSeq, "999"); err != nil {
		return trace, err
	}
	if err := runWorker("fresh-first"); err != nil {
		return trace, err
	}
	if effects != 3 {
		return trace, fmt.Errorf("fresh stage consumed stale signal: effects=%d", effects)
	}
	freshLive, err := retention.SweepBlobsQuiescentWithPort(ctx, p.Blobs, 0, p.Now().Add(time.Hour))
	if err != nil || freshLive.Referenced != 2 || freshLive.Deleted != 0 {
		return trace, fmt.Errorf("fresh live sweep=%+v err=%v", freshLive, err)
	}
	if err := signal(fresh.InvSeq, "77"); err != nil {
		return trace, err
	}
	if err := runWorker("fresh-replacement"); err != nil {
		return trace, err
	}
	if err := checkTerminal(fresh.InvSeq, "154"); err != nil {
		return trace, err
	}
	if effects != 4 || len(keys) != 4 {
		return trace, fmt.Errorf("effect count=%d keys=%v", effects, keys)
	}
	for i := range keys {
		for j := 0; j < i; j++ {
			if keys[i] == keys[j] {
				return trace, fmt.Errorf("effect key reused across stages/generations")
			}
		}
	}
	if err := retention.PurgeWithPort(ctx, p, typ, id, time.Hour); err != nil {
		return trace, err
	}
	final, err := retention.SweepBlobsQuiescentWithPort(ctx, p.Blobs, 0, p.Now().Add(time.Hour))
	if err != nil || final.Deleted != 2 || final.Referenced != 0 || p.PurgeEventCount() != 2 {
		return trace, fmt.Errorf("final sweep=%+v events=%d err=%v", final, p.PurgeEventCount(), err)
	}
	finalState, err := p.Blobs.State().Get(ctx, identity.Key(typ, id))
	if err != nil {
		return trace, err
	}
	finalMarker, finalTomb, err := retention.Decode(finalState.Value)
	if err != nil || !finalTomb || finalMarker.InvSeq != fresh.InvSeq {
		return trace, fmt.Errorf("wrong final tombstone generation: %+v err=%v", finalMarker, err)
	}
	if _, err := p.LastInvocation(ctx, identity.InvocationSubject(typ, id)); !errors.Is(err, jetstream.ErrMsgNotFound) {
		return trace, fmt.Errorf("fresh invocation retained after retirement: %v", err)
	}
	for _, key := range []string{"snap." + identity.Key(typ, id), "purging." + identity.Key(typ, id)} {
		if _, err := p.Blobs.State().Get(ctx, key); !errors.Is(err, jetstream.ErrKeyNotFound) {
			return trace, fmt.Errorf("retained retirement metadata %s: %v", key, err)
		}
	}
	schedule.RecordTransport(TransportEvent{Operation: "check_worker_retirement", Subject: identity.Key(typ, id), Sequence: fresh.InvSeq, Outcome: mode, AtMillis: schedule.NowMillis()})
	if err := schedule.Finish(); err != nil {
		return trace, err
	}
	return schedule.Trace(), nil
}

func TestSeededWorkerRetirementReplay(t *testing.T) {
	if os.Getenv("SIM_WORKER_RETIREMENT_HELPER") == "1" {
		seed, err := strconv.ParseInt(os.Getenv("FAULT_SEED"), 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		trace, err := runSeededWorkerRetirement(seed, nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := trace.Save(os.Getenv("SIM_WORKER_RETIREMENT_OUT")); err != nil {
			t.Fatal(err)
		}
		return
	}
	observed := map[string]int{}
	for seed, limit := int64(1), seededScheduleLimit(t); seed <= limit; seed++ {
		generated, err := runSeededWorkerRetirement(seed, nil)
		if err != nil {
			path := os.Getenv("FAULT_TRACE_OUT")
			if path == "" {
				path = filepath.Join(t.TempDir(), "worker-retirement-failure.json")
			}
			_ = generated.Save(path)
			t.Fatalf("FAULT_SEED=%d FAULT_TRACE=%s: %v", seed, path, err)
		}
		observed[generated.Decisions[0].Chosen]++
		if seed <= 10 {
			replayed, err := runSeededWorkerRetirement(seed, &generated)
			if err != nil || !reflect.DeepEqual(generated, replayed) {
				t.Fatalf("FAULT_SEED=%d replay: %v", seed, err)
			}
		}
	}
	if len(observed) != 7 {
		t.Fatalf("covered %d/7 worker retirement modes", len(observed))
	}
	var files [2]string
	for i := range files {
		files[i] = filepath.Join(t.TempDir(), fmt.Sprintf("worker-retirement-%d.json", i))
		cmd := exec.Command(os.Args[0], "-test.run=^TestSeededWorkerRetirementReplay$")
		cmd.Env = append(os.Environ(), "SIM_WORKER_RETIREMENT_HELPER=1", "SIM_WORKER_RETIREMENT_OUT="+files[i], "FAULT_SEED=42")
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
