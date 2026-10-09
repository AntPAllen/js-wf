package sim

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"js-wf/client"
	"js-wf/identity"
	"js-wf/internal/graphpublication"
	"js-wf/journal"
	"js-wf/retention"
	"js-wf/wf"
)

type awaitContentionAuthority struct {
	*GraphPublicationTransport
	schedule  *Scheduler
	mode      string
	remaining int
	armed     bool
}

func (p *awaitContentionAuthority) ReadRoot(ctx context.Context, key string) (graphpublication.Root, error) {
	root, err := p.GraphPublicationTransport.ReadRoot(ctx, key)
	if err != nil {
		return root, err
	}
	match := p.mode != "release" && (p.mode != "pinned" || len(root.Readers) > 0)
	if p.armed && match && p.remaining > 0 {
		p.remaining--
		p.schedule.RecordTransport(TransportEvent{Operation: "await_root_read", Subject: key, Outcome: p.mode})
		if p.mode == "deadline" || p.mode == "deadline-corrupt" {
			<-ctx.Done()
			if p.mode == "deadline-corrupt" {
				return graphpublication.Root{}, wf.ErrCorruptJournal
			}
			return graphpublication.Root{}, ctx.Err()
		}
		if p.mode == "corrupt" {
			return graphpublication.Root{}, wf.ErrCorruptJournal
		}
		if p.mode == "unknown" {
			return graphpublication.Root{}, context.DeadlineExceeded
		}
		return graphpublication.Root{}, fmt.Errorf("read witness rejected: %w", graphpublication.ErrConflict)
	}
	return root, nil
}
func (p *awaitContentionAuthority) CASRoot(ctx context.Context, key string, head uint64, root graphpublication.Root) (graphpublication.Root, error) {
	if p.armed && p.mode == "release" && p.remaining > 0 {
		old, err := p.GraphPublicationTransport.ReadRoot(ctx, key)
		if err != nil {
			return old, err
		}
		if len(root.Readers) < len(old.Readers) {
			p.remaining--
			p.schedule.RecordTransport(TransportEvent{Operation: "await_reader_release", Subject: key, Expected: head, Outcome: "conflict"})
			return graphpublication.Root{}, graphpublication.ErrConflict
		}
	}
	return p.GraphPublicationTransport.CASRoot(ctx, key, head, root)
}

type awaitContentionResults struct {
	*SignalTransport
	state  []byte
	onWait func() error
	waits  int
}

func (p *awaitContentionResults) State(context.Context, string) ([]byte, error) {
	if p.state == nil {
		return nil, jetstream.ErrKeyNotFound
	}
	return bytes.Clone(p.state), nil
}
func (p *awaitContentionResults) Wait(ctx context.Context, d time.Duration) error {
	p.waits++
	if err := p.SignalTransport.Wait(ctx, d); err != nil {
		return err
	}
	if p.onWait != nil {
		f := p.onWait
		p.onWait = nil
		return f()
	}
	return nil
}

// RunGraphAwaitContentionSchedule exercises production Await decisions against
// the supplied scheduler. The caller sets the workload and owns exact replay.
// Deadline modes use a real context timer and are excluded from seeded suites.
func RunGraphAwaitContentionSchedule(schedule *Scheduler, version int, mode string) (trace Trace, err error) {
	if schedule == nil || version < 4 || version > 6 {
		return trace, fmt.Errorf("invalid Await contention configuration")
	}
	switch mode {
	case "observe", "pinned", "release", "generation", "purge", "cancel", "unknown", "corrupt", "deadline", "deadline-corrupt":
	default:
		return trace, fmt.Errorf("unsupported Await contention mode %q", mode)
	}
	defer func() { trace = schedule.Trace() }()
	count, err := schedule.Choose([]string{"once", "three"})
	if err != nil {
		return trace, err
	}
	remaining := 1
	if count == "three" {
		remaining = 3
	}
	if mode == "release" {
		remaining = 16
	}
	if mode == "unknown" || mode == "corrupt" || mode == "deadline" || mode == "deadline-corrupt" {
		remaining = 1
	}
	model := NewGraphPublicationTransport(schedule)
	port := &awaitContentionAuthority{GraphPublicationTransport: model, schedule: schedule, mode: mode, remaining: remaining}
	protocol := model.Protocol()
	protocol.Port = port
	now := func() time.Time { return time.UnixMilli(schedule.NowMillis()).UTC() }
	store, err := journal.NewGraphStore(journal.GraphConfig{Protocol: protocol, Now: now, PinTTL: 30 * time.Second, IntentTTL: time.Second, CanonicalStarts: true, CanonicalSignals: true, CheckpointIndex: version >= 5, ArchiveCheckpoints: version == 6})
	if err != nil {
		return trace, err
	}
	source := NewSignalTransport(schedule)
	results := &awaitContentionResults{SignalTransport: source}
	sdk, err := client.NewWithGraphJournalPorts(source, source, results, store)
	if err != nil {
		return trace, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	handle, err := sdk.Start(ctx, "flow", "contention", []byte(`7`))
	if err != nil {
		return trace, err
	}
	finish := func(invocation uint64, value string) (uint64, error) {
		status, e := store.InspectStart(ctx, handle.Type, handle.ID)
		if e != nil {
			return 0, e
		}
		tail, e := store.Begin(ctx, handle.Type, handle.ID, invocation)
		if e != nil {
			return 0, e
		}
		started, _ := json.Marshal(map[string]string{"input_sha256": status.State.Start.InputSHA256})
		tail, e = store.Append(ctx, handle.Type, handle.ID, invocation, journal.Entry{Kind: journal.Started, Payload: started}, tail, [][]byte{[]byte(`7`)}, nil)
		if e != nil {
			return 0, e
		}
		terminal, _ := json.Marshal(wf.Outcome{InvSeq: invocation, Result: json.RawMessage(value)})
		return store.Append(ctx, handle.Type, handle.ID, invocation, journal.Entry{Kind: journal.Completed, Index: 1, Payload: terminal}, tail, nil, nil)
	}
	tail, err := finish(handle.InvSeq, `42`)
	if err != nil {
		return trace, err
	}
	activeInvocation := handle.InvSeq
	if mode == "generation" {
		results.onWait = func() error {
			port.armed = false
			if err := store.Retire(ctx, handle.Type, handle.ID, handle.InvSeq, tail); err != nil {
				return err
			}
			source.PurgeInvocation(identity.InvocationSubject(handle.Type, handle.ID))
			next, err := sdk.Start(ctx, handle.Type, handle.ID, []byte(`7`))
			if err != nil {
				return err
			}
			activeInvocation = next.InvSeq
			tail, err = finish(next.InvSeq, `99`)
			return err
		}
	}
	if mode == "purge" {
		results.onWait = func() error {
			results.state, _ = json.Marshal(retention.Tombstone{Tombstone: true, InvSeq: handle.InvSeq, PurgedAt: now(), ExpiresAt: now().Add(time.Hour)})
			return nil
		}
	}
	if mode == "cancel" {
		results.onWait = func() error { cancel(); return ctx.Err() }
	}
	port.armed = true
	value, readErr := sdk.Await(ctx, handle.Type, handle.ID)
	switch mode {
	case "generation", "purge":
		if !errors.Is(readErr, client.ErrPurged) || len(value) != 0 {
			return trace, fmt.Errorf("%s transferred/returned result: value=%s error=%v", mode, value, readErr)
		}
	case "cancel":
		if !errors.Is(readErr, context.Canceled) || len(value) != 0 {
			return trace, fmt.Errorf("cancel returned value=%s error=%v", value, readErr)
		}
	case "unknown":
		if !errors.Is(readErr, context.DeadlineExceeded) || len(value) != 0 || results.waits != 0 {
			return trace, fmt.Errorf("unknown read was retried/accepted: value=%s error=%v waits=%d", value, readErr, results.waits)
		}
	case "corrupt", "deadline-corrupt":
		if !errors.Is(readErr, wf.ErrCorruptJournal) || len(value) != 0 || results.waits != 0 {
			return trace, fmt.Errorf("corruption at local deadline was retried/accepted: value=%s error=%v waits=%d", value, readErr, results.waits)
		}
	default:
		if readErr != nil || string(value) != "42" || results.waits == 0 {
			return trace, fmt.Errorf("%s did not freshly await: value=%s error=%v waits=%d", mode, value, readErr, results.waits)
		}
	}
	port.armed = false
	cleanup := context.Background()
	if err = store.Retire(cleanup, handle.Type, handle.ID, activeInvocation, tail); err != nil {
		return trace, err
	}
	if err = schedule.AdvanceMillis(31000); err != nil {
		return trace, err
	}
	if _, err = protocol.SweepWithReaders(cleanup, now()); err != nil {
		return trace, err
	}
	objects, err := model.Objects(cleanup)
	if err != nil || len(objects) != 0 {
		return trace, fmt.Errorf("contention objects did not drain: %d %v", len(objects), err)
	}
	if err = model.CheckReferences(); err != nil {
		return trace, err
	}
	if err = schedule.Finish(); err != nil {
		return trace, err
	}
	return trace, nil
}
