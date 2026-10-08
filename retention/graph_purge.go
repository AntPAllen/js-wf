package retention

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"js-wf/identity"
	"js-wf/journal"
	"js-wf/lease"
	"js-wf/wf"
)

// GraphPurgePort adds conditional creation for an absent compatibility state.
// Neither compatibility state nor the legacy journal authorizes graph purge.
type GraphPurgePort interface {
	PurgePort
	NativeTimerRetirePort
	CreateState(context.Context, string, []byte) error
}

func (p *jetStreamPurgePort) CreateState(ctx context.Context, key string, value []byte) error {
	state, err := p.stateKV(ctx)
	if err != nil {
		return err
	}
	_, err = state.Create(ctx, key, value)
	return err
}

// PurgeGraph runs ordered retirement using the workers' canonical graph store.
// It fences graph admission before removing dependent stores, then retires live
// graph ownership before publishing the tombstone and removing invocation last.
// Retained readers remain protected until release/expiry. This does not enable GC.
func PurgeGraph(ctx context.Context, js jetstream.JetStream, graph *journal.GraphStore, typ, id string, grace time.Duration) error {
	if graph == nil || identity.Validate(typ, id) != nil || grace <= 0 {
		return fmt.Errorf("invalid graph purge configuration")
	}
	leasing, err := lease.New(ctx, js)
	if err != nil {
		return err
	}
	port := &jetStreamPurgePort{js: js, leasing: leasing, streams: map[string]jetstream.Stream{}}
	return PurgeGraphWithPort(ctx, port, graph, typ, id, grace)
}

func PurgeGraphWithPort(ctx context.Context, port GraphPurgePort, graph *journal.GraphStore, typ, id string, grace time.Duration) error {
	return purgeGraphWithPort(ctx, port, graph, typ, id, 0, grace, nil)
}

func purgeGraphWithPort(ctx context.Context, port GraphPurgePort, graph *journal.GraphStore, typ, id string, expected uint64, grace time.Duration, afterStage func(string) error) (err error) {
	if port == nil || graph == nil || grace <= 0 || identity.Validate(typ, id) != nil {
		return fmt.Errorf("invalid graph purge configuration")
	}
	stage := func(name string) error {
		if afterStage != nil {
			return afterStage(name)
		}
		return nil
	}
	l, err := port.Acquire(ctx, typ, id)
	if errors.Is(err, lease.ErrHeld) || errors.Is(err, lease.ErrLost) && errors.Is(err, jetstream.ErrKeyRevisionMismatch) {
		return ErrActive
	}
	if err != nil {
		return err
	}
	defer func() {
		cleanup, stop := context.WithTimeout(context.WithoutCancel(ctx), 3*time.Second)
		defer stop()
		releaseErr := l.Release(cleanup)
		if err == nil {
			err = releaseErr
		}
	}()
	key := identity.Key(typ, id)
	purgeKey := "purging." + key
	input, invErr := port.Invocation(ctx, identity.InvocationSubject(typ, id))
	if invErr != nil && !errors.Is(invErr, jetstream.ErrMsgNotFound) {
		return invErr
	}
	status, err := graph.InspectRetirement(ctx, typ, id)
	if err != nil {
		return err
	}
	if expected != 0 && (status.Invocation != 0 && status.Invocation != expected || invErr == nil && input.Sequence != expected) {
		return journal.ErrStale
	}
	if errors.Is(invErr, jetstream.ErrMsgNotFound) {
		// Only the canonical committed fence+retirement certifies this resume.
		// Never rerun broad subject purges after invocation deletion/id reuse.
		if !status.Purging || !status.Retired {
			return ErrNotFound
		}
		if err = port.PublishPurge(ctx, typ, id, status.Invocation); err != nil {
			return err
		}
		return clearPurgeMarker(ctx, port, purgeKey, status.Invocation)
	}
	if status.Invocation == 0 {
		return ErrNotTerminal
	}
	if input.Sequence == 0 || status.Invocation != input.Sequence {
		return journal.ErrStale
	}
	if !status.Purging {
		view, openErr := graph.OpenTerminal(ctx, typ, id, input.Sequence)
		if openErr != nil {
			return openErr
		}
		if view == nil {
			return ErrNotTerminal
		}
		func() {
			defer func() {
				cleanup, stop := context.WithTimeout(context.WithoutCancel(ctx), 3*time.Second)
				defer stop()
				closeErr := view.Close(cleanup)
				if err == nil {
					err = closeErr
				}
			}()
			if err = view.ValidateStartInvocation(ctx, &jetstream.RawStreamMsg{Subject: identity.InvocationSubject(typ, id), Sequence: input.Sequence, Header: input.Header, Data: input.Data}); err != nil {
				return
			}
			var terminal wf.GraphTerminal
			terminal, err = wf.ReadGraphTerminal(ctx, view, input.Sequence, graph.PayloadReadLimit())
			if err != nil {
				return
			}
			err = graphChildMayRetire(ctx, port, graph, input, typ, id, terminal)
			if err != nil {
				return
			}
			current, e := port.Invocation(ctx, identity.InvocationSubject(typ, id))
			if e != nil {
				err = e
				return
			}
			if current.Sequence != input.Sequence {
				err = journal.ErrStale
				return
			}
			if err = l.Renew(ctx); err != nil {
				return
			}
			err = graph.FencePurge(ctx, typ, id, input.Sequence, view.Tail())
		}()
		if err != nil {
			return err
		}
	}
	if err = stage("fence"); err != nil {
		return err
	}
	// This compatibility marker is not resume authority. Canonical admission
	// is already fenced even if this publication or its reply is lost.
	if err = l.Renew(ctx); err != nil {
		return err
	}
	if err = port.PutState(ctx, purgeKey, []byte(strconv.FormatUint(input.Sequence, 10))); err != nil {
		return err
	}
	if err = stage("marker"); err != nil {
		return err
	}
	for _, operation := range []struct{ stream, subject, name string }{{"WF_SIG", "wf.sig." + typ + "." + id + ".*", "signals"}, {"WF_JRN", identity.JournalSubject(typ, id), "journal"}} {
		if err = l.Renew(ctx); err != nil {
			return err
		}
		if err = port.PurgeSubject(ctx, operation.stream, operation.subject, 0); err != nil {
			return err
		}
		if err = stage(operation.name); err != nil {
			return err
		}
	}
	hasTimers, err := port.HasFallbackTimers(ctx)
	if err != nil {
		return err
	}
	if hasTimers {
		if err = l.Renew(ctx); err != nil {
			return err
		}
		if err = port.PurgeSubject(ctx, "WF_TIMER", fmt.Sprintf("wf.timer.%s.%s.%d.*", typ, id, input.Sequence), 0); err != nil {
			return err
		}
		if err = stage("timers"); err != nil {
			return err
		}
	}
	snapshot, err := port.State(ctx, "snap."+key)
	if err != nil && !errors.Is(err, jetstream.ErrKeyNotFound) {
		return err
	}
	if err == nil {
		if err = port.DeleteState(ctx, "snap."+key, snapshot.Revision); err != nil {
			return err
		}
	}
	if err = stage("snapshot"); err != nil {
		return err
	}
	if err = l.Renew(ctx); err != nil {
		return err
	}
	if _, err = RetireNativeTimerHints(ctx, port, typ, id, input.Sequence, false); err != nil {
		return err
	}
	if err = stage("native_timers"); err != nil {
		return err
	}
	if err = l.Renew(ctx); err != nil {
		return err
	}
	if err = graph.Retire(ctx, typ, id, input.Sequence, status.Tail); err != nil {
		return err
	}
	if err = stage("graph"); err != nil {
		return err
	}
	if err = l.Renew(ctx); err != nil {
		return err
	}
	if err = writeGraphTombstone(ctx, port, key, input.Sequence, grace); err != nil {
		return err
	}
	if err = stage("tombstone"); err != nil {
		return err
	}
	if err = port.PublishPurge(ctx, typ, id, input.Sequence); err != nil {
		return err
	}
	if err = stage("event"); err != nil {
		return err
	}
	if err = l.Renew(ctx); err != nil {
		return err
	}
	if err = port.PurgeSubject(ctx, "WF_INV", identity.InvocationSubject(typ, id), input.Sequence+1); err != nil {
		return err
	}
	if err = stage("invocation"); err != nil {
		return err
	}
	return clearPurgeMarker(ctx, port, purgeKey, input.Sequence)
}

func writeGraphTombstone(ctx context.Context, port GraphPurgePort, key string, generation uint64, grace time.Duration) error {
	state, err := port.State(ctx, key)
	missing := errors.Is(err, jetstream.ErrKeyNotFound)
	if err != nil && !missing {
		return err
	}
	if !missing {
		marker, tomb, decodeErr := Decode(state.Value)
		if decodeErr == nil && tomb {
			if marker.InvSeq > generation {
				return journal.ErrStale
			}
			if marker.InvSeq == generation {
				return nil
			}
		}
	}
	now := port.Now().UTC()
	data, _ := json.Marshal(Tombstone{Tombstone: true, InvSeq: generation, PurgedAt: now, ExpiresAt: now.Add(grace)})
	if missing {
		err = port.CreateState(ctx, key, data)
	} else {
		err = port.UpdateState(ctx, key, data, state.Revision)
	}
	if err == nil {
		return nil
	}
	// Reconcile only an exact acknowledged/read-back generation tombstone.
	current, readErr := port.State(ctx, key)
	if readErr != nil {
		return err
	}
	marker, tomb, decodeErr := Decode(current.Value)
	if decodeErr != nil || !tomb || marker.InvSeq != generation {
		return err
	}
	return nil
}

func graphChildMayRetire(ctx context.Context, port GraphPurgePort, graph *journal.GraphStore, input PurgeInvocation, typ, id string, terminal wf.GraphTerminal) (err error) {
	parentType := input.Header.Get("Wf-Parent-Type")
	parentID := input.Header.Get("Wf-Parent-ID")
	if parentType == "" && parentID == "" {
		return nil
	}
	if identity.Validate(parentType, parentID) != nil {
		return wf.ErrCorruptJournal
	}
	generation, e := strconv.ParseUint(input.Header.Get("Wf-Parent-Inv-Seq"), 10, 64)
	if e != nil || generation == 0 {
		return wf.ErrCorruptJournal
	}
	signal := input.Header.Get("Wf-Parent-Signal")
	if identity.ValidateToken(signal) != nil {
		return wf.ErrCorruptJournal
	}
	status, err := graph.InspectRetirement(ctx, parentType, parentID)
	if err != nil {
		return err
	}
	if status.Invocation > generation {
		return nil
	} // Begin requires old terminal retirement.
	if status.Invocation != generation {
		return ErrNotTerminal
	}
	if status.Purging || status.Retired {
		return nil
	}
	view, err := graph.OpenExisting(ctx, parentType, parentID, generation)
	if err != nil {
		return err
	}
	if view == nil {
		return ErrNotTerminal
	}
	defer func() {
		cleanup, stop := context.WithTimeout(context.WithoutCancel(ctx), 3*time.Second)
		defer stop()
		closeErr := view.Close(cleanup)
		if err == nil {
			err = closeErr
		}
	}()
	if status.Kind == journal.Completed || status.Kind == journal.Failed {
		_, err = wf.ReadGraphTerminal(ctx, view, generation, graph.PayloadReadLimit())
		return err
	}
	owned, err := wf.GraphOwnsChildResult(ctx, view, typ, id, input.Sequence, signal, terminal, graph.PayloadReadLimit())
	if err != nil {
		return err
	}
	if !owned {
		return ErrNotTerminal
	}
	return nil
}

// PurgeGraphInvocation binds retries to an explicit target generation. A reused
// identity fails rather than authorizing another generation's deletion.
func PurgeGraphInvocation(ctx context.Context, js jetstream.JetStream, graph *journal.GraphStore, typ, id string, invocation uint64, grace time.Duration) error {
	if invocation == 0 || graph == nil || identity.Validate(typ, id) != nil || grace <= 0 {
		return fmt.Errorf("invalid graph purge configuration")
	}
	leasing, err := lease.New(ctx, js)
	if err != nil {
		return err
	}
	return PurgeGraphInvocationWithPort(ctx, &jetStreamPurgePort{js: js, leasing: leasing, streams: map[string]jetstream.Stream{}}, graph, typ, id, invocation, grace)
}

func PurgeGraphInvocationWithPort(ctx context.Context, port GraphPurgePort, graph *journal.GraphStore, typ, id string, invocation uint64, grace time.Duration) error {
	if invocation == 0 {
		return fmt.Errorf("graph purge requires invocation sequence")
	}
	return purgeGraphWithPort(ctx, port, graph, typ, id, invocation, grace, nil)
}
