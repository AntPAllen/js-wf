package worker

import (
	"context"
	"encoding/json"
	"sort"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"js-wf/identity"
	"js-wf/journal"
	"js-wf/wf"
)

// GraphReplaySnapshot copies offline replay inputs from one pinned canonical
// generation. It never begins a generation, repairs a source or reads WF_BLOB.
type GraphReplaySnapshot struct {
	Format        string
	Input         []byte
	Records       []journal.Record
	Objects       map[string][]byte
	PendingSignal *GraphReplaySignal
}

// GraphReplaySignal describes a rejected drain verified against the captured
// canonical queue and the terminal record's owned payload. It is not a native
// WF_SIG source message.
type GraphReplaySignal struct {
	Sequence        uint64
	Name, Ref, Hash string
}

func ReadGraphReplaySnapshot(ctx context.Context, store *journal.GraphStore, typ, id string, invocation *jetstream.RawStreamMsg) (snapshot GraphReplaySnapshot, err error) {
	if store == nil || !store.CanonicalStarts() || !store.CanonicalSignals() || invocation == nil || identity.Validate(typ, id) != nil {
		return snapshot, journal.ErrGap
	}
	view, err := store.OpenExisting(ctx, typ, id, invocation.Sequence)
	if err != nil {
		return snapshot, err
	}
	if view == nil {
		return snapshot, journal.ErrStale
	}
	defer func() {
		cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), 3*time.Second)
		defer cancel()
		if closeErr := view.Close(cleanup); err == nil && closeErr != nil {
			err = closeErr
		}
		if err != nil {
			snapshot = GraphReplaySnapshot{}
		}
	}()
	if err = view.RenewIfNeeded(ctx); err != nil {
		return snapshot, err
	}
	if err = view.ValidateStartInvocation(ctx, invocation); err != nil {
		return snapshot, err
	}
	snapshot.Input, err = view.StartInput(ctx)
	if err != nil {
		return snapshot, err
	}
	snapshot.Objects = map[string][]byte{}
	g := &graphDelivery{store: store, typ: typ, id: id, invocation: invocation.Sequence, view: view, refs: map[string]graphPayload{}, pending: snapshot.Objects, children: map[string]graphChildResult{}, childSignals: map[uint64]signalRecord{}}
	loadedHashes := map[string]string{}
	next, lastSequence := uint64(0), uint64(0)
	if err = view.RenewIfNeeded(ctx); err != nil {
		return snapshot, err
	}
	err = view.ReadRange(ctx, 0, view.Count(), func(record journal.GraphRecord) error {
		if e := view.RenewIfNeeded(ctx); e != nil {
			return e
		}
		i := record.Index
		if i == 0 && record.Kind != journal.Started {
			return journal.ErrGap
		}
		if i > 0 {
			previous := snapshot.Records[len(snapshot.Records)-1]
			if record.Sequence != previous.Sequence+1 || record.Epoch < previous.Epoch || previous.Kind == journal.Completed || previous.Kind == journal.Failed {
				return journal.ErrGap
			}
		}
		if e := g.register(record); e != nil {
			return e
		}
		refs, e := graphReferences(record.Entry)
		if e != nil {
			return e
		}
		names := make([]string, 0, len(refs))
		for name, hash := range refs {
			if prior, ok := loadedHashes[name]; ok && prior != hash {
				return journal.ErrGap
			}
			if _, ok := snapshot.Objects[name]; !ok {
				names = append(names, name)
			}
		}
		sort.Strings(names)
		for _, name := range names {
			ref := g.refs[name]
			if e := view.RenewIfNeeded(ctx); e != nil {
				return e
			}
			data, e := view.Payload(ctx, ref.index, ref.link, store.PayloadReadLimit())
			if e != nil {
				return e
			}
			snapshot.Objects[name] = data
			loadedHashes[name] = ref.link.Hash
		}
		if e = g.validateChildSignal(ctx, record.Entry); e != nil {
			return e
		}
		var event *signalRecord
		switch record.Kind {
		case journal.Started:
			var start struct {
				Hash string `json:"input_sha256"`
			}
			if i != 0 || json.Unmarshal(record.Payload, &start) != nil || start.Hash != graphHash(snapshot.Input) {
				return journal.ErrGap
			}
		case journal.SignalConsumed:
			event = &signalRecord{}
			if json.Unmarshal(record.Payload, event) != nil {
				return journal.ErrGap
			}
		case journal.Completed, journal.Failed:
			var outcome wf.Outcome
			if json.Unmarshal(record.Payload, &outcome) != nil || outcome.InvSeq != invocation.Sequence || record.Kind == journal.Completed && outcome.Error != "" || record.Kind == journal.Failed && (outcome.Error == "" || len(outcome.Result) != 0 || outcome.ResultRef != "" || outcome.ResultHash != "") {
				return wf.ErrCorruptJournal
			}
			if record.Kind == journal.Completed {
				if _, e := outcome.ResultBytes(ctx, func(_ context.Context, name string) ([]byte, error) {
					data, ok := snapshot.Objects[name]
					if !ok {
						return nil, wf.ErrReplayObjectMissing
					}
					return data, nil
				}); e != nil {
					return e
				}
			}
			if outcome.LimitEntry != nil && outcome.LimitEntry.Kind == string(journal.SignalConsumed) {
				event = &signalRecord{}
				if json.Unmarshal(outcome.LimitEntry.Payload, event) != nil {
					return journal.ErrGap
				}
			}
		}
		if event != nil {
			if event.Canonical == nil || event.Canonical.Index != next || event.Sequence <= lastSequence || event.Ref != "graph-signal-"+event.Hash || len(event.Payload) != 0 {
				return wf.ErrCorruptJournal
			}
			binding, e := view.SignalBindingAt(ctx, next)
			if e != nil {
				return e
			}
			data, ok := snapshot.Objects[event.Ref]
			if !ok || binding.Sequence != event.Sequence || binding.Input.Token != event.Canonical.Token || binding.Input.Request.Name != event.Name || binding.Input.InputSHA256 != event.Hash || binding.Input.InputSize != len(data) || graphHash(data) != event.Hash {
				return wf.ErrCorruptJournal
			}
			if record.Kind == journal.Failed {
				snapshot.PendingSignal = &GraphReplaySignal{Sequence: event.Sequence, Name: event.Name, Ref: event.Ref, Hash: event.Hash}
			} else {
				next++
				lastSequence = event.Sequence
			}
		}
		snapshot.Records = append(snapshot.Records, record.Record)
		return nil
	})
	if err != nil {
		return snapshot, err
	}
	if err = wf.ValidateReplayGraphHistory(snapshot.Records, snapshot.Objects, typ, id, invocation.Sequence, wf.ReplayFormatGraphV1); err != nil {
		return snapshot, err
	}
	snapshot.Format = wf.ReplayFormatGraphV1
	return snapshot, nil
}
