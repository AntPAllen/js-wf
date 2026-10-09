package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"js-wf/journal"
	"js-wf/wf"
)

// drainCanonicalSignals copies each queued body into SignalConsumed ownership.
// Native source discovery may repair published-but-unbound cuts; consumption
// and replay use the canonical queue and journal, never a source body/ref.
func (w *Worker) drainCanonicalSignals(ctx context.Context, g *graphDelivery, cursor uint64, records []journal.Record, appendEntry func(journal.Kind, json.RawMessage) error, ops *deliveryOperations) (signals []wf.Signal, err error) {
	if cursor != 0 && g.checkpoint == nil {
		return nil, journal.ErrGap
	} // A saved cursor requires a verified materialized graph checkpoint.
	port := w.signalDrainPort
	if port == nil {
		port = NewSignalDrainPort(w.js)
	}
	if ops != nil {
		port = observedSignalDrain{SignalDrainPort: port, operations: ops}
	}
	lookup, stop := context.WithTimeout(ctx, 5*time.Second)
	through, e := port.LastSignalSequence(lookup)
	stop()
	if e != nil {
		return nil, e
	}
	finished := false
	for step := 0; step < 256; step++ {
		lookup, stop = context.WithTimeout(ctx, 5*time.Second)
		progress, e := g.store.BindNextSignal(lookup, g.typ, g.id, g.invocation, through, port)
		stop()
		if e != nil {
			if !errors.Is(e, journal.ErrStale) || ctx.Err() != nil {
				return nil, e
			}
			continue
		}
		if !progress {
			finished = true
			break
		}
	}
	if !finished {
		return nil, fmt.Errorf("%w: canonical source binding budget exhausted", journal.ErrUnknown)
	}
	queue, e := g.store.Open(ctx, g.typ, g.id, g.invocation)
	if e != nil {
		return nil, e
	}
	defer func() {
		cleanup, stop := context.WithTimeout(context.WithoutCancel(ctx), 3*time.Second)
		defer stop()
		if e := queue.Close(cleanup); err == nil {
			err = e
		}
		if err != nil {
			signals = nil
		}
	}()
	next := g.signalBase
	lastSequence := g.signalLast
	for _, record := range records {
		if record.Kind != journal.SignalConsumed {
			continue
		}
		var event signalRecord
		if json.Unmarshal(record.Payload, &event) != nil || event.Canonical == nil || event.Canonical.Index != next || event.Sequence <= lastSequence {
			return nil, wf.ErrCorruptJournal
		}
		binding, e := queue.SignalBindingAt(ctx, next)
		if e != nil {
			return nil, e
		}
		if binding.Input.Token != event.Canonical.Token || binding.Sequence != event.Sequence || binding.Input.Request.Name != event.Name || binding.Input.InputSHA256 != event.Hash {
			return nil, wf.ErrCorruptJournal
		}
		// Canonical replay must resolve the journal-owned edge. Inline bytes
		// or an alternate locator cannot authorize the consumed payload.
		if event.Ref != "graph-signal-"+event.Hash || len(event.Payload) != 0 {
			return nil, wf.ErrCorruptJournal
		}
		data, e := g.GetBytes(ctx, event.Ref)
		if e != nil {
			return nil, e
		}
		if len(data) != binding.Input.InputSize || graphHash(data) != event.Hash {
			return nil, wf.ErrCorruptJournal
		}
		signals = append(signals, wf.Signal{Sequence: event.Sequence, Name: event.Name, Payload: data})
		next++
		lastSequence = event.Sequence
	}
	for ; next < queue.SignalQueueCount(); next++ {
		binding, data, e := queue.SignalAt(ctx, next)
		if e != nil {
			return nil, e
		}
		if binding.Sequence <= lastSequence {
			return nil, wf.ErrCorruptJournal
		}
		ref := "graph-signal-" + binding.Input.InputSHA256
		g.pending[ref] = data
		event := signalRecord{Sequence: binding.Sequence, Name: binding.Input.Request.Name, Ref: ref, Hash: binding.Input.InputSHA256, Canonical: &canonicalSignalRecord{Index: next, Token: binding.Input.Token}}
		encoded, e := json.Marshal(event)
		if e != nil {
			return nil, e
		}
		if e = appendEntry(journal.SignalConsumed, encoded); e != nil {
			return nil, e
		}
		signals = append(signals, wf.Signal{Sequence: event.Sequence, Name: event.Name, Payload: data})
		lastSequence = event.Sequence
	}
	return signals, nil
}
