package journal

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"js-wf/identity"
	"js-wf/internal/retainedgraph"
	"js-wf/internal/retainedindex"
)

const graphSignalQueueForest = "signal-queue"
const graphSignalBindingSchema = "js-wf-canonical-signal-binding-v1"

// GraphSignalSource must return the FIRST retained message at or after from
// matching subject, or ErrMsgNotFound for witnessed absence. Uncertain reads
// must be errors. An arbitrary selected source sequence cannot prove ordering.
type GraphSignalSource interface {
	NextSignal(context.Context, uint64, string) (*jetstream.RawStreamMsg, error)
}

type GraphSignalBinding struct {
	Schema   string           `json:"schema"`
	Input    GraphSignalInput `json:"input"`
	Index    uint64           `json:"index"`
	Sequence uint64           `json:"sequence"`
}
type graphSignalQueueRecord struct {
	Binding     GraphSignalBinding `json:"binding"`
	IndexPacket []byte             `json:"index_packet"`
}
type signalQueueIndexReader struct{ view *GraphView }

func (r signalQueueIndexReader) record(ctx context.Context, index uint64) (graphSignalQueueRecord, retainedgraph.Record, error) {
	v := r.view
	if err := v.alive(); err != nil {
		return graphSignalQueueRecord{}, retainedgraph.Record{}, err
	}
	raw, err := v.store.cfg.Protocol.ReadRetainedStream(ctx, v.reader, graphSignalQueueForest, index, v.store.cfg.Now())
	if err != nil {
		return graphSignalQueueRecord{}, raw, err
	}
	var record graphSignalQueueRecord
	b := &record.Binding
	if graphDecode(raw.Data, &record) != nil || b.Schema != graphSignalBindingSchema || b.Index != index || b.Input.Index >= v.cursor.SignalInputs || !validSignalInput(b.Input, v.cursor, b.Input.Index, v.store.cfg.PayloadReadLimit) || b.Sequence == 0 || b.Sequence > v.cursor.SignalSource || len(record.IndexPacket) > retainedindex.MaxPacketBytes || len(raw.Blobs) != 1 || raw.Blobs[0].Hash != b.Input.InputSHA256 {
		return record, raw, ErrGap
	}
	if err = v.alive(); err != nil {
		return record, raw, err
	}
	return record, raw, nil
}
func (r signalQueueIndexReader) ReadIndex(ctx context.Context, index uint64) ([]byte, error) {
	record, _, err := r.record(ctx, index)
	return record.IndexPacket, err
}

func (v *GraphView) SignalQueueCount() uint64     { return v.cursor.SignalBindings }
func (v *GraphView) SignalSourceSequence() uint64 { return v.cursor.SignalSource }

// SignalConsumedCount is the canonical journal consumption count captured by
// this pin. It does not include bound inputs that have not been consumed.
func (v *GraphView) SignalConsumedCount() uint64 { return v.cursor.SignalConsumed }

// SignalInputAt resolves a pointer's reservation index on this exact retained
// generation. The descriptor and owned body are validated before use.
func (v *GraphView) SignalInputAt(ctx context.Context, index uint64) (GraphSignalInput, []byte, error) {
	if !v.store.cfg.CanonicalSignals || index >= v.cursor.SignalInputs {
		return GraphSignalInput{}, nil, ErrGap
	}
	record, raw, err := (signalInputIndexReader{v}).record(ctx, index)
	if err != nil {
		return GraphSignalInput{}, nil, err
	}
	data, err := v.store.cfg.Protocol.Port.Get(ctx, raw.Blobs[0], v.store.cfg.PayloadReadLimit)
	if err != nil {
		return GraphSignalInput{}, nil, err
	}
	if len(data) != record.Input.InputSize || startHash(data) != record.Input.InputSHA256 {
		return GraphSignalInput{}, nil, ErrGap
	}
	if err = v.alive(); err != nil {
		return GraphSignalInput{}, nil, err
	}
	return record.Input, data, nil
}

// SignalBindingAt validates queue identity on this exact retained generation
// without fetching its input body. This metadata is not a payload grant: fresh
// consumption uses SignalAt; replay validates its separately journal-owned body.
func (v *GraphView) SignalBindingAt(ctx context.Context, index uint64) (GraphSignalBinding, error) {
	if !v.store.cfg.CanonicalSignals || index >= v.cursor.SignalBindings {
		return GraphSignalBinding{}, ErrGap
	}
	record, _, err := (signalQueueIndexReader{v}).record(ctx, index)
	if err != nil {
		return GraphSignalBinding{}, err
	}
	return record.Binding, nil
}

// SignalAt reads canonical queue order, independently of retained WF_SIG.
func (v *GraphView) SignalAt(ctx context.Context, index uint64) (GraphSignalBinding, []byte, error) {
	if !v.store.cfg.CanonicalSignals || index >= v.cursor.SignalBindings {
		return GraphSignalBinding{}, nil, ErrGap
	}
	record, raw, err := (signalQueueIndexReader{v}).record(ctx, index)
	if err != nil {
		return GraphSignalBinding{}, nil, err
	}
	data, err := v.store.cfg.Protocol.Port.Get(ctx, raw.Blobs[0], v.store.cfg.PayloadReadLimit)
	if err != nil {
		return GraphSignalBinding{}, nil, err
	}
	if len(data) != record.Binding.Input.InputSize || startHash(data) != record.Binding.Input.InputSHA256 {
		return GraphSignalBinding{}, nil, ErrGap
	}
	if err = v.alive(); err != nil {
		return GraphSignalBinding{}, nil, err
	}
	return record.Binding, data, nil
}
func (v *GraphView) SignalBinding(ctx context.Context, r GraphSignalRequest) (GraphSignalBinding, []byte, bool, error) {
	if !v.store.cfg.CanonicalSignals || !validSignalRequest(r) || v.cursor.Start == nil {
		return GraphSignalBinding{}, nil, false, ErrGap
	}
	if r.Type != v.cursor.Start.Request.Type || r.ID != v.cursor.Start.Request.ID || r.Invocation != v.cursor.Invocation {
		return GraphSignalBinding{}, nil, false, ErrStale
	}
	if err := v.alive(); err != nil {
		return GraphSignalBinding{}, nil, false, err
	}
	index, found, err := retainedindex.Lookup(ctx, signalQueueIndexReader{v}, v.cursor.SignalBindings, signalIndexKey(r))
	if err != nil || !found {
		return GraphSignalBinding{}, nil, false, err
	}
	binding, data, err := v.SignalAt(ctx, index)
	if err != nil {
		return binding, nil, false, err
	}
	if binding.Input.Request != r {
		return binding, nil, false, ErrGap
	}
	return binding, data, true, nil
}
func (s *GraphStore) ReadSignalBinding(ctx context.Context, r GraphSignalRequest) (binding GraphSignalBinding, data []byte, found bool, err error) {
	if !s.cfg.CanonicalSignals || !validSignalRequest(r) {
		return binding, nil, false, ErrGap
	}
	v, err := s.Open(ctx, r.Type, r.ID, r.Invocation)
	if err != nil {
		return binding, nil, false, err
	}
	defer func() {
		cleanup, stop := context.WithTimeout(context.WithoutCancel(ctx), 3*time.Second)
		defer stop()
		if e := v.Close(cleanup); err == nil {
			err = e
		}
		if err != nil {
			data, found = nil, false
		}
	}()
	return v.SignalBinding(ctx, r)
}

// BindNextSignal processes at most one matching source at or before through.
// It owns the queue body, bound-key index and source frontier in one CAS. A
// duplicate advances only the source frontier and retains the FIRST binding.
// Retired older invocations may be skipped using the root's retirement proof.
// A missing or uncertain current reservation never advances the frontier.
func (s *GraphStore) BindNextSignal(ctx context.Context, typ, id string, invocation, through uint64, source GraphSignalSource) (progress bool, err error) {
	if !s.cfg.CanonicalSignals || source == nil || invocation == 0 || identity.Validate(typ, id) != nil {
		return false, ErrGap
	}
	v, err := s.Open(ctx, typ, id, invocation)
	if err != nil {
		return false, err
	}
	closed := false
	defer func() {
		if !closed {
			cleanup, stop := context.WithTimeout(context.WithoutCancel(ctx), 3*time.Second)
			defer stop()
			if e := v.Close(cleanup); err == nil {
				err = e
			}
			if err != nil {
				progress = false
			}
		}
	}()
	if v.cursor.SignalSource >= through || v.cursor.SignalSource == math.MaxUint64 {
		return false, nil
	}
	prefix := "wf.sig." + typ + "." + id + "."
	msg, err := source.NextSignal(ctx, v.cursor.SignalSource+1, prefix+"*")
	if errors.Is(err, jetstream.ErrMsgNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if msg == nil || msg.Sequence <= v.cursor.SignalSource || !strings.HasPrefix(msg.Subject, prefix) || identity.ValidateToken(strings.TrimPrefix(msg.Subject, prefix)) != nil {
		return false, ErrGap
	}
	if msg.Sequence > through {
		return false, nil
	}
	generation, err := strconv.ParseUint(msg.Header.Get("Wf-Inv-Seq"), 10, 64)
	if err != nil || generation == 0 || strconv.FormatUint(generation, 10) != msg.Header.Get("Wf-Inv-Seq") {
		return false, ErrGap
	}
	if generation > invocation {
		return false, ErrStale
	}
	var binding GraphSignalBinding
	var body, packet []byte
	appendQueue := false
	if generation == invocation {
		var pointer struct {
			Schema string `json:"schema"`
			Token  string `json:"token"`
			Index  uint64 `json:"index"`
		}
		if graphDecode(msg.Data, &pointer) != nil || pointer.Schema != graphSignalPointerSchema || !validStartToken(pointer.Token) {
			return false, ErrGap
		}
		input, data, e := v.SignalInputAt(ctx, pointer.Index)
		if e != nil {
			return false, e
		}
		if !input.MatchesSignal(msg) {
			return false, ErrGap
		}
		reader := signalQueueIndexReader{v}
		index, found, e := retainedindex.Lookup(ctx, reader, v.cursor.SignalBindings, signalIndexKey(input.Request))
		if e != nil {
			return false, e
		}
		if found {
			existing, _, e := reader.record(ctx, index)
			if e != nil {
				return false, e
			}
			if existing.Binding.Input != input {
				return false, ErrGap
			}
		} else {
			if v.cursor.SignalBindings == math.MaxInt64 {
				return false, ErrTooLong
			}
			packet, _, _, e = retainedindex.Update(ctx, reader, v.cursor.SignalBindings, signalIndexKey(input.Request), v.cursor.SignalBindings)
			if e != nil {
				return false, e
			}
			binding = GraphSignalBinding{Schema: graphSignalBindingSchema, Input: input, Index: v.cursor.SignalBindings, Sequence: msg.Sequence}
			body = data
			appendQueue = true
		}
	} else if generation > v.cursor.PreviousInvocation {
		return false, ErrStale
	}
	captured := v.cursor
	cleanup, stop := context.WithTimeout(context.WithoutCancel(ctx), 3*time.Second)
	err = v.Close(cleanup)
	stop()
	closed = true
	if err != nil {
		return false, err
	}
	destination, root, current, err := s.observe(ctx, typ, id)
	if err != nil {
		return false, err
	}
	if current == nil || current.Invocation != invocation || current.Purging || current.Retired || current.SignalBindings != captured.SignalBindings || current.SignalSource != captured.SignalSource {
		return false, ErrStale
	}
	next := *current
	next.SignalSource = msg.Sequence
	if appendQueue {
		next.SignalBindings++
	}
	app, _ := json.Marshal(next)
	if !appendQueue {
		_, err = s.cfg.Protocol.UpdateApplication(ctx, destination, root.Head, app)
	} else {
		data, _ := json.Marshal(graphSignalQueueRecord{Binding: binding, IndexPacket: packet})
		if len(data) > retainedgraph.MaxDataBytes {
			return false, ErrTooLong
		}
		prepared, e := s.cfg.Protocol.PrepareStreamAppendWithApplication(ctx, destination, root.Head, graphSignalQueueForest, data, [][]byte{body}, nil, s.cfg.Now().Add(s.cfg.IntentTTL), app)
		if e != nil {
			return false, graphMutationError(e)
		}
		_, err = s.cfg.Protocol.Commit(ctx, prepared)
	}
	if err != nil {
		return false, graphMutationError(err)
	}
	return true, nil
}
