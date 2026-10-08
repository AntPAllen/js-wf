package journal

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"math"
	"strconv"
	"time"
	"unicode/utf8"

	"github.com/nats-io/nats.go/jetstream"
	"js-wf/identity"
	"js-wf/internal/retainedgraph"
	"js-wf/internal/retainedindex"
)

const graphSignalInputForest = "signal-input"
const graphSignalInputSchema = "js-wf-canonical-signal-input-v1"
const graphSignalPointerSchema = "js-wf-canonical-signal-pointer-v1"
const GraphSignalTokenHeader = "Wf-Graph-Signal-Token"

var ErrSignalMismatch = errors.New("canonical signal input differs")
var ErrSignalNotRunning = errors.New("canonical signal invocation is terminal")

type GraphSignalRequest struct {
	Type       string `json:"type"`
	ID         string `json:"id"`
	Invocation uint64 `json:"invocation"`
	Name       string `json:"name"`
	Key        string `json:"key"`
}

// GraphSignalInput identifies one graph-owned reservation, independently of
// whether its source has been published or bound. Index is forest-local, not a
// WF_SIG sequence. Reservation zero is valid; lookup presence is explicit.
type GraphSignalInput struct {
	Schema      string             `json:"schema"`
	Request     GraphSignalRequest `json:"request"`
	Token       string             `json:"token"`
	Index       uint64             `json:"index"`
	InputSHA256 string             `json:"input_sha256"`
	InputSize   int                `json:"input_size"`
}

type graphSignalInputRecord struct {
	Input       GraphSignalInput `json:"input"`
	IndexPacket []byte           `json:"index_packet"`
}

func validSignalRequest(r GraphSignalRequest) bool {
	return identity.Validate(r.Type, r.ID) == nil && identity.ValidateToken(r.Name) == nil && len(r.Type) <= 256 && len(r.ID) <= 256 && len(r.Name) <= 256 && r.Invocation > 0 && len(r.Key) > 0 && len(r.Key) <= 128 && utf8.ValidString(r.Key)
}
func signalIndexKey(r GraphSignalRequest) retainedindex.Key {
	data, _ := json.Marshal(r)
	return retainedindex.Key(sha256.Sum256(data))
}
func validSignalInput(input GraphSignalInput, cursor graphCursor, index uint64, limit int) bool {
	return cursor.Start != nil && input.Schema == graphSignalInputSchema && validSignalRequest(input.Request) && input.Request.Type == cursor.Start.Request.Type && input.Request.ID == cursor.Start.Request.ID && input.Request.Invocation == cursor.Invocation && input.Index == index && validStartToken(input.Token) && startHex(input.InputSHA256, 32) && input.InputSize >= 0 && input.InputSize <= limit
}

type signalInputIndexReader struct{ view *GraphView }

func (r signalInputIndexReader) record(ctx context.Context, index uint64) (graphSignalInputRecord, retainedgraph.Record, error) {
	v := r.view
	if err := v.alive(); err != nil {
		return graphSignalInputRecord{}, retainedgraph.Record{}, err
	}
	raw, err := v.store.cfg.Protocol.ReadRetainedStream(ctx, v.reader, graphSignalInputForest, index, v.store.cfg.Now())
	if err != nil {
		return graphSignalInputRecord{}, raw, err
	}
	var record graphSignalInputRecord
	if graphDecode(raw.Data, &record) != nil || !validSignalInput(record.Input, v.cursor, index, v.store.cfg.PayloadReadLimit) || len(record.IndexPacket) > retainedindex.MaxPacketBytes || len(raw.Blobs) != 1 || raw.Blobs[0].Hash != record.Input.InputSHA256 {
		return record, raw, ErrGap
	}
	if err = v.alive(); err != nil {
		return record, raw, err
	}
	return record, raw, nil
}
func (r signalInputIndexReader) ReadIndex(ctx context.Context, index uint64) ([]byte, error) {
	record, _, err := r.record(ctx, index)
	return record.IndexPacket, err
}

func signalAdmission(c *graphCursor, r GraphSignalRequest, requireRunning bool) error {
	if c == nil || c.Invocation != r.Invocation || c.Purging || c.Retired {
		return ErrStale
	}
	if requireRunning && (c.Kind == Completed || c.Kind == Failed) {
		return ErrSignalNotRunning
	}
	return nil
}

// ReserveSignal atomically publishes immutable request/input identity, owned
// input and the updated bounded key index. It never writes WF_SIG or enqueues.
// A definite conflict requires fresh admission; an uncertain outcome is not
// success and must be recovered using an authoritative retained input read.
func (s *GraphStore) ReserveSignal(ctx context.Context, r GraphSignalRequest, input []byte, requireRunning bool) (result GraphSignalInput, err error) {
	if !s.cfg.CanonicalSignals || !validSignalRequest(r) {
		return result, ErrGap
	}
	if len(input) > s.cfg.PayloadReadLimit {
		return result, ErrTooLong
	}
	_, _, c, err := s.observe(ctx, r.Type, r.ID)
	if err != nil {
		return result, err
	}
	if err = signalAdmission(c, r, requireRunning); err != nil {
		return result, err
	}
	v, err := s.Open(ctx, r.Type, r.ID, r.Invocation)
	if err != nil {
		return result, err
	}
	closed := false
	defer func() {
		if !closed {
			cleanup, stop := context.WithTimeout(context.WithoutCancel(ctx), 3*time.Second)
			defer stop()
			if e := v.Close(cleanup); err == nil {
				err = e
			}
		}
	}()
	if err = signalAdmission(&v.cursor, r, requireRunning); err != nil {
		return result, err
	}
	reader := signalInputIndexReader{v}
	index, found, err := retainedindex.Lookup(ctx, reader, v.cursor.SignalInputs, signalIndexKey(r))
	if err != nil {
		return result, err
	}
	if found {
		record, raw, e := reader.record(ctx, index)
		if e != nil {
			return result, e
		}
		if record.Input.Request != r || record.Input.InputSize != len(input) || record.Input.InputSHA256 != startHash(input) {
			return record.Input, ErrSignalMismatch
		}
		// Hash/size equality is not a presence-based adoption of an object.
		data, e := s.cfg.Protocol.Port.Get(ctx, raw.Blobs[0], s.cfg.PayloadReadLimit)
		if e != nil {
			return result, e
		}
		if !bytes.Equal(data, input) || startHash(data) != record.Input.InputSHA256 {
			return result, ErrGap
		}
		if e = v.alive(); e != nil {
			return result, e
		}
		return record.Input, nil
	}
	if v.cursor.SignalInputs == math.MaxInt64 {
		return result, ErrTooLong
	}
	packet, _, _, err := retainedindex.Update(ctx, reader, v.cursor.SignalInputs, signalIndexKey(r), v.cursor.SignalInputs)
	if err != nil {
		return result, err
	}
	captured := v.cursor.SignalInputs
	cleanup, stop := context.WithTimeout(context.WithoutCancel(ctx), 3*time.Second)
	err = v.Close(cleanup)
	stop()
	closed = true
	if err != nil {
		return result, err
	}
	destination, root, current, err := s.observe(ctx, r.Type, r.ID)
	if err != nil {
		return result, err
	}
	if err = signalAdmission(current, r, requireRunning); err != nil {
		return result, err
	}
	if current.SignalInputs != captured {
		return result, ErrStale
	}
	token := ""
	if s.cfg.Protocol.NewID != nil {
		token, err = s.cfg.Protocol.NewID()
	} else {
		var random [16]byte
		_, err = rand.Read(random[:])
		token = hex.EncodeToString(random[:])
	}
	if err != nil {
		return result, err
	}
	if !validStartToken(token) {
		return result, ErrGap
	}
	result = GraphSignalInput{Schema: graphSignalInputSchema, Request: r, Token: token, Index: captured, InputSHA256: startHash(input), InputSize: len(input)}
	data, _ := json.Marshal(graphSignalInputRecord{Input: result, IndexPacket: packet})
	if len(data) > retainedgraph.MaxDataBytes {
		return result, ErrTooLong
	}
	next := *current
	next.SignalInputs++
	if current.SignalInputs == current.SignalBindings {
		next.SignalRepair = captured
	}
	app, _ := json.Marshal(next)
	prepared, err := s.cfg.Protocol.PrepareStreamAppendWithApplication(ctx, destination, root.Head, graphSignalInputForest, data, [][]byte{input}, nil, s.cfg.Now().Add(s.cfg.IntentTTL), app)
	if err != nil {
		return result, graphMutationError(err)
	}
	_, err = s.cfg.Protocol.Commit(ctx, prepared)
	return result, graphMutationError(err)
}

// ReadSignalInput validates the exact retained request and owned body. A missing
// key is a successful witnessed absence (found=false), never a swallowed read
// error. This permits recovery without caller input or a local prepared object.
func (s *GraphStore) ReadSignalInput(ctx context.Context, r GraphSignalRequest) (result GraphSignalInput, input []byte, found bool, err error) {
	if !s.cfg.CanonicalSignals || !validSignalRequest(r) {
		return result, nil, false, ErrGap
	}
	v, err := s.Open(ctx, r.Type, r.ID, r.Invocation)
	if err != nil {
		return result, nil, false, err
	}
	defer func() {
		cleanup, stop := context.WithTimeout(context.WithoutCancel(ctx), 3*time.Second)
		defer stop()
		if e := v.Close(cleanup); err == nil {
			err = e
		}
		if err != nil {
			input, found = nil, false
		}
	}()
	return v.SignalInput(ctx, r)
}

// SignalInput reads from this exact retained invocation snapshot. An acquired
// view may outlive retirement; a new store read cannot acquire a retired view.
func (v *GraphView) SignalInput(ctx context.Context, r GraphSignalRequest) (result GraphSignalInput, input []byte, found bool, err error) {
	if !v.store.cfg.CanonicalSignals || !validSignalRequest(r) || v.cursor.Start == nil {
		return result, nil, false, ErrGap
	}
	if r.Invocation != v.cursor.Invocation || r.Type != v.cursor.Start.Request.Type || r.ID != v.cursor.Start.Request.ID {
		return result, nil, false, ErrStale
	}
	if err = v.alive(); err != nil {
		return result, nil, false, err
	}
	reader := signalInputIndexReader{v}
	index, found, err := retainedindex.Lookup(ctx, reader, v.cursor.SignalInputs, signalIndexKey(r))
	if err != nil || !found {
		return result, nil, found, err
	}
	record, raw, err := reader.record(ctx, index)
	if err != nil {
		return result, nil, false, err
	}
	if record.Input.Request != r {
		return result, nil, false, ErrGap
	}
	input, err = v.store.cfg.Protocol.Port.Get(ctx, raw.Blobs[0], v.store.cfg.PayloadReadLimit)
	if err != nil {
		return result, nil, false, err
	}
	if len(input) != record.Input.InputSize || startHash(input) != record.Input.InputSHA256 {
		return result, nil, false, ErrGap
	}
	if err = v.alive(); err != nil {
		return result, nil, false, err
	}
	return record.Input, input, true, nil
}

func (input GraphSignalInput) PointerBytes() []byte {
	data, _ := json.Marshal(struct {
		Schema string `json:"schema"`
		Token  string `json:"token"`
		Index  uint64 `json:"index"`
	}{graphSignalPointerSchema, input.Token, input.Index})
	return data
}

func (input GraphSignalInput) MatchesSignal(msg *jetstream.RawStreamMsg) bool {
	r := input.Request
	return msg != nil && msg.Sequence > 0 && msg.Subject == "wf.sig."+r.Type+"."+r.ID+"."+r.Name && bytes.Equal(msg.Data, input.PointerBytes()) && msg.Header.Get(GraphSignalTokenHeader) == input.Token && msg.Header.Get("Wf-Input-SHA256") == input.InputSHA256 && msg.Header.Get("Wf-Inv-Seq") == strconv.FormatUint(r.Invocation, 10) && msg.Header.Get("Wf-Signal-Ref") == ""
}
