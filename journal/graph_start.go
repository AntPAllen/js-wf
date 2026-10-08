package journal

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strconv"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"js-wf/identity"
	"js-wf/internal/graphpublication"
)

const GraphStartTokenHeader = "Wf-Graph-Start-Token"
const graphStartSchema = "js-wf-canonical-start-v1"
const graphStartPointerSchema = "js-wf-canonical-start-pointer-v1"

var ErrStartMismatch = errors.New("canonical start input or parent differs")

// GraphStartRequest preserves the write-once input/parent identity. Type/ID
// must match the destination; parent fields are either all absent or complete.
type GraphStartRequest struct {
	Type             string `json:"type"`
	ID               string `json:"id"`
	ParentType       string `json:"parent_type,omitempty"`
	ParentID         string `json:"parent_id,omitempty"`
	ParentInvocation uint64 `json:"parent_invocation,omitempty"`
	SignalName       string `json:"signal_name,omitempty"`
}

// GraphStart is both the bounded cursor descriptor and immutable input-leaf
// metadata. Its token identifies the exact source publication attempt across
// process/client restarts; a matching hash alone cannot bind another attempt.
type GraphStart struct {
	Schema      string            `json:"schema"`
	Token       string            `json:"token"`
	Request     GraphStartRequest `json:"request"`
	InputSHA256 string            `json:"input_sha256"`
	InputSize   int               `json:"input_size"`
}

type GraphStartState struct {
	Start      GraphStart
	Invocation uint64
	Pending    bool
}

func startHash(data []byte) string { h := sha256.Sum256(data); return hex.EncodeToString(h[:]) }
func startHex(value string, size int) bool {
	b, e := hex.DecodeString(value)
	return e == nil && len(b) == size && hex.EncodeToString(b) == value
}
func validStartToken(token string) bool {
	if len(token) == 0 || len(token) > 32 {
		return false
	}
	for _, c := range token {
		if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '-') {
			return false
		}
	}
	return true
}
func validateStartRequest(r GraphStartRequest) error {
	if err := identity.Validate(r.Type, r.ID); err != nil {
		return err
	}
	if r.ParentType == "" && r.ParentID == "" && r.ParentInvocation == 0 && r.SignalName == "" {
		return nil
	}
	if err := identity.Validate(r.ParentType, r.ParentID); err != nil {
		return err
	}
	if r.ParentInvocation == 0 || identity.ValidateToken(r.SignalName) != nil {
		return ErrGap
	}
	return nil
}
func validateStartCursor(root graphpublication.Root, c graphCursor, typ, id string, limit int, signals bool) error {
	if c.Start == nil || c.Start.Schema != graphStartSchema || !validStartToken(c.Start.Token) || !startHex(c.Start.InputSHA256, 32) || c.Start.InputSize < 0 || c.Start.InputSize > limit || validateStartRequest(c.Start.Request) != nil || c.Start.Request.Type != typ || c.Start.Request.ID != id {
		return ErrGap
	}
	if c.Invocation == 0 {
		if c.Count != 0 || c.Kind != "" || c.Epoch != 0 || c.Retired || c.Purging {
			return ErrGap
		}
	} else if c.Invocation <= c.PreviousInvocation {
		return ErrGap
	}
	if len(root.Streams) == 0 || root.Streams[0].Name != "input" || !signals && len(root.Streams) != 1 {
		return ErrGap
	}
	want := uint64(1)
	if c.Retired {
		want = 0
	}
	if root.Streams[0].Graph.Count != want {
		return ErrGap
	}
	if signals {
		if c.Invocation == 0 && (c.SignalInputs != 0 || c.SignalBindings != 0 || c.SignalSource != 0 || c.SignalConsumed != 0 || c.SignalRepair != 0) {
			return ErrGap
		}
		inputs, bindings := uint64(0), uint64(0)
		for _, stream := range root.Streams[1:] {
			switch stream.Name {
			case graphSignalInputForest:
				inputs = stream.Graph.Count
			case graphSignalQueueForest:
				bindings = stream.Graph.Count
			default:
				return ErrGap
			}
		}
		wantInputs, wantBindings := c.SignalInputs, c.SignalBindings
		if c.Retired {
			wantInputs, wantBindings = 0, 0
		}
		if inputs != wantInputs || bindings != wantBindings || c.SignalBindings > 0 && c.SignalSource == 0 {
			return ErrGap
		}
	}

	return nil
}

func startState(c *graphCursor) GraphStartState {
	return GraphStartState{Start: *c.Start, Invocation: c.Invocation, Pending: c.Invocation == 0}
}

// ReserveStart publishes graph-owned input and a pending lifecycle together.
// It never allocates/binds a source sequence or starts runtime history. A
// definite conflict requires a fresh identity observation; unknown outcomes
// remain unknown until the exact canonical descriptor is witnessed.
func (s *GraphStore) ReserveStart(ctx context.Context, r GraphStartRequest, input []byte) (GraphStartState, error) {
	if !s.cfg.CanonicalStarts || validateStartRequest(r) != nil {
		return GraphStartState{}, ErrGap
	}
	if len(input) > s.cfg.PayloadReadLimit {
		return GraphStartState{}, ErrTooLong
	}
	destination, root, c, err := s.observe(ctx, r.Type, r.ID)
	if err != nil {
		return GraphStartState{}, err
	}
	hash := startHash(input)
	if c != nil && !c.Retired {
		if c.Purging {
			return GraphStartState{}, ErrStale
		}
		if c.Start.Request != r || c.Start.InputSHA256 != hash || c.Start.InputSize != len(input) {
			return startState(c), ErrStartMismatch
		}
		return startState(c), nil
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
		return GraphStartState{}, err
	}
	if !validStartToken(token) {
		return GraphStartState{}, ErrGap
	}
	start := GraphStart{Schema: graphStartSchema, Token: token, Request: r, InputSHA256: hash, InputSize: len(input)}
	next := graphCursor{Schema: graphStartCursorSchema, Start: &start}
	if s.cfg.CanonicalSignals {
		next.Schema = graphSignalCursorSchema
	}
	if c != nil {
		next.Base = c.Base + c.Count
		next.PreviousInvocation = c.Invocation
	}
	app, _ := json.Marshal(next)
	metadata, _ := json.Marshal(start)
	prepared, err := s.cfg.Protocol.PrepareStreamAppendWithApplication(ctx, destination, root.Head, "input", metadata, [][]byte{input}, nil, s.cfg.Now().Add(s.cfg.IntentTTL), app)
	if err != nil {
		return GraphStartState{}, graphMutationError(err)
	}
	_, err = s.cfg.Protocol.Commit(ctx, prepared)
	if err != nil {
		return GraphStartState{}, graphMutationError(err)
	}
	return startState(&next), nil
}

// OpenStart pins the pending or ready input graph. It never begins runtime
// history. Old handles remain readable through terminal retirement until their
// lease expires; fresh admission fails once purge has fenced the generation.
func (s *GraphStore) OpenStart(ctx context.Context, typ, id string) (*GraphView, error) {
	if !s.cfg.CanonicalStarts {
		return nil, ErrGap
	}
	for attempt := 0; attempt < 16; attempt++ {
		destination, root, c, err := s.observe(ctx, typ, id)
		if err != nil {
			return nil, err
		}
		if c == nil {
			return nil, nil
		}
		if c.Retired || c.Purging {
			return nil, ErrStale
		}
		expires := s.cfg.Now().Add(s.cfg.PinTTL)
		reader, _, err := s.cfg.Protocol.AcquireReader(ctx, destination, root.Head, expires)
		if err == nil {
			return &GraphView{store: s, destination: destination, reader: reader, cursor: *c, expires: expires}, nil
		}
		if !errors.Is(err, graphpublication.ErrConflict) || ctx.Err() != nil {
			return nil, graphMutationError(err)
		}
	}
	return nil, graphMutationError(graphpublication.ErrConflict)
}

func (v *GraphView) StartState() (GraphStartState, error) {
	if err := v.alive(); err != nil {
		return GraphStartState{}, err
	}
	if v.cursor.Start == nil {
		return GraphStartState{}, ErrGap
	}
	return startState(&v.cursor), nil
}

// StartInput checks the exact immutable descriptor, owned input edge, byte
// count and SHA while retaining the complete captured graph set.
func (v *GraphView) StartInput(ctx context.Context) ([]byte, error) {
	if err := v.alive(); err != nil {
		return nil, err
	}
	if v.cursor.Start == nil {
		return nil, ErrGap
	}
	raw, err := v.store.cfg.Protocol.ReadRetainedStream(ctx, v.reader, "input", 0, v.store.cfg.Now())
	if err != nil {
		return nil, err
	}
	var stored GraphStart
	if graphDecode(raw.Data, &stored) != nil || stored != *v.cursor.Start || len(raw.Blobs) != 1 || raw.Blobs[0].Hash != stored.InputSHA256 {
		return nil, ErrGap
	}
	data, err := v.store.cfg.Protocol.Port.Get(ctx, raw.Blobs[0], v.store.cfg.PayloadReadLimit)
	if err != nil {
		return nil, err
	}
	if len(data) != stored.InputSize || startHash(data) != stored.InputSHA256 {
		return nil, ErrGap
	}
	if err = v.alive(); err != nil {
		return nil, err
	}
	return data, nil
}

// BindStart records a positive, verified source invocation sequence. The
// caller must establish the exact acknowledged/write-once source pointer using
// GraphStart.MatchesInvocation. Presence of an arbitrary source message or
// object is insufficient. Every retry reobserves the original pending token.
func (s *GraphStore) BindStart(ctx context.Context, typ, id, token string, invocation uint64) error {
	if !s.cfg.CanonicalStarts || invocation == 0 {
		return ErrStale
	}
	validated, _, err := s.ReadStart(ctx, typ, id)
	if err != nil {
		return err
	}
	if validated.Start.Token != token {
		return ErrStale
	}
	destination, root, c, err := s.observe(ctx, typ, id)
	if err != nil {
		return err
	}
	if c == nil || c.Start.Token != token || *c.Start != validated.Start || c.Purging || c.Retired || invocation <= c.PreviousInvocation {
		return ErrStale
	}
	if c.Invocation != 0 {
		if c.Invocation == invocation {
			return nil
		}
		return ErrStale
	}
	next := *c
	next.Invocation = invocation
	data, _ := json.Marshal(next)
	_, err = s.cfg.Protocol.UpdateApplication(ctx, destination, root.Head, data)
	return graphMutationError(err)
}

func (start GraphStart) PointerBytes() []byte {
	data, _ := json.Marshal(struct {
		Schema string `json:"schema"`
		Token  string `json:"token"`
	}{graphStartPointerSchema, start.Token})
	return data
}

// MatchesInvocation validates the complete source pointer and parent identity,
// not its sequence allocation. Bound callers additionally compare Sequence to
// the canonical invocation. No legacy object reference can replace this input.
func (start GraphStart) MatchesInvocation(msg *jetstream.RawStreamMsg) bool {
	if msg == nil || msg.Sequence == 0 || msg.Subject != identity.InvocationSubject(start.Request.Type, start.Request.ID) || !bytes.Equal(msg.Data, start.PointerBytes()) {
		return false
	}
	parentSequence := ""
	if start.Request.ParentInvocation != 0 {
		parentSequence = strconv.FormatUint(start.Request.ParentInvocation, 10)
	}
	return msg.Header.Get(GraphStartTokenHeader) == start.Token && msg.Header.Get("Wf-Input-SHA256") == start.InputSHA256 && msg.Header.Get("Wf-Input-Ref") == "" && msg.Header.Get("Wf-Parent-Type") == start.Request.ParentType && msg.Header.Get("Wf-Parent-ID") == start.Request.ParentID && msg.Header.Get("Wf-Parent-Inv-Seq") == parentSequence && msg.Header.Get("Wf-Parent-Signal") == start.Request.SignalName
}

func (v *GraphView) ValidateStartInvocation(ctx context.Context, msg *jetstream.RawStreamMsg) error {
	if !v.store.cfg.CanonicalStarts {
		if msg != nil && msg.Header.Get(GraphStartTokenHeader) != "" {
			return ErrGap
		}
		return nil
	}
	if err := v.alive(); err != nil {
		return err
	}
	if v.cursor.Invocation == 0 || msg == nil || msg.Sequence != v.cursor.Invocation || !v.cursor.Start.MatchesInvocation(msg) {
		return ErrGap
	}
	raw, err := v.store.cfg.Protocol.ReadRetainedStream(ctx, v.reader, "input", 0, v.store.cfg.Now())
	if err != nil {
		return err
	}
	var stored GraphStart
	if graphDecode(raw.Data, &stored) != nil || stored != *v.cursor.Start || len(raw.Blobs) != 1 || raw.Blobs[0].Hash != stored.InputSHA256 {
		return ErrGap
	}
	if err = v.alive(); err != nil {
		return err
	}

	return nil
}

// ReadStart is the input-copy convenience operation used by restarted clients.
// Unknown pin acquisition/release cannot be converted into a successful read.
func (s *GraphStore) ReadStart(ctx context.Context, typ, id string) (state GraphStartState, input []byte, err error) {
	v, err := s.OpenStart(ctx, typ, id)
	if err != nil || v == nil {
		return state, nil, err
	}
	defer func() {
		cleanup, stop := context.WithTimeout(context.WithoutCancel(ctx), 3*time.Second)
		defer stop()
		if e := v.Close(cleanup); err == nil {
			err = e
		}
	}()
	state, err = v.StartState()
	if err != nil {
		return state, nil, err
	}
	input, err = v.StartInput(ctx)
	return state, input, err
}
