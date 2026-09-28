package client

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"js-wf/identity"
	"js-wf/journal"
	"js-wf/provision"
	"js-wf/retention"
	"js-wf/wf"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

var (
	ErrAlreadyStarted  = errors.New("workflow already started")
	ErrInputMismatch   = errors.New("workflow input differs from original start")
	ErrStartUnknown    = errors.New("start outcome unknown")
	ErrEnqueueUnknown  = errors.New("invocation stored but run enqueue is unconfirmed")
	ErrNotFound        = errors.New("workflow invocation not found")
	ErrNotRunning      = errors.New("workflow invocation is no longer running")
	ErrPurged          = errors.New("workflow invocation was purged")
	ErrSignalUnknown   = errors.New("signal publish outcome unknown")
	ErrSignalMismatch  = errors.New("signal payload differs from earlier idempotency key")
	ErrStaleGeneration = errors.New("invocation generation has been retired or replaced")
	ErrReservedSignal  = errors.New("signal name is reserved for runtime control")
	ErrCancelled       = errors.New("workflow cancelled")
)

const MaxInlineInput = 900 * 1024
const MaxInlineSignal = 600 * 1024
const inputHashHeader = "Wf-Input-SHA256"
const inputRefHeader = "Wf-Input-Ref"
const ParentTypeHeader = "Wf-Parent-Type"
const ParentIDHeader = "Wf-Parent-ID"
const ParentSignalHeader = "Wf-Parent-Signal"
const ParentInvSeqHeader = "Wf-Parent-Inv-Seq"
const CancelSignalName = "_wf_cancel"

type Handle struct {
	Type   string `json:"type"`
	ID     string `json:"id"`
	InvSeq uint64 `json:"inv_seq"`
}

type Client struct {
	js         jetstream.JetStream
	startPort  StartPort
	signalPort SignalPort
	observer   Observer
}

func New(js jetstream.JetStream) *Client { return &Client{js: js} }

// StartPort is the durable boundary used by Start and StartChild. The model
// implements this small surface while the production adapter uses JetStream.
type StartPort interface {
	PublishInvocation(context.Context, *nats.Msg) (uint64, error)
	LastInvocation(context.Context, string) (*jetstream.RawStreamMsg, error)
	PutInput(context.Context, string, []byte) error
	EnqueueRun(context.Context, string, []byte, string) error
	Wait(context.Context, time.Duration) error
}

// NewWithStartPort runs production start decisions against a supplied port.
func NewWithStartPort(port StartPort) *Client { return &Client{startPort: port} }

// NewWithSignalPorts runs production start and signal decisions through
// supplied transports, including SignalWithStart and wakeup enqueue.
func NewWithSignalPorts(start StartPort, signal SignalPort) *Client {
	return &Client{startPort: start, signalPort: signal}
}

func (c *Client) signalOperations() SignalPort {
	if c.signalPort != nil {
		return c.signalPort
	}
	return jetStreamSignalPort{js: c.js}
}

type jetStreamStartPort struct{ js jetstream.JetStream }

func (p jetStreamStartPort) PublishInvocation(ctx context.Context, msg *nats.Msg) (uint64, error) {
	ack, err := p.js.PublishMsg(ctx, msg)
	if err != nil {
		return 0, err
	}
	return ack.Sequence, nil
}

func (p jetStreamStartPort) LastInvocation(ctx context.Context, subject string) (*jetstream.RawStreamMsg, error) {
	stream, err := p.js.Stream(ctx, "WF_INV")
	if err != nil {
		return nil, err
	}
	return stream.GetLastMsgForSubject(ctx, subject)
}

func (p jetStreamStartPort) PutInput(ctx context.Context, key string, input []byte) error {
	objects, err := p.js.ObjectStore(ctx, "WF_BLOB")
	if err != nil {
		return err
	}
	_, err = objects.PutBytes(ctx, key, input)
	return err
}

func (p jetStreamStartPort) EnqueueRun(ctx context.Context, subject string, data []byte, dedupID string) error {
	_, err := p.js.Publish(ctx, subject, data, jetstream.WithMsgID(dedupID))
	return err
}

func (jetStreamStartPort) Wait(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func (c *Client) startOperations() StartPort {
	if c.startPort != nil {
		return c.startPort
	}
	return jetStreamStartPort{js: c.js}
}

// Start stores a write-once invocation and enqueues its first run. The writes
// span two streams, so a reconciler must repair an invocation whose enqueue
// was interrupted. Retrying Start is safe and compares the original input.
func (c *Client) Start(ctx context.Context, typ, id string, input []byte) (handle Handle, err error) {
	if c.observer != nil {
		started := time.Now()
		inputHash := hashBytes(input)
		defer func() {
			c.observe(started, "start", struct {
				Type      string `json:"type"`
				ID        string `json:"id"`
				InputHash string `json:"input_hash"`
				InputSize int    `json:"input_size"`
			}{typ, id, inputHash, len(input)}, struct {
				Status string `json:"status"`
				InvSeq uint64 `json:"inv_seq"`
			}{startStatus(err), handle.InvSeq}, err)
		}()
	}
	return c.start(ctx, typ, id, input, "", "", 0, "")
}

func (c *Client) StartChild(ctx context.Context, typ, id string, input []byte, parentType, parentID string, parentInvSeq uint64, signalName string) (handle Handle, err error) {
	if c.observer != nil {
		started := time.Now()
		inputHash := hashBytes(input)
		defer func() {
			c.observe(started, "start_child", struct {
				Type         string `json:"type"`
				ID           string `json:"id"`
				InputHash    string `json:"input_hash"`
				ParentType   string `json:"parent_type"`
				ParentID     string `json:"parent_id"`
				ParentInvSeq uint64 `json:"parent_inv_seq"`
				SignalName   string `json:"signal_name"`
			}{typ, id, inputHash, parentType, parentID, parentInvSeq, signalName}, struct {
				Status string `json:"status"`
				InvSeq uint64 `json:"inv_seq"`
			}{startStatus(err), handle.InvSeq}, err)
		}()
	}
	if err := identity.Validate(parentType, parentID); err != nil {
		return Handle{}, err
	}
	if parentInvSeq == 0 {
		return Handle{}, fmt.Errorf("parent invocation sequence must be positive")
	}
	if err := identity.ValidateToken(signalName); err != nil {
		return Handle{}, err
	}
	return c.start(ctx, typ, id, input, parentType, parentID, parentInvSeq, signalName)
}

func (c *Client) start(ctx context.Context, typ, id string, input []byte, parentType, parentID string, parentInvSeq uint64, signalName string) (Handle, error) {
	h := Handle{Type: typ, ID: id}
	if err := identity.Validate(typ, id); err != nil {
		return h, err
	}
	digest := sha256.Sum256(input)
	m := &nats.Msg{Subject: identity.InvocationSubject(typ, id), Data: input, Header: nats.Header{}}
	m.Header.Set(inputHashHeader, hex.EncodeToString(digest[:]))
	if parentType != "" {
		m.Header.Set(ParentTypeHeader, parentType)
		m.Header.Set(ParentIDHeader, parentID)
		m.Header.Set(ParentInvSeqHeader, strconv.FormatUint(parentInvSeq, 10))
		m.Header.Set(ParentSignalHeader, signalName)
	}
	if len(input) > MaxInlineInput {
		key := "input-" + hex.EncodeToString(digest[:])
		if err := c.startOperations().PutInput(ctx, key, input); err != nil {
			return h, err
		}
		m.Data = nil
		m.Header.Set(inputRefHeader, key)
	}
	var sequence uint64
	var err error
	for {
		attemptCtx, stop := context.WithTimeout(ctx, 3*time.Second)
		sequence, err = c.startOperations().PublishInvocation(attemptCtx, m)
		stop()
		if err == nil {
			break
		}
		// A rejected duplicate may reach the stream leader before a pinned
		// replica serves that subject. A timeout may also hide a committed
		// publish. Read before retrying so the original handle is returned.
		existing, getErr := c.readStartAfterPublishError(ctx, m.Subject)
		if getErr == nil {
			h.InvSeq = existing.Sequence
			if existing.Header.Get(inputHashHeader) != hex.EncodeToString(digest[:]) || existing.Header.Get(ParentTypeHeader) != parentType || existing.Header.Get(ParentIDHeader) != parentID || existing.Header.Get(ParentInvSeqHeader) != m.Header.Get(ParentInvSeqHeader) || existing.Header.Get(ParentSignalHeader) != signalName {
				return h, ErrInputMismatch
			}
			return h, ErrAlreadyStarted
		}
		var apiErr *jetstream.APIError
		if ctx.Err() != nil || errors.As(err, &apiErr) && !strings.Contains(apiErr.Description, "maximum messages per subject exceeded") {
			return h, fmt.Errorf("%w: publish: %v; read: %v", ErrStartUnknown, err, getErr)
		}
		if waitErr := c.startOperations().Wait(ctx, 50*time.Millisecond); waitErr != nil {
			return h, fmt.Errorf("%w: publish: %v; read: %v", ErrStartUnknown, err, getErr)
		}
	}
	h.InvSeq = sequence
	if err := c.Enqueue(ctx, typ, id, "start:"+identity.Key(typ, id)+":"+strconv.FormatUint(h.InvSeq, 10)); err != nil {
		return h, fmt.Errorf("%w: %v", ErrEnqueueUnknown, err)
	}
	return h, nil
}

func (c *Client) readStartAfterPublishError(ctx context.Context, subject string) (*jetstream.RawStreamMsg, error) {
	readCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	var lastErr error
	for attempt := 0; attempt < 80; attempt++ {
		existing, err := c.startOperations().LastInvocation(readCtx, subject)
		if err == nil {
			return existing, nil
		}
		lastErr = err
		if err := c.startOperations().Wait(readCtx, 25*time.Millisecond); err != nil {
			return nil, fmt.Errorf("%v: %w", lastErr, err)
		}
	}
	return nil, lastErr
}

func (c *Client) Enqueue(ctx context.Context, typ, id, dedupID string) error {
	if err := identity.Validate(typ, id); err != nil {
		return err
	}
	return c.startOperations().EnqueueRun(ctx, identity.RunSubject(typ, id, provision.Partitions), []byte(identity.Key(typ, id)), dedupID)
}

// Signal stores an external event, then enqueues a wakeup. idempotencyKey
// must be stable across retries. JetStream deduplicates it within the stream's
// duplicate window; a reconciler repairs a stored signal with no wakeup.
func (c *Client) Signal(ctx context.Context, typ, id, name string, payload []byte, idempotencyKey string) (sequence uint64, err error) {
	return c.SignalWithOptions(ctx, typ, id, name, payload, idempotencyKey, SignalOptions{})
}

// SignalOptions controls admission checks before publishing a signal.
type SignalOptions struct {
	RequireRunning bool
}

// SignalWithOptions can reject a signal when the journal is already terminal.
// The check and the signal publish are in different streams, so a concurrent
// completion may still win after the check.
func (c *Client) SignalWithOptions(ctx context.Context, typ, id, name string, payload []byte, idempotencyKey string, options SignalOptions) (sequence uint64, err error) {
	if name == CancelSignalName {
		return 0, ErrReservedSignal
	}
	return c.signal(ctx, typ, id, name, payload, idempotencyKey, 0, "signal", options.RequireRunning)
}

// SignalWithStart starts a missing invocation with startInput, then stores the
// signal for that exact generation. A matching concurrent Start is accepted;
// a different input fails with ErrInputMismatch. The signal wakeup also repairs
// a start whose run enqueue acknowledgment was lost.
func (c *Client) SignalWithStart(ctx context.Context, typ, id, name string, payload []byte, idempotencyKey string, startInput []byte) (Handle, uint64, error) {
	if err := identity.Validate(typ, id); err != nil {
		return Handle{}, 0, err
	}
	if err := identity.ValidateToken(name); err != nil {
		return Handle{}, 0, err
	}
	if name == CancelSignalName {
		return Handle{}, 0, ErrReservedSignal
	}
	if idempotencyKey == "" || len(idempotencyKey) > 128 {
		return Handle{}, 0, fmt.Errorf("invalid signal idempotency key")
	}
	handle, err := c.Start(ctx, typ, id, startInput)
	if err != nil && !errors.Is(err, ErrAlreadyStarted) && !errors.Is(err, ErrEnqueueUnknown) {
		return handle, 0, err
	}
	if handle.InvSeq == 0 {
		return handle, 0, ErrStartUnknown
	}
	sequence, err := c.signal(ctx, typ, id, name, payload, idempotencyKey, handle.InvSeq, "signal_with_start", false)
	return handle, sequence, err
}

// SignalToGeneration sends a child result only to the parent invocation that
// created the child. A late child cannot resolve a newer invocation with the
// same parent ID.
func (c *Client) SignalToGeneration(ctx context.Context, typ, id, name string, payload []byte, idempotencyKey string, invSeq uint64) (uint64, error) {
	if invSeq == 0 {
		return 0, fmt.Errorf("invocation sequence must be positive")
	}
	return c.signal(ctx, typ, id, name, payload, idempotencyKey, invSeq, "signal_to_generation", false)
}

// Cancel durably requests cancellation of a live invocation. A worker applies
// the request at its next dispatch; a workflow that has already completed
// keeps its original terminal outcome.
func (c *Client) Cancel(ctx context.Context, typ, id string) (uint64, error) {
	return c.signal(ctx, typ, id, CancelSignalName, nil, "cancel", 0, "cancel", false)
}

func (c *Client) signal(ctx context.Context, typ, id, name string, payload []byte, idempotencyKey string, expectedInvSeq uint64, operation string, requireRunning bool) (sequence uint64, err error) {
	var observedInvSeq uint64
	if c.observer != nil {
		started := time.Now()
		payloadHash := hashBytes(payload)
		defer func() {
			c.observe(started, operation, struct {
				Type           string `json:"type"`
				ID             string `json:"id"`
				Name           string `json:"name"`
				PayloadHash    string `json:"payload_hash"`
				IdempotencyKey string `json:"idempotency_key"`
				InvSeq         uint64 `json:"inv_seq"`
				RequireRunning bool   `json:"require_running,omitempty"`
			}{typ, id, name, payloadHash, idempotencyKey, observedInvSeq, requireRunning}, struct {
				Status    string `json:"status"`
				SignalSeq uint64 `json:"signal_seq"`
			}{signalStatus(err), sequence}, err)
		}()
	}
	if err := identity.Validate(typ, id); err != nil {
		return 0, err
	}
	if err := identity.ValidateToken(name); err != nil {
		return 0, err
	}
	if idempotencyKey == "" || len(idempotencyKey) > 128 {
		return 0, fmt.Errorf("invalid signal idempotency key")
	}
	port := c.signalOperations()
	invocation, err := port.LastInvocation(ctx, identity.InvocationSubject(typ, id))
	if err != nil {
		if errors.Is(err, jetstream.ErrMsgNotFound) {
			if expectedInvSeq != 0 {
				retired, retireErr := c.generationRetired(ctx, typ, id, expectedInvSeq)
				if retireErr != nil {
					return 0, retireErr
				}
				if retired {
					return 0, ErrStaleGeneration
				}
			}
			return 0, ErrNotFound
		}
		return 0, err
	}
	observedInvSeq = invocation.Sequence
	if expectedInvSeq != 0 && invocation.Sequence != expectedInvSeq {
		return 0, ErrStaleGeneration
	}
	if value, getErr := port.StateValue(ctx, identity.Key(typ, id)); getErr == nil {
		marker, tomb, decodeErr := retention.Decode(value)
		if decodeErr != nil {
			return 0, decodeErr
		}
		if tomb && marker.InvSeq == invocation.Sequence {
			return 0, ErrPurged
		}
	} else if !errors.Is(getErr, jetstream.ErrKeyNotFound) {
		return 0, getErr
	}
	if requireRunning {
		last, err := port.LastJournal(ctx, identity.JournalSubject(typ, id))
		if err != nil && !errors.Is(err, jetstream.ErrMsgNotFound) {
			return 0, err
		}
		if err == nil {
			var entry journal.Entry
			if json.Unmarshal(last.Data, &entry) != nil || entry.Kind == "" {
				return 0, journal.ErrGap
			}
			switch entry.Kind {
			case journal.Completed, journal.Failed:
				return 0, ErrNotRunning
			case journal.Started, journal.StepRequested, journal.StepCompleted, journal.Suspended, journal.SignalConsumed, journal.Attempt:
			default:
				return 0, journal.ErrGap
			}
		}
	}
	digest := sha256.Sum256(payload)
	m := &nats.Msg{Subject: "wf.sig." + typ + "." + id + "." + name, Data: payload, Header: nats.Header{}}
	m.Header.Set(inputHashHeader, hex.EncodeToString(digest[:]))
	m.Header.Set("Wf-Inv-Seq", strconv.FormatUint(invocation.Sequence, 10))
	if len(payload) > MaxInlineSignal {
		key := "signal-" + hex.EncodeToString(digest[:])
		if err := port.PutSignalBlob(ctx, key, payload); err != nil {
			return 0, err
		}
		m.Data = nil
		m.Header.Set("Wf-Signal-Ref", key)
	}
	messageID := "signal:" + typ + ":" + id + ":" + strconv.FormatUint(invocation.Sequence, 10) + ":" + name + ":" + idempotencyKey
	ack, err := port.PublishSignal(ctx, m, messageID)
	if err != nil {
		return 0, fmt.Errorf("%w: %v", ErrSignalUnknown, err)
	}
	if ack.Duplicate {
		prior, err := port.SignalBySequence(ctx, ack.Sequence)
		if errors.Is(err, jetstream.ErrMsgNotFound) {
			if err := c.verifyConsumedSignal(ctx, typ, id, name, ack.Sequence, hex.EncodeToString(digest[:])); err != nil {
				return 0, err
			}
		} else if err != nil {
			return 0, err
		} else if prior.Header.Get(inputHashHeader) != hex.EncodeToString(digest[:]) || prior.Header.Get("Wf-Inv-Seq") != strconv.FormatUint(invocation.Sequence, 10) {
			return 0, ErrSignalMismatch
		}
	}
	if err := port.EnqueueRun(ctx, identity.RunSubject(typ, id, provision.Partitions), []byte(identity.Key(typ, id)), fmt.Sprintf("signal-wakeup:%d", ack.Sequence)); err != nil {
		return ack.Sequence, fmt.Errorf("%w: %v", ErrEnqueueUnknown, err)
	}
	return ack.Sequence, nil
}

func (c *Client) generationRetired(ctx context.Context, typ, id string, invSeq uint64) (bool, error) {
	port := c.signalOperations()
	key := identity.Key(typ, id)
	if value, err := port.StateValue(ctx, key); err == nil {
		marker, tomb, err := retention.Decode(value)
		if err != nil {
			return false, err
		}
		if tomb && marker.InvSeq >= invSeq {
			return true, nil
		}
	} else if !errors.Is(err, jetstream.ErrKeyNotFound) {
		return false, err
	}
	if value, err := port.StateValue(ctx, "purging."+key); err == nil {
		retiring, err := strconv.ParseUint(string(value), 10, 64)
		if err != nil {
			return false, err
		}
		return retiring >= invSeq, nil
	} else if !errors.Is(err, jetstream.ErrKeyNotFound) {
		return false, err
	}
	return false, nil
}

func (c *Client) verifyConsumedSignal(ctx context.Context, typ, id, name string, seq uint64, hash string) error {
	records, err := c.signalOperations().ReadJournal(ctx, typ, id)
	if err != nil {
		return err
	}
	for _, record := range records {
		if record.Kind != journal.SignalConsumed {
			continue
		}
		var consumed struct {
			Sequence uint64 `json:"sig_seq"`
			Name     string `json:"name"`
			Hash     string `json:"hash"`
		}
		if err := json.Unmarshal(record.Payload, &consumed); err != nil {
			return fmt.Errorf("corrupt consumed signal: %w", err)
		}
		if consumed.Sequence == seq {
			if consumed.Name != name || consumed.Hash != hash {
				return ErrSignalMismatch
			}
			return nil
		}
	}
	return fmt.Errorf("%w: duplicate signal %d was purged before journal confirmation", ErrSignalUnknown, seq)
}

// Await reads the immutable terminal value from WF_STATE. It is valid after
// a worker has persisted completion and remains stable until retention purge.
func (c *Client) Await(ctx context.Context, typ, id string) (value []byte, err error) {
	var observedInvSeq uint64
	var terminalFailed bool
	if c.observer != nil {
		started := time.Now()
		defer func() {
			c.observe(started, "getResult", struct {
				Type string `json:"type"`
				ID   string `json:"id"`
			}{typ, id}, struct {
				Status     string `json:"status"`
				InvSeq     uint64 `json:"inv_seq"`
				ResultHash string `json:"result_hash,omitempty"`
			}{awaitStatus(err, terminalFailed), observedInvSeq, func() string {
				if err == nil {
					return hashBytes(value)
				}
				if terminalFailed {
					return hashBytes([]byte(err.Error()))
				}
				return ""
			}()}, err)
		}()
	}
	if err := identity.Validate(typ, id); err != nil {
		return nil, err
	}
	state, err := retryAwaitRead(ctx, func(attempt context.Context) (jetstream.KeyValue, error) {
		return c.js.KeyValue(attempt, "WF_STATE")
	})
	if err != nil {
		return nil, err
	}
	inv, err := retryAwaitRead(ctx, func(attempt context.Context) (jetstream.Stream, error) {
		return c.js.Stream(attempt, "WF_INV")
	})
	if err != nil {
		return nil, err
	}
	input, err := retryAwaitRead(ctx, func(attempt context.Context) (*jetstream.RawStreamMsg, error) {
		return inv.GetLastMsgForSubject(attempt, identity.InvocationSubject(typ, id))
	})
	if errors.Is(err, jetstream.ErrMsgNotFound) {
		if value, getErr := retryAwaitRead(ctx, func(attempt context.Context) (jetstream.KeyValueEntry, error) {
			return state.Get(attempt, identity.Key(typ, id))
		}); getErr == nil {
			marker, tomb, decodeErr := retention.Decode(value.Value())
			if decodeErr != nil {
				return nil, decodeErr
			}
			if tomb && time.Now().Before(marker.ExpiresAt) {
				observedInvSeq = marker.InvSeq
				return nil, ErrPurged
			}
		}
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	observedInvSeq = input.Sequence
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		e, err := retryAwaitRead(ctx, func(attempt context.Context) (jetstream.KeyValueEntry, error) {
			return state.Get(attempt, identity.Key(typ, id))
		})
		if err == nil {
			marker, tomb, decodeErr := retention.Decode(e.Value())
			if decodeErr != nil {
				return nil, decodeErr
			}
			if tomb {
				if marker.InvSeq == input.Sequence {
					return nil, ErrPurged
				}
				// A prior generation's tombstone is still present while this
				// invocation is running.
				goto wait
			}
			current, getErr := retryAwaitRead(ctx, func(attempt context.Context) (*jetstream.RawStreamMsg, error) {
				return inv.GetLastMsgForSubject(attempt, identity.InvocationSubject(typ, id))
			})
			if errors.Is(getErr, jetstream.ErrMsgNotFound) || getErr == nil && current.Sequence != input.Sequence {
				return nil, ErrPurged
			}
			if getErr != nil {
				return nil, getErr
			}
			var out wf.Outcome
			if err := json.Unmarshal(e.Value(), &out); err != nil {
				return nil, err
			}
			if out.InvSeq != 0 && out.InvSeq != input.Sequence {
				if out.InvSeq > input.Sequence {
					return nil, ErrPurged
				}
				goto wait
			}
			if out.Error != "" {
				terminalFailed = true
				if out.Error == ErrCancelled.Error() {
					return nil, ErrCancelled
				}
				return nil, errors.New(out.Error)
			}
			if out.ResultRef == "" {
				return out.ResultBytes(ctx, nil)
			}
			objects, err := retryAwaitRead(ctx, func(attempt context.Context) (jetstream.ObjectStore, error) {
				return c.js.ObjectStore(attempt, "WF_BLOB")
			})
			if err != nil {
				return nil, err
			}
			return out.ResultBytes(ctx, func(ctx context.Context, name string) ([]byte, error) {
				return retryAwaitRead(ctx, func(attempt context.Context) ([]byte, error) { return objects.GetBytes(attempt, name) })
			})
		}
		if !errors.Is(err, jetstream.ErrKeyNotFound) {
			return nil, err
		}
	wait:
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-ticker.C:
		}
	}
}

// A JetStream request can wait for the caller's entire Await deadline during
// leader movement. Retry one bounded read against the same durable state.
func retryAwaitRead[T any](ctx context.Context, read func(context.Context) (T, error)) (T, error) {
	for {
		attempt, stop := context.WithTimeout(ctx, 5*time.Second)
		value, err := read(attempt)
		stop()
		if err == nil || !retryableAwaitReadError(err) || ctx.Err() != nil {
			if ctx.Err() != nil && err != nil {
				return value, ctx.Err()
			}
			return value, err
		}
		select {
		case <-ctx.Done():
			var zero T
			return zero, ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
}

func retryableAwaitReadError(err error) bool {
	var api *jetstream.APIError
	if errors.As(err, &api) && api.ErrorCode == 10008 {
		return true
	}
	return errors.Is(err, jetstream.ErrNoStreamResponse) || errors.Is(err, context.DeadlineExceeded) ||
		errors.Is(err, nats.ErrTimeout) || errors.Is(err, nats.ErrNoResponders) ||
		errors.Is(err, nats.ErrDisconnected) || errors.Is(err, nats.ErrConnectionReconnecting)
}
