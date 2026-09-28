package worker

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"js-wf/client"
	"js-wf/identity"
	"js-wf/journal"
	"js-wf/lease"
	"js-wf/provision"
	"js-wf/retention"
	"js-wf/wf"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

type Handler func(*wf.Context, json.RawMessage) (json.RawMessage, error)

type timerWakeup struct {
	generation uint64
	step       uint64
	scheduled  bool
}

type signalRecord struct {
	Sequence uint64 `json:"sig_seq"`
	Name     string `json:"name"`
	Payload  []byte `json:"payload,omitempty"`
	Ref      string `json:"ref,omitempty"`
	Hash     string `json:"hash,omitempty"`
}

type cancelWaiter struct {
	cancel   context.CancelFunc
	detected bool
}

type Worker struct {
	js                   jetstream.JetStream
	jrn                  *journal.Store
	leases               *lease.Store
	state                jetstream.KeyValue
	client               *client.Client
	ID                   string
	Handlers             map[string]Handler
	maxEntries           uint64
	maxPanicAttempts     int
	partitionConcurrency int
	ackWait              time.Duration
	heartbeatInterval    time.Duration
	nativeSchedules      bool
	metrics              metricsCounters
	cancelMu             sync.Mutex
	cancelWaiters        map[string]*cancelWaiter
	cancelStream         jetstream.Stream
	cancelSubscription   *nats.Subscription
}

type Option func(*Worker) error

// WithMaxPanicAttempts sets the number of journaled handler panics allowed
// before the invocation is durably failed. It must be set before RunPartition.
func WithMaxPanicAttempts(count int) Option {
	return func(w *Worker) error {
		if count < 1 {
			return fmt.Errorf("max panic attempts must be positive")
		}
		w.maxPanicAttempts = count
		return nil
	}
}

// WithPartitionConcurrency bounds the number of messages handled at once by
// each partition loop. The invocation lease still serializes messages for the
// same workflow. The default is one, preserving strict serial dispatch.
func WithPartitionConcurrency(count int) Option {
	return func(w *Worker) error {
		if count < 1 || count > 256 {
			return fmt.Errorf("partition concurrency must be between 1 and 256")
		}
		w.partitionConcurrency = count
		return nil
	}
}

// WithDispatchTiming controls redelivery after a worker loses access to the
// consumer. Progress heartbeats must be frequent enough to protect live work.
// Every worker sharing a durable partition must use the same AckWait.
func WithDispatchTiming(ackWait, heartbeat time.Duration) Option {
	return func(w *Worker) error {
		if heartbeat < time.Second || ackWait < 3*time.Second || ackWait > 5*time.Minute || heartbeat > ackWait/3 {
			return fmt.Errorf("invalid dispatch acknowledgment and heartbeat intervals")
		}
		w.ackWait = ackWait
		w.heartbeatInterval = heartbeat
		return nil
	}
}

func New(ctx context.Context, js jetstream.JetStream, id string, handlers map[string]Handler, options ...Option) (*Worker, error) {
	if id == "" {
		return nil, fmt.Errorf("empty worker ID")
	}
	// A cluster restart can leave one metadata lookup waiting for the caller's
	// whole workflow lifetime. Bound constructor I/O so the caller can retry
	// against the recovered leader.
	attemptCtx, stopAttempt := context.WithTimeout(ctx, 5*time.Second)
	defer stopAttempt()
	l, err := lease.New(attemptCtx, js)
	if err != nil {
		return nil, fmt.Errorf("lease bucket: %w", err)
	}
	state, err := js.KeyValue(attemptCtx, "WF_STATE")
	if err != nil {
		return nil, fmt.Errorf("state bucket: %w", err)
	}
	run, err := js.Stream(attemptCtx, "WF_RUN")
	if err != nil {
		return nil, fmt.Errorf("run stream: %w", err)
	}
	runInfo, err := run.Info(attemptCtx)
	if err != nil {
		return nil, fmt.Errorf("run stream info: %w", err)
	}
	if !runInfo.Config.AllowMsgSchedules {
		if _, err := js.Stream(attemptCtx, "WF_TIMER"); err != nil {
			return nil, fmt.Errorf("fallback timer stream: %w", err)
		}
	}
	w := &Worker{js: js, jrn: journal.New(js), leases: l, state: state, client: client.New(js), ID: id, Handlers: handlers, maxEntries: journal.MaxEntries, maxPanicAttempts: 3, partitionConcurrency: 1, ackWait: 30 * time.Second, heartbeatInterval: 10 * time.Second, nativeSchedules: runInfo.Config.AllowMsgSchedules, cancelWaiters: make(map[string]*cancelWaiter)}
	for _, option := range options {
		if err := option(w); err != nil {
			return nil, err
		}
	}
	w.cancelStream, err = js.Stream(attemptCtx, "WF_SIG")
	if err != nil {
		return nil, fmt.Errorf("signal stream: %w", err)
	}
	w.cancelSubscription, err = js.Conn().Subscribe("wf.sig.*.*."+client.CancelSignalName, w.observeCancel)
	if err != nil {
		return nil, err
	}
	if err := js.Conn().FlushWithContext(attemptCtx); err != nil {
		_ = w.cancelSubscription.Unsubscribe()
		return nil, fmt.Errorf("cancel subscription flush: %w", err)
	}
	return w, nil
}

type panicRetryError struct{ attempt int }

func (e *panicRetryError) Error() string { return fmt.Sprintf("handler panic attempt %d", e.attempt) }

func (e *panicRetryError) Delay() time.Duration {
	if e.attempt >= 10 {
		return 5 * time.Minute
	}
	return time.Second << (e.attempt - 1)
}

func (w *Worker) consumer(ctx context.Context, partition uint32) (jetstream.Consumer, error) {
	if partition >= provision.Partitions {
		return nil, fmt.Errorf("partition out of range")
	}
	attemptCtx, stopAttempt := context.WithTimeout(ctx, 5*time.Second)
	defer stopAttempt()
	run, err := w.js.Stream(attemptCtx, "WF_RUN")
	if err != nil {
		return nil, err
	}
	name := fmt.Sprintf("WF_P_%02d", partition)
	want := jetstream.ConsumerConfig{Name: name, Durable: name, FilterSubject: "wf.run." + strconv.FormatUint(uint64(partition), 10), AckPolicy: jetstream.AckExplicitPolicy, AckWait: w.ackWait, MaxDeliver: -1, MaxAckPending: 1000}
	c, err := run.CreateConsumer(attemptCtx, want)
	if err != nil {
		return nil, err
	}
	return c, nil
}

// RunPartition processes one partition until cancellation. Multiple workers
// may share the consumer; the per-invocation lease fences concurrent delivery.
func (w *Worker) RunPartition(ctx context.Context, partition uint32) error {
	var slots chan struct{}
	var active sync.WaitGroup
	retryDelay := 100 * time.Millisecond
	backoff := func() bool {
		delay := retryDelay
		if retryDelay < 2*time.Second {
			retryDelay *= 2
			if retryDelay > 2*time.Second {
				retryDelay = 2 * time.Second
			}
		}
		select {
		case <-ctx.Done():
			return false
		case <-time.After(delay):
			return true
		}
	}
	if w.partitionConcurrency > 1 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithCancel(ctx)
		slots = make(chan struct{}, w.partitionConcurrency)
		defer func() {
			cancel()
			active.Wait()
		}()
	}
	for ctx.Err() == nil {
		c, err := w.consumer(ctx, partition)
		if err != nil {
			if !retryableConsumerError(err) {
				return err
			}
			if !backoff() {
				return nil
			}
			continue
		}
		emptyPolls := 0
		refreshAfterEmpty := func() bool {
			emptyPolls++
			if emptyPolls < 3 {
				return false
			}
			emptyPolls = 0
			attempt, stop := context.WithTimeout(ctx, 2*time.Second)
			info, err := c.Info(attempt)
			stop()
			active := 0
			if slots != nil {
				active = len(slots)
			}
			return err != nil || info == nil || info.NumPending > 0 || info.NumAckPending > active
		}
		for ctx.Err() == nil {
			if slots != nil {
				select {
				case slots <- struct{}{}:
				case <-ctx.Done():
					return nil
				}
			}
			batch, err := c.Fetch(1, jetstream.FetchMaxWait(time.Second))
			if err != nil {
				if slots != nil {
					<-slots
				}
				if ctx.Err() != nil {
					return nil
				}
				if errors.Is(err, nats.ErrTimeout) || errors.Is(err, jetstream.ErrNoMessages) || errors.Is(err, context.DeadlineExceeded) {
					retryDelay = 100 * time.Millisecond
					if refreshAfterEmpty() {
						break
					}
					continue
				}
				if retryableConsumerError(err) {
					if !backoff() {
						return nil
					}
					break
				}
				return err
			}
			dispatched := false
			for msg := range batch.Messages() {
				dispatched = true
				emptyPolls = 0
				retryDelay = 100 * time.Millisecond
				if slots == nil {
					w.handle(ctx, msg)
					continue
				}
				active.Add(1)
				go func(msg jetstream.Msg) {
					defer active.Done()
					defer func() { <-slots }()
					w.handle(ctx, msg)
				}(msg)
			}
			if slots != nil && !dispatched {
				<-slots
			}
			batchErr := batch.Error()
			if !dispatched && (batchErr == nil || errors.Is(batchErr, context.DeadlineExceeded) || errors.Is(batchErr, nats.ErrTimeout) || errors.Is(batchErr, jetstream.ErrNoMessages)) && refreshAfterEmpty() {
				break
			}
			if batchErr != nil && !errors.Is(batchErr, context.DeadlineExceeded) && !errors.Is(batchErr, nats.ErrTimeout) && !errors.Is(batchErr, jetstream.ErrNoMessages) {
				if ctx.Err() != nil {
					return nil
				}
				if retryableConsumerError(batchErr) {
					if !backoff() {
						return nil
					}
					break
				}
				return batchErr
			}
		}
	}
	return nil
}

func retryableConsumerError(err error) bool {
	var api *jetstream.APIError
	if errors.As(err, &api) && (api.ErrorCode == 10008 || api.ErrorCode == 10164) { // Cluster unavailable or transient CAS during leader movement.
		return true
	}
	return errors.Is(err, nats.ErrTimeout) || errors.Is(err, nats.ErrNoResponders) ||
		errors.Is(err, jetstream.ErrNoStreamResponse) ||
		errors.Is(err, nats.ErrConnectionReconnecting) || errors.Is(err, nats.ErrDisconnected) ||
		errors.Is(err, nats.ErrNoServers) || errors.Is(err, context.DeadlineExceeded) ||
		errors.Is(err, jetstream.ErrConsumerLeadershipChanged) || errors.Is(err, jetstream.ErrServerShutdown)
}

func (w *Worker) handle(parent context.Context, msg jetstream.Msg) {
	parts := strings.Split(string(msg.Data()), ".")
	if len(parts) != 2 || identity.Validate(parts[0], parts[1]) != nil {
		_ = msg.Term()
		return
	}
	typ, id := parts[0], parts[1]
	var timer timerWakeup
	if generation := msg.Headers().Get(identity.TimerInvSeqHeader); generation != "" {
		step := msg.Headers().Get(identity.TimerStepHeader)
		var err error
		timer.generation, err = strconv.ParseUint(generation, 10, 64)
		if err != nil || timer.generation == 0 || step == "" {
			_ = msg.Term()
			return
		}
		timer.step, err = strconv.ParseUint(step, 10, 64)
		if err != nil {
			_ = msg.Term()
			return
		}
		timer.scheduled = true
	} else if msg.Headers().Get(identity.TimerStepHeader) != "" {
		_ = msg.Term()
		return
	}
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	metadata, err := msg.Metadata()
	if err != nil {
		_ = msg.NakWithDelay(time.Second)
		return
	}
	w.metrics.recordRedelivery(metadata)
	acquireCtx, stopAcquire := context.WithTimeout(ctx, 5*time.Second)
	l, err := w.leases.Acquire(acquireCtx, typ, id, w.ID)
	stopAcquire()
	if errors.Is(err, lease.ErrHeld) {
		w.metrics.leaseContentions.Add(1)
		_ = msg.NakWithDelay(time.Second)
		return
	}
	if err != nil {
		w.metrics.leaseAcquireFailures.Add(1)
		if errors.Is(err, lease.ErrLost) {
			w.metrics.fencingEvents.Add(1)
		}
		_ = msg.NakWithDelay(time.Second)
		return
	}
	w.metrics.leaseAcquisitions.Add(1)
	w.metrics.recordLeaseLatency(metadata, time.Now())
	release := func() error {
		releaseCtx, stopRelease := context.WithTimeout(context.Background(), 15*time.Second)
		defer stopRelease()
		attempt, stopAttempt := context.WithTimeout(releaseCtx, 2*time.Second)
		err := l.Release(attempt)
		stopAttempt()
		if err == nil {
			return nil
		}
		for releaseCtx.Err() == nil {
			attempt, stopAttempt := context.WithTimeout(releaseCtx, 2*time.Second)
			cleanupErr := l.Cleanup(attempt)
			stopAttempt()
			if cleanupErr == nil || errors.Is(cleanupErr, lease.ErrLost) {
				return cleanupErr
			}
			select {
			case <-releaseCtx.Done():
			case <-time.After(200 * time.Millisecond):
			}
		}
		return fmt.Errorf("lease cleanup after %v: %w", err, releaseCtx.Err())
	}
	released := false
	defer func() {
		if !released {
			_ = release()
		}
	}()
	var leaseLost atomic.Bool
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		ticker := time.NewTicker(w.heartbeatInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				renewCtx, stopRenew := context.WithTimeout(ctx, 3*time.Second)
				err := l.Renew(renewCtx)
				stopRenew()
				if err != nil {
					if errors.Is(err, lease.ErrLost) {
						leaseLost.Store(true)
					}
					cancel()
					return
				}
				if err := msg.InProgress(); err != nil {
					cancel()
					return
				}
			}
		}
	}()
	var cancelledTimerNoOp bool
	err = w.execute(ctx, typ, id, l, metadata.Timestamp, timer, &cancelledTimerNoOp)
	if err == nil && ctx.Err() == nil {
		err = w.jrn.MaybeSnapshot(ctx, typ, id, 256, 16)
	}
	processingCanceled := ctx.Err() != nil
	cancel()
	<-stopped
	if leaseLost.Load() || errors.Is(err, lease.ErrLost) || errors.Is(err, journal.ErrStale) {
		w.metrics.fencingEvents.Add(1)
	}
	if err != nil || processingCanceled || leaseLost.Load() {
		delay := time.Second
		var retry *panicRetryError
		if errors.As(err, &retry) {
			delay = retry.Delay()
		}
		_ = msg.NakWithDelay(delay)
		return
	}
	if err := release(); err != nil {
		if errors.Is(err, lease.ErrLost) && !leaseLost.Load() && !errors.Is(err, journal.ErrStale) {
			w.metrics.fencingEvents.Add(1)
		}
		_ = msg.NakWithDelay(time.Second)
		return
	}
	released = true
	if err := msg.Ack(); err == nil && cancelledTimerNoOp {
		w.metrics.cancelledTimerNoOps.Add(1)
	}
}

func (w *Worker) execute(ctx context.Context, typ, id string, l *lease.Lease, wakeupAt time.Time, timer timerWakeup, cancelledTimerNoOp *bool) error {
	readCtx, stopRead := context.WithTimeout(ctx, 15*time.Second)
	records, tail, err := w.jrn.Read(readCtx, typ, id)
	stopRead()
	if err != nil {
		return err
	}
	lookupCtx, stopLookup := context.WithTimeout(ctx, 5*time.Second)
	inv, err := w.js.Stream(lookupCtx, "WF_INV")
	stopLookup()
	if err != nil {
		return err
	}
	lookupCtx, stopLookup = context.WithTimeout(ctx, 5*time.Second)
	input, err := inv.GetLastMsgForSubject(lookupCtx, identity.InvocationSubject(typ, id))
	stopLookup()
	if errors.Is(err, jetstream.ErrMsgNotFound) {
		return nil
	} // Purged invocation: wakeup is a no-op.
	if err != nil {
		return err
	}
	if timer.scheduled && timer.generation != input.Sequence {
		return nil
	}
	if timer.scheduled && timerWasCancelled(records, timer.step) {
		*cancelledTimerNoOp = true
	}
	inputData := input.Data
	if key := input.Header.Get("Wf-Input-Ref"); key != "" {
		objects, err := w.js.ObjectStore(ctx, "WF_BLOB")
		if err != nil {
			return err
		}
		inputData, err = objects.GetBytes(ctx, key)
		if err != nil {
			return err
		}
	}
	if expected := input.Header.Get("Wf-Input-SHA256"); expected != "" {
		digest := sha256.Sum256(inputData)
		if hex.EncodeToString(digest[:]) != expected {
			return fmt.Errorf("input hash mismatch for %s.%s", typ, id)
		}
	}
	if len(records) > 0 && (records[len(records)-1].Kind == journal.Completed || records[len(records)-1].Kind == journal.Failed) {
		return w.persistAndNotify(ctx, typ, id, input.Sequence, records[len(records)-1].Payload, input.Header)
	}
	writeEntry := func(kind journal.Kind, payload json.RawMessage) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		renewCtx, stopRenew := context.WithTimeout(ctx, 3*time.Second)
		err := l.Renew(renewCtx)
		stopRenew()
		if err != nil {
			return err
		}
		seq, err := w.jrn.Append(ctx, typ, id, journal.Entry{Epoch: l.Epoch(), Index: uint64(len(records)), Kind: kind, Payload: payload, WorkerID: w.ID}, tail)
		if err != nil {
			return err
		}
		records = append(records, journal.Record{Entry: journal.Entry{Epoch: l.Epoch(), Index: uint64(len(records)), Kind: kind, Payload: payload, WorkerID: w.ID}, Sequence: seq})
		tail = seq
		return nil
	}
	appendEntry := func(kind journal.Kind, payload json.RawMessage) error {
		if uint64(len(records)) >= w.maxEntries {
			return journal.ErrTooLong
		}
		// A new request needs a completion and a terminal slot. Other
		// nonterminal entries also leave one slot for a durable failure.
		if kind != journal.Completed && kind != journal.Failed &&
			(uint64(len(records)) >= w.maxEntries-1 || kind == journal.StepRequested && uint64(len(records)) >= w.maxEntries-2) {
			outcome := wf.Outcome{InvSeq: input.Sequence, Error: journal.ErrTooLong.Error()}
			if kind == journal.StepRequested {
				outcome.LimitRequest = payload
			}
			failed, _ := json.Marshal(outcome)
			if err := writeEntry(journal.Failed, failed); err != nil {
				return err
			}
			return journal.ErrTooLong
		}
		return writeEntry(kind, payload)
	}
	if len(records) == 0 {
		if err := appendEntry(journal.Started, nil); err != nil {
			return err
		}
	}
	var attempts int
	var lastPanic string
	for _, record := range records {
		if record.Kind != journal.Attempt {
			continue
		}
		attempt, err := journal.DecodeAttempt(record.Payload)
		if err != nil || attempt.Count != attempts+1 {
			return wf.ErrCorruptJournal
		}
		attempts, lastPanic = attempt.Count, attempt.Error
	}
	failPanic := func(reason string) error {
		payload, err := json.Marshal(wf.Outcome{InvSeq: input.Sequence, Error: reason})
		if err != nil {
			return err
		}
		if err := appendEntry(journal.Failed, payload); err != nil {
			return err
		}
		return w.persistAndNotify(ctx, typ, id, input.Sequence, payload, input.Header)
	}
	// If the previous worker committed the final attempt but stopped before
	// appending Failed, finish from the journal without running user code again.
	if attempts >= w.maxPanicAttempts {
		return failPanic(lastPanic)
	}
	signals, err := w.drainSignals(ctx, typ, id, input.Sequence, records, appendEntry)
	if err != nil {
		return err
	}
	for _, signal := range signals {
		if signal.Name != client.CancelSignalName {
			continue
		}
		payload, err := json.Marshal(wf.Outcome{InvSeq: input.Sequence, Error: client.ErrCancelled.Error()})
		if err != nil {
			return err
		}
		if err := appendEntry(journal.Failed, payload); err != nil {
			return err
		}
		return w.persistAndNotify(ctx, typ, id, input.Sequence, payload, input.Header)
	}
	handler := w.Handlers[typ]
	if handler == nil {
		return fmt.Errorf("no handler for type %q", typ)
	}
	steps := make([]wf.Entry, 0, len(records)-1)
	for _, r := range records[1:] {
		switch r.Kind {
		case journal.StepRequested, journal.StepCompleted:
			steps = append(steps, wf.Entry{Index: r.Index, Kind: wf.Kind(r.Kind), Payload: r.Payload})
		case journal.SignalConsumed, journal.Suspended, journal.Attempt:
		default:
			return wf.ErrCorruptJournal
		}
	}
	handlerCtx, cancelHandler := context.WithCancel(ctx)
	defer cancelHandler()
	wctx := wf.NewContext(handlerCtx, steps, func(ctx context.Context, k wf.Kind, p json.RawMessage) error { return appendEntry(journal.Kind(k), p) }, signals...)
	wctx.SetResultStore(func(ctx context.Context, data []byte) (string, error) {
		objects, err := w.js.ObjectStore(ctx, "WF_BLOB")
		if err != nil {
			return "", err
		}
		digest := sha256.Sum256(data)
		name := "step-result-" + hex.EncodeToString(digest[:])
		_, err = objects.PutBytes(ctx, name, data)
		return name, err
	}, func(ctx context.Context, name string) ([]byte, error) {
		objects, err := w.js.ObjectStore(ctx, "WF_BLOB")
		if err != nil {
			return nil, err
		}
		return objects.GetBytes(ctx, name)
	})
	wctx.SetTimerSupport(wakeupAt, func(ctx context.Context) (time.Time, error) { return w.serverNow(ctx) }, func(ctx context.Context, step uint64, fireAt time.Time) error {
		renewCtx, stopRenew := context.WithTimeout(ctx, 3*time.Second)
		err := l.Renew(renewCtx)
		stopRenew()
		if err != nil {
			return err
		}
		return w.scheduleTimer(ctx, typ, id, input.Sequence, step, fireAt)
	})
	wctx.SetTimerObserver(w.metrics.recordTimerFired)
	wctx.SetChildSupport(typ, id, input.Sequence, func(ctx context.Context, childType, childID string, childInput []byte, signalName string) error {
		_, err := w.client.StartChild(ctx, childType, childID, childInput, typ, id, input.Sequence, signalName)
		if errors.Is(err, client.ErrAlreadyStarted) {
			return nil
		}
		return err
	})
	var result json.RawMessage
	var runErr error
	var panicked bool
	stopCancelWatch := w.watchRunningCancellation(ctx, typ, id, input.Sequence, cancelHandler)
	func() {
		defer func() {
			if p := recover(); p != nil {
				panicked = true
				runErr = fmt.Errorf("workflow panic: %v", p)
			}
		}()
		result, runErr = handler(wctx, inputData)
	}()
	cancelSeen := stopCancelWatch()
	if cancelSeen {
		current, err := w.drainSignals(ctx, typ, id, input.Sequence, records, appendEntry)
		if err != nil {
			return err
		}
		found := false
		for _, signal := range current {
			if signal.Name == client.CancelSignalName {
				found = true
				break
			}
		}
		if !found {
			return journal.ErrUnknown
		}
		payload, err := json.Marshal(wf.Outcome{InvSeq: input.Sequence, Error: client.ErrCancelled.Error()})
		if err != nil {
			return err
		}
		if err := appendEntry(journal.Failed, payload); err != nil {
			return err
		}
		return w.persistAndNotify(ctx, typ, id, input.Sequence, payload, input.Header)
	}
	if panicked {
		reason := runErr.Error()
		if len(reason) > 4096 {
			reason = reason[:4096]
		}
		payload, _ := json.Marshal(journal.AttemptPayload{Count: attempts + 1, Error: reason})
		if err := appendEntry(journal.Attempt, payload); err != nil {
			return err
		}
		if attempts+1 >= w.maxPanicAttempts {
			return failPanic(reason)
		}
		return &panicRetryError{attempt: attempts + 1}
	}
	if records[len(records)-1].Kind == journal.Failed {
		return w.persistAndNotify(ctx, typ, id, input.Sequence, records[len(records)-1].Payload, input.Header)
	}
	if errors.Is(runErr, wf.ErrSuspended) || wctx.WaitingOn() != "" {
		waiting := wctx.WaitingOn()
		if waiting == "" {
			return wf.ErrCorruptJournal
		}
		payload, _ := json.Marshal(struct {
			WaitingOn string `json:"waiting_on"`
		}{waiting})
		if records[len(records)-1].Kind == journal.Suspended && bytes.Equal(records[len(records)-1].Payload, payload) {
			return nil
		}
		return appendEntry(journal.Suspended, payload)
	}
	if runErr == nil {
		runErr = wctx.CheckComplete()
	}
	if errors.Is(runErr, journal.ErrStale) || errors.Is(runErr, journal.ErrUnknown) || errors.Is(runErr, lease.ErrLost) || errors.Is(runErr, context.Canceled) || errors.Is(runErr, wf.ErrTimerSchedule) || errors.Is(runErr, wf.ErrChildStart) {
		return runErr
	}
	out := wf.Outcome{InvSeq: input.Sequence, Result: result}
	kind := journal.Completed
	if runErr != nil {
		kind = journal.Failed
		out = wf.Outcome{InvSeq: input.Sequence, Error: runErr.Error()}
	} else if len(result) > wf.MaxInlineTerminal {
		objects, err := w.js.ObjectStore(ctx, "WF_BLOB")
		if err != nil {
			return err
		}
		digest := sha256.Sum256(result)
		name := "terminal-result-" + hex.EncodeToString(digest[:])
		if _, err := objects.PutBytes(ctx, name, result); err != nil {
			return err
		}
		out.Result = nil
		out.ResultRef = name
		out.ResultHash = hex.EncodeToString(digest[:])
	}
	payload, err := json.Marshal(out)
	if err != nil {
		return err
	}
	if err := appendEntry(kind, payload); err != nil {
		return err
	}
	return w.persistAndNotify(ctx, typ, id, input.Sequence, payload, input.Header)
}

func (w *Worker) persistAndNotify(ctx context.Context, typ, id string, invSeq uint64, payload []byte, headers nats.Header) error {
	if err := w.persistOutcome(ctx, typ, id, invSeq, payload); err != nil {
		return err
	}
	parentType := headers.Get(client.ParentTypeHeader)
	if parentType == "" {
		return nil
	}
	parentID := headers.Get(client.ParentIDHeader)
	parentInvSeq, err := strconv.ParseUint(headers.Get(client.ParentInvSeqHeader), 10, 64)
	if err != nil || parentInvSeq == 0 {
		return fmt.Errorf("invalid parent invocation sequence")
	}
	signalName := headers.Get(client.ParentSignalHeader)
	digest := sha256.Sum256([]byte(identity.Key(typ, id) + ":" + strconv.FormatUint(invSeq, 10)))
	key := "child-" + hex.EncodeToString(digest[:])
	_, err = w.client.SignalToGeneration(ctx, parentType, parentID, signalName, payload, key, parentInvSeq)
	if errors.Is(err, client.ErrStaleGeneration) || errors.Is(err, client.ErrPurged) {
		return nil
	}
	return err
}

func (w *Worker) scheduleTimer(ctx context.Context, typ, id string, invSeq, step uint64, fireAt time.Time) error {
	attemptCtx, stopAttempt := context.WithTimeout(ctx, 3*time.Second)
	defer stopAttempt()
	messageID := "timer:" + identity.Key(typ, id) + ":" + strconv.FormatUint(invSeq, 10) + ":" + strconv.FormatUint(step, 10)
	if !w.nativeSchedules {
		payload, err := json.Marshal(struct {
			FireAt time.Time `json:"fire_at"`
		}{fireAt})
		if err != nil {
			return err
		}
		ack, err := w.js.Publish(attemptCtx, identity.TimerSubject(typ, id, invSeq, step), payload, jetstream.WithMsgID(messageID))
		if err == nil && !ack.Duplicate {
			w.metrics.timersScheduled.Add(1)
		}
		return err
	}
	target := identity.RunSubject(typ, id, provision.Partitions)
	source := fmt.Sprintf("wf.schedule.%s.%s.%d", typ, id, step)
	m := &nats.Msg{Subject: source, Data: []byte(identity.Key(typ, id)), Header: nats.Header{}}
	m.Header.Set(jetstream.ScheduleHeader, "@at "+fireAt.UTC().Format(time.RFC3339Nano))
	m.Header.Set(jetstream.ScheduleTargetHeader, target)
	m.Header.Set(identity.TimerInvSeqHeader, strconv.FormatUint(invSeq, 10))
	m.Header.Set(identity.TimerStepHeader, strconv.FormatUint(step, 10))
	ack, err := w.js.PublishMsg(attemptCtx, m, jetstream.WithMsgID(messageID))
	if err == nil && !ack.Duplicate {
		w.metrics.timersScheduled.Add(1)
	}
	return err
}

func timerWasCancelled(records []journal.Record, step uint64) bool {
	for i := 0; i+1 < len(records); i++ {
		if records[i].Kind != journal.StepRequested || records[i+1].Kind != journal.StepCompleted {
			continue
		}
		var request struct {
			Kind      string `json:"kind"`
			TimerStep uint64 `json:"timer_step"`
		}
		var completion struct {
			Cancelled bool `json:"cancelled"`
		}
		if json.Unmarshal(records[i].Payload, &request) == nil && request.Kind == "timer_cancel" && request.TimerStep == step &&
			json.Unmarshal(records[i+1].Payload, &completion) == nil && completion.Cancelled {
			return true
		}
	}
	return false
}

func (w *Worker) serverNow(ctx context.Context) (time.Time, error) {
	attemptCtx, stopAttempt := context.WithTimeout(ctx, 3*time.Second)
	defer stopAttempt()
	run, err := w.js.Stream(attemptCtx, "WF_RUN")
	if err != nil {
		return time.Time{}, err
	}
	info, err := run.Info(attemptCtx)
	if err != nil {
		return time.Time{}, err
	}
	if info.TimeStamp.IsZero() {
		return time.Time{}, fmt.Errorf("server did not provide stream timestamp")
	}
	return info.TimeStamp, nil
}

// Close releases the worker's live cancellation subscription after all
// RunPartition loops have stopped.
func (w *Worker) Close() error {
	w.cancelMu.Lock()
	subscription := w.cancelSubscription
	w.cancelSubscription = nil
	w.cancelMu.Unlock()
	if subscription == nil {
		return nil
	}
	return subscription.Unsubscribe()
}

func cancelKey(typ, id, generation string) string { return typ + "." + id + ":" + generation }

func (w *Worker) markRunningCancellation(key string, expected *cancelWaiter) {
	w.cancelMu.Lock()
	defer w.cancelMu.Unlock()
	waiter := w.cancelWaiters[key]
	if waiter == nil || expected != nil && waiter != expected || waiter.detected {
		return
	}
	waiter.detected = true
	waiter.cancel()
}

func (w *Worker) observeCancel(message *nats.Msg) {
	parts := strings.Split(message.Subject, ".")
	if len(parts) != 5 || parts[0] != "wf" || parts[1] != "sig" || parts[4] != client.CancelSignalName {
		return
	}
	generation := message.Header.Get("Wf-Inv-Seq")
	if generation == "" {
		return
	}
	w.markRunningCancellation(cancelKey(parts[2], parts[3], generation), nil)
}

// watchRunningCancellation observes the core signal notification and checks
// the durable stream at registration and after gaps. Journal writes remain on
// the execute goroutine after the handler has returned.
func (w *Worker) watchRunningCancellation(ctx context.Context, typ, id string, invSeq uint64, cancelHandler context.CancelFunc) func() bool {
	generation := strconv.FormatUint(invSeq, 10)
	key := cancelKey(typ, id, generation)
	waiter := &cancelWaiter{cancel: cancelHandler}
	w.cancelMu.Lock()
	w.cancelWaiters[key] = waiter
	w.cancelMu.Unlock()
	watchCtx, stop := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(15 * time.Second)
		defer ticker.Stop()
		subject := "wf.sig." + typ + "." + id + "." + client.CancelSignalName
		for watchCtx.Err() == nil {
			lookupCtx, finish := context.WithTimeout(watchCtx, 2*time.Second)
			message, err := w.cancelStream.GetLastMsgForSubject(lookupCtx, subject)
			finish()
			if err == nil && message.Header.Get("Wf-Inv-Seq") == generation {
				w.markRunningCancellation(key, waiter)
				return
			}
			select {
			case <-watchCtx.Done():
				return
			case <-ticker.C:
			}
		}
	}()
	return func() bool {
		w.cancelMu.Lock()
		if w.cancelWaiters[key] == waiter {
			delete(w.cancelWaiters, key)
		}
		detected := waiter.detected
		w.cancelMu.Unlock()
		stop()
		<-done
		return detected
	}
}

func (w *Worker) drainSignals(ctx context.Context, typ, id string, invSeq uint64, records []journal.Record, appendEntry func(journal.Kind, json.RawMessage) error) ([]wf.Signal, error) {
	var lastSeq uint64
	var signals []wf.Signal
	for _, r := range records {
		if r.Kind != journal.SignalConsumed {
			continue
		}
		var event signalRecord
		if err := json.Unmarshal(r.Payload, &event); err != nil || event.Sequence <= lastSeq {
			return nil, wf.ErrCorruptJournal
		}
		lastSeq = event.Sequence
		payload, err := w.signalPayload(ctx, event)
		if err != nil {
			return nil, err
		}
		signals = append(signals, wf.Signal{Sequence: event.Sequence, Name: event.Name, Payload: payload})
	}
	lookupCtx, stopLookup := context.WithTimeout(ctx, 5*time.Second)
	stream, err := w.js.Stream(lookupCtx, "WF_SIG")
	stopLookup()
	if err != nil {
		return nil, err
	}
	lookupCtx, stopLookup = context.WithTimeout(ctx, 5*time.Second)
	info, err := stream.Info(lookupCtx)
	stopLookup()
	if err != nil {
		return nil, err
	}
	prefix := "wf.sig." + typ + "." + id + "."
	for seq := lastSeq + 1; seq <= info.State.LastSeq && seq != 0; {
		lookupCtx, stopLookup = context.WithTimeout(ctx, 5*time.Second)
		m, err := stream.GetMsg(lookupCtx, seq, jetstream.WithGetMsgSubject(prefix+"*"))
		stopLookup()
		if errors.Is(err, jetstream.ErrMsgNotFound) {
			break
		}
		if err != nil {
			return nil, err
		}
		if m.Sequence > info.State.LastSeq {
			break
		}
		seq = m.Sequence + 1
		if generation := m.Header.Get("Wf-Inv-Seq"); generation != "" {
			storedSeq, err := strconv.ParseUint(generation, 10, 64)
			if err != nil {
				return nil, wf.ErrCorruptJournal
			}
			if storedSeq != invSeq {
				continue
			}
		}
		name := strings.TrimPrefix(m.Subject, prefix)
		if err := identity.ValidateToken(name); err != nil {
			return nil, err
		}
		event := signalRecord{Sequence: m.Sequence, Name: name, Payload: m.Data, Ref: m.Header.Get("Wf-Signal-Ref"), Hash: m.Header.Get("Wf-Input-SHA256")}
		if event.Ref != "" {
			event.Payload = nil
		}
		payload, err := w.signalPayload(ctx, event)
		if err != nil {
			return nil, err
		}
		encoded, err := json.Marshal(event)
		if err != nil {
			return nil, err
		}
		if err := appendEntry(journal.SignalConsumed, encoded); err != nil {
			return nil, err
		}
		signals = append(signals, wf.Signal{Sequence: event.Sequence, Name: name, Payload: payload})
		lastSeq = event.Sequence
	}
	return signals, nil
}

func (w *Worker) signalPayload(ctx context.Context, event signalRecord) ([]byte, error) {
	payload := event.Payload
	if event.Ref != "" {
		objects, err := w.js.ObjectStore(ctx, "WF_BLOB")
		if err != nil {
			return nil, err
		}
		payload, err = objects.GetBytes(ctx, event.Ref)
		if err != nil {
			return nil, err
		}
	}
	if event.Hash != "" {
		digest := sha256.Sum256(payload)
		if hex.EncodeToString(digest[:]) != event.Hash {
			return nil, fmt.Errorf("signal payload hash mismatch")
		}
	}
	return payload, nil
}

func (w *Worker) persistOutcome(ctx context.Context, typ, id string, invSeq uint64, payload []byte) error {
	attemptCtx, stopAttempt := context.WithTimeout(ctx, 5*time.Second)
	defer stopAttempt()
	key := identity.Key(typ, id)
	_, err := w.state.Create(attemptCtx, key, payload)
	if err == nil {
		return nil
	}
	if !errors.Is(err, jetstream.ErrKeyExists) {
		return err
	}
	previous, err := w.state.Get(attemptCtx, key)
	if err != nil {
		return err
	}
	marker, tomb, err := retention.Decode(previous.Value())
	if err != nil {
		return err
	}
	if tomb {
		if marker.InvSeq >= invSeq {
			return client.ErrPurged
		}
		_, err := w.state.Update(attemptCtx, key, payload, previous.Revision())
		if err == nil {
			return nil
		}
		current, getErr := w.state.Get(attemptCtx, key)
		if getErr == nil && bytes.Equal(current.Value(), payload) {
			return nil
		}
		return err
	}
	if !bytes.Equal(previous.Value(), payload) {
		return fmt.Errorf("terminal result changed for %s", key)
	}
	return nil
}
