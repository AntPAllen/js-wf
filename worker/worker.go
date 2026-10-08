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
	"js-wf/internal/graphpublication"
	"js-wf/journal"
	"js-wf/lease"
	"js-wf/provision"
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

type canonicalSignalRecord struct {
	Index uint64 `json:"index"`
	Token string `json:"token"`
}

type signalRecord struct {
	Sequence  uint64                 `json:"sig_seq"`
	Name      string                 `json:"name"`
	Payload   []byte                 `json:"payload,omitempty"`
	Ref       string                 `json:"ref,omitempty"`
	Hash      string                 `json:"hash,omitempty"`
	Child     *graphChildResult      `json:"graph_child,omitempty"`
	Canonical *canonicalSignalRecord `json:"canonical_signal,omitempty"`
}

type cancelWaiter struct {
	cancel   context.CancelFunc
	detected bool
}

type Worker struct {
	fencingObserver            func(FencingEvent)
	js                         jetstream.JetStream
	jrn                        *journal.Store
	graphJournal               *journal.GraphStore
	legacyJournalOption        bool
	leases                     *lease.Store
	state                      jetstream.KeyValue
	outcomePort                OutcomePort
	invocationPort             InvocationPort
	signalDrainPort            SignalDrainPort
	resultBlobPort             ResultBlobPort
	timerSchedulePort          TimerSchedulePort
	timerNowPort               func(context.Context) (time.Time, error)
	timerClockDomain           string
	timerClockBounds           func(context.Context) (time.Time, time.Time, error)
	client                     *client.Client
	ID                         string
	Handlers                   map[string]Handler
	continuations              map[string]map[string]ContinuationHandler
	maxEntries                 uint64
	maxPanicAttempts           int
	partitionConcurrency       int
	ackWait                    time.Duration
	heartbeatInterval          time.Duration
	heartbeatTicks             <-chan time.Time
	nativeSchedules            bool
	metrics                    metricsCounters
	dispatchObserver           func(DispatchEvent)
	operationObserver          func(OperationEvent)
	operationNow               func() time.Time
	cancelMu                   sync.Mutex
	cancelWaiters              map[string]*cancelWaiter
	cancelStream               jetstream.Stream
	cancelSubscription         *nats.Subscription
	cancelPollPort             CancellationPollPort
	modeledCancelNotifications bool
	terminalHints              terminalHints
}

// DefaultAckWait allows a killed owner's lease to expire before redelivery,
// with one second for initialization/transport skew. Two missed deliveries
// leave processing time inside the 30-second recovery gate.
const DefaultAckWait = provision.LeaseTTL + time.Second

const defaultHeartbeatInterval = 3 * time.Second

// A run that loses a lease race is still needed if the owner stops. Delay its
// redelivery long enough to avoid a storm when many signal or timer wakeups
// arrive for one healthy, long-running invocation.
const heldLeaseNakDelay = 5 * time.Second

// CancellationPollPort reads the latest durable cancel signal for one subject.
type CancellationPollPort interface {
	LastGeneration(context.Context, string) (string, error)
}

type jetStreamCancellationPollPort struct{ stream jetstream.Stream }

func (p jetStreamCancellationPollPort) LastGeneration(ctx context.Context, subject string) (string, error) {
	message, err := p.stream.GetLastMsgForSubject(ctx, subject)
	if err != nil {
		return "", err
	}
	return message.Header.Get("Wf-Inv-Seq"), nil
}

type Option func(*Worker) error

// DispatchEvent reports one durable delivery's progress through the worker.
// The observer runs on partition goroutines and must be concurrency-safe and
// quick enough not to delay dispatch.
type DispatchEvent struct {
	At          time.Time
	Worker      string
	Type        string
	ID          string
	Stage       string
	RunSequence uint64
	Delivery    uint64
	Error       string
	HeldLease   *lease.HeldObservation `json:"HeldLease,omitempty"`
}

// WithJournalEncoding selects new writes. Upgrade every reader before enabling
// protobuf; legacy JSON and protobuf entries can coexist in one invocation.
func WithJournalEncoding(encoding journal.Encoding) Option {
	return func(w *Worker) error {
		store, err := journal.NewWithEncoding(w.js, encoding)
		if err != nil {
			return err
		}
		w.jrn = store
		w.legacyJournalOption = true
		return nil
	}
}

// WithDispatchObserver supplies optional per-delivery diagnostics.
func WithDispatchObserver(observe func(DispatchEvent)) Option {
	return func(w *Worker) error {
		w.dispatchObserver = observe
		return nil
	}
}

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

// WithHeartbeatTicks supplies ticks for deterministic worker tests. Closing
// the channel makes an active worker stop its effect and hand off the message.
func WithHeartbeatTicks(ticks <-chan time.Time) Option {
	return func(w *Worker) error {
		if ticks == nil {
			return fmt.Errorf("heartbeat tick channel is nil")
		}
		w.heartbeatTicks = ticks
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
	w := &Worker{js: js, jrn: journal.New(js), leases: l, state: state, client: client.New(js), ID: id, Handlers: handlers, maxEntries: journal.MaxEntries, maxPanicAttempts: 3, partitionConcurrency: 1, ackWait: DefaultAckWait, heartbeatInterval: defaultHeartbeatInterval, nativeSchedules: runInfo.Config.AllowMsgSchedules, cancelWaiters: make(map[string]*cancelWaiter)}
	for _, option := range options {
		if err := option(w); err != nil {
			return nil, err
		}
	}
	if err := w.validateGraphOptions(); err != nil {
		return nil, err
	}
	w.cancelStream, err = js.Stream(attemptCtx, "WF_SIG")
	if err != nil {
		return nil, fmt.Errorf("signal stream: %w", err)
	}
	w.cancelPollPort = jetStreamCancellationPollPort{stream: w.cancelStream}
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

// ModeledWorkerPorts supplies the durable boundaries needed to execute
// workflow handlers through the production Worker delivery path.
type ModeledWorkerPorts struct {
	Journal     *journal.Store
	Leases      *lease.Store
	Outcome     OutcomePort
	Invocation  InvocationPort
	Signals     SignalDrainPort
	ResultBlobs ResultBlobPort
	Timer       TimerSchedulePort
	TimerNow    func(context.Context) (time.Time, error)
	NativeTimer bool
	Client      *client.Client
	// Enable waiter registration for modeled core notifications. The caller
	// delivers committed notifications through ObserveCancellationNotification.
	CancellationNotifications bool
	CancellationPoll          CancellationPollPort
	// HeartbeatTicks replaces the wall-clock ticker in modeled workers.
	HeartbeatTicks <-chan time.Time
	// Optional diagnostics use virtual time in modeled workers. These callbacks
	// must not advance the schedule or make durable transport calls.
	OperationObserver func(OperationEvent)
	OperationNow      func() time.Time
	// JournalEntryLimit lowers the global entry budget for bounded modeled
	// boundary schedules. Zero uses journal.MaxEntries; values outside 4..MaxEntries
	// are invalid. The JetStream-facing constructor keeps the production cap.
	JournalEntryLimit uint64
}

// NewWithPorts builds a worker whose delivery and execution decisions run
// against narrow transports. Snapshot objects are not provided by this
// constructor; timer scheduling is available when Timer and TimerNow are set.
// Modeled cancellation notifications do not include the durable signal poll.
func NewWithPorts(id string, handlers map[string]Handler, ports ModeledWorkerPorts, options ...Option) (*Worker, error) {
	if id == "" || ports.Journal == nil || ports.Leases == nil || ports.Outcome == nil || ports.Invocation == nil || ports.Signals == nil || ports.Client == nil {
		return nil, fmt.Errorf("invalid modeled worker configuration")
	}
	limit := ports.JournalEntryLimit
	if limit == 0 {
		limit = journal.MaxEntries
	}
	if limit < 4 || limit > journal.MaxEntries {
		return nil, fmt.Errorf("invalid modeled journal entry limit %d", limit)
	}
	w := &Worker{jrn: ports.Journal, leases: ports.Leases, outcomePort: ports.Outcome, invocationPort: ports.Invocation, signalDrainPort: ports.Signals, resultBlobPort: ports.ResultBlobs, timerSchedulePort: ports.Timer, timerNowPort: ports.TimerNow, nativeSchedules: ports.NativeTimer, client: ports.Client, ID: id, Handlers: handlers, maxEntries: journal.MaxEntries, maxPanicAttempts: 3, partitionConcurrency: 1, ackWait: DefaultAckWait, heartbeatInterval: defaultHeartbeatInterval, heartbeatTicks: ports.HeartbeatTicks, operationObserver: ports.OperationObserver, operationNow: ports.OperationNow, cancelWaiters: make(map[string]*cancelWaiter), modeledCancelNotifications: ports.CancellationNotifications, cancelPollPort: ports.CancellationPoll}
	w.maxEntries = limit
	for _, option := range options {
		if err := option(w); err != nil {
			return nil, err
		}
	}
	if err := w.validateGraphOptions(); err != nil {
		return nil, err
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
	return RunPartitionWithPort(ctx, partition, jetStreamDispatchPort{worker: w}, w.handle, w.partitionConcurrency)
}

// RunPartitionWithTransport executes the same worker handler over a supplied
// durable consumer, including lease, journal, and result decisions.
func (w *Worker) RunPartitionWithTransport(ctx context.Context, partition uint32, port DispatchPort) error {
	return RunPartitionWithPort(ctx, partition, port, w.handle, w.partitionConcurrency)
}

// RunPartitionWithPort runs the production dispatch loop over a supplied
// consumer port and message handler. It is the Tier 1 dispatch simulation seam.
func RunPartitionWithPort(ctx context.Context, partition uint32, port DispatchPort, handle func(context.Context, jetstream.Msg), concurrency int) error {
	if partition >= provision.Partitions || port == nil || handle == nil || concurrency < 1 || concurrency > 256 {
		return fmt.Errorf("invalid dispatch configuration")
	}
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
		if err := port.Wait(ctx, delay); err != nil {
			return false
		}
		return true
	}
	if concurrency > 1 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithCancel(ctx)
		slots = make(chan struct{}, concurrency)
		defer func() {
			cancel()
			active.Wait()
		}()
	}
	for ctx.Err() == nil {
		c, err := port.Consumer(ctx, partition)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
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
			pending, ackPending, err := c.Info(attempt)
			stop()
			active := 0
			if slots != nil {
				active = len(slots)
			}
			return err != nil || pending > 0 || ackPending > active
		}
		for ctx.Err() == nil {
			if slots != nil {
				select {
				case slots <- struct{}{}:
				case <-ctx.Done():
					return nil
				}
			}
			batch, err := c.FetchOne(ctx)
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
			messages := batch.Messages()
			for messages != nil {
				if ctx.Err() != nil {
					if slots != nil && !dispatched {
						<-slots
					}
					return nil
				}
				var msg jetstream.Msg
				select {
				case <-ctx.Done():
					if slots != nil && !dispatched {
						<-slots
					}
					return nil
				case next, open := <-messages:
					if !open {
						messages = nil
						continue
					}
					msg = next
				}
				dispatched = true
				emptyPolls = 0
				retryDelay = 100 * time.Millisecond
				if slots == nil {
					handle(ctx, msg)
					continue
				}
				active.Add(1)
				go func(msg jetstream.Msg) {
					defer active.Done()
					defer func() { <-slots }()
					handle(ctx, msg)
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
	var heldObservation *lease.HeldObservation
	emit := func(stage string, eventErr error) {
		if w.dispatchObserver == nil {
			return
		}
		event := DispatchEvent{At: time.Now(), Worker: w.ID, Type: typ, ID: id, Stage: stage, RunSequence: metadata.Sequence.Stream, Delivery: metadata.NumDelivered}
		if eventErr != nil {
			event.Error = eventErr.Error()
		}
		if stage == "lease_held" {
			event.HeldLease = heldObservation
		}
		w.dispatchObserver(event)
	}
	ops := w.deliveryOperations(typ, id, metadata.Sequence.Stream, metadata.NumDelivered)
	emit("fetched", nil)
	w.metrics.recordRedelivery(metadata)
	acquireStarted := ops.begin()
	acquireCtx, stopAcquire := context.WithTimeout(ctx, 5*time.Second)
	var observeHeld func(lease.HeldObservation)
	if w.dispatchObserver != nil {
		observeHeld = func(o lease.HeldObservation) { heldObservation = &o }
	}
	l, err := w.leases.AcquireWithHeldObserver(acquireCtx, typ, id, w.ID, observeHeld)
	stopAcquire()
	ops.finish(acquireStarted, "lease_acquire", 0, "", err)
	if errors.Is(err, lease.ErrHeld) {
		emit("lease_held", err)
		w.metrics.leaseContentions.Add(1)
		terminal, canceledTimer, terminalErr := w.terminalHeldDelivery(ctx, typ, id, timer, ops)
		if terminal {
			emit("terminal_held", nil)
			ackErr := w.acknowledgeDispatch(parent, msg, ops)
			emit("ack", ackErr)
			if ackErr == nil && canceledTimer {
				w.metrics.cancelledTimerNoOps.Add(1)
			}
			return
		}
		if terminalErr != nil {
			emit("terminal_probe_error", terminalErr)
		}
		emit("nak", msg.NakWithDelay(heldLeaseNakDelay))
		return
	}
	if err != nil {
		emit("lease_error", err)
		w.metrics.leaseAcquireFailures.Add(1)
		if errors.Is(err, lease.ErrLost) {
			w.recordFencing(FencingEvent{Type: typ, ID: id, RunSequence: metadata.Sequence.Stream, Delivery: metadata.NumDelivered, Reason: "lease_initialization_lost"}, err)
		}
		_ = msg.NakWithDelay(time.Second)
		return
	}
	emit("lease_acquired", nil)
	w.metrics.leaseAcquisitions.Add(1)
	w.metrics.recordLeaseLatency(metadata, time.Now())
	// A failed Release still proves loss when Cleanup later resolves safely.
	// Execution, explicit release and deferred cleanup share this delivery's
	// observation so the same lost ownership is counted only once.
	fencingRecorded := false
	recordFencing := func(reason string, cause error) {
		if fencingRecorded {
			return
		}
		fencingRecorded = true
		w.recordFencing(FencingEvent{Type: typ, ID: id, RunSequence: metadata.Sequence.Stream, Delivery: metadata.NumDelivered, Epoch: l.Epoch(), Reason: reason}, cause)
	}
	release := func() error {
		releaseCtx, stopRelease := context.WithTimeout(context.Background(), 15*time.Second)
		defer stopRelease()
		attempt, stopAttempt := context.WithTimeout(releaseCtx, 2*time.Second)
		started := ops.begin()
		err := l.Release(attempt)
		stopAttempt()
		ops.finish(started, "lease_release", 0, "", err)
		if err == nil {
			emit("released", nil)
			return nil
		}
		emit("release_initial_error", err)
		if errors.Is(err, lease.ErrLost) {
			recordFencing("lease_release_lost", err)
		}
		for releaseCtx.Err() == nil {
			attempt, stopAttempt := context.WithTimeout(releaseCtx, 2*time.Second)
			started := ops.begin()
			cleanupErr := l.Cleanup(attempt)
			stopAttempt()
			ops.finish(started, "lease_cleanup", 0, "", cleanupErr)
			if errors.Is(cleanupErr, lease.ErrLost) {
				recordFencing("lease_cleanup_lost", cleanupErr)
			}
			if cleanupErr == nil || errors.Is(cleanupErr, lease.ErrLost) {
				emit("release_cleanup_done", cleanupErr)
				return cleanupErr
			}
			emit("release_cleanup_error", cleanupErr)
			select {
			case <-releaseCtx.Done():
			case <-time.After(200 * time.Millisecond):
			}
		}
		cleanupErr := fmt.Errorf("lease cleanup after %v: %w", err, releaseCtx.Err())
		emit("release_cleanup_timeout", cleanupErr)
		return cleanupErr
	}
	released := false
	defer func() {
		if !released {
			_ = release()
		}
	}()
	var leaseLost atomic.Bool
	var heartbeatLostErr error
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		ticks := w.heartbeatTicks
		if ticks == nil {
			ticker := time.NewTicker(w.heartbeatInterval)
			defer ticker.Stop()
			ticks = ticker.C
		}
		for {
			select {
			case <-ctx.Done():
				return
			case _, open := <-ticks:
				if !open {
					cancel()
					return
				}
				started := ops.begin()
				renewCtx, stopRenew := context.WithTimeout(ctx, 3*time.Second)
				// Acquisition and append renewals already acknowledge ownership.
				// Apply the same request-start freshness bound to every heartbeat;
				// each journal append still renews unconditionally.
				renewed, timing, err := ops.renew(renewCtx, l, w.heartbeatInterval)
				stopRenew()
				operation := "lease_renew_heartbeat"
				if err == nil && !renewed {
					operation = "lease_heartbeat_recent"
				}
				ops.finish(started, operation, 0, "", err, timing)
				if err != nil {
					if errors.Is(err, lease.ErrLost) {
						heartbeatLostErr = err
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
	terminalDuplicate := false
	if w.terminalHints.contains(identity.Key(typ, id)) {
		var probeErr error
		terminalDuplicate, cancelledTimerNoOp, probeErr = w.terminalHeldDelivery(ctx, typ, id, timer, ops)
		if probeErr != nil {
			emit("terminal_probe_error", probeErr)
		}
	}
	if !terminalDuplicate {
		err = w.execute(ctx, typ, id, l, metadata.Timestamp, timer, &cancelledTimerNoOp, ops)
	}
	if err == nil && ctx.Err() == nil && !terminalDuplicate && w.graphJournal == nil && w.jrn.HasSnapshotTransport() && w.continuations[typ] == nil {
		// Missing snapshot metadata/object replies must not keep a suspended
		// invocation owned for its entire workflow lifetime. Publication and
		// purge already support uncertain replies; retry them on redelivery.
		snapshotCtx, stopSnapshot := context.WithTimeout(ctx, 15*time.Second)
		started := ops.begin()
		err = w.jrn.MaybeSnapshot(snapshotCtx, typ, id, 256, 16)
		stopSnapshot()
		ops.finish(started, "journal_snapshot", 0, "", err)
		if err == nil && ctx.Err() == nil {
			w.terminalHints.snapshotReady(identity.Key(typ, id))
		}
	}
	processingCanceled := ctx.Err() != nil
	cancel()
	<-stopped
	if leaseLost.Load() || errors.Is(err, lease.ErrLost) || errors.Is(err, journal.ErrStale) {
		reason, cause := "lease_execution_lost", err
		if leaseLost.Load() {
			reason, cause = "lease_heartbeat_lost", heartbeatLostErr
		} else if errors.Is(err, journal.ErrStale) {
			reason = "journal_stale"
		}
		recordFencing(reason, cause)
	}
	if err != nil || processingCanceled || leaseLost.Load() {
		retryErr := err
		if retryErr == nil {
			if leaseLost.Load() {
				retryErr = lease.ErrLost
			} else {
				retryErr = ctx.Err()
			}
		}
		emit("execution_retry", retryErr)
		delay := time.Second
		var retry *panicRetryError
		if errors.As(err, &retry) {
			delay = retry.Delay()
		}
		// A NAK sent while the consumer has no quorum can disappear even when
		// the local connection accepts it. A live worker whose heartbeat stopped
		// also enqueues a durable, deduplicated handoff after the route heals.
		// The original delivery remains safe under the invocation lease.
		if processingCanceled && parent.Err() == nil {
			if cleanupErr := release(); cleanupErr == nil || errors.Is(cleanupErr, lease.ErrLost) {
				released = true
				w.enqueueCanceledHandoff(parent, typ, id, metadata.Sequence.Stream)
			}
		}
		emit("nak", msg.NakWithDelay(delay))
		return
	}
	if err := release(); err != nil {
		emit("release_error", err)
		emit("nak", msg.NakWithDelay(time.Second))
		return
	}
	released = true
	ackErr := w.acknowledgeDispatch(parent, msg, ops)
	emit("ack", ackErr)
	if ackErr == nil && cancelledTimerNoOp {
		w.metrics.cancelledTimerNoOps.Add(1)
	}
}

// A successful local ACK publish does not establish the consumer's outcome.
// Bound confirmed acknowledgement independently of the cancelled handler context.
// An ambiguous error leaves durable state intact for safe redelivery; this does
// not imply physical WorkQueue deletion has completed.
func (w *Worker) acknowledgeDispatch(parent context.Context, msg jetstream.Msg, ops *deliveryOperations) error {
	started := ops.begin()
	ctx, stop := context.WithTimeout(parent, 2*time.Second)
	defer stop()
	err := msg.DoubleAck(ctx)
	ops.finish(started, "dispatch_ack_confirmed", 0, "", err)
	return err
}

func (w *Worker) enqueueCanceledHandoff(parent context.Context, typ, id string, runSequence uint64) {
	if w.client == nil || runSequence == 0 {
		return
	}
	ctx, cancel := context.WithTimeout(parent, 20*time.Second)
	defer cancel()
	messageID := fmt.Sprintf("worker-handoff:%d", runSequence)
	for ctx.Err() == nil {
		attempt, stop := context.WithTimeout(ctx, 2*time.Second)
		err := w.client.Enqueue(attempt, typ, id, messageID)
		stop()
		if err == nil {
			w.metrics.handoffEnqueues.Add(1)
			return
		}
		select {
		case <-ctx.Done():
		case <-time.After(100 * time.Millisecond):
		}
	}
}

func (w *Worker) execute(ctx context.Context, typ, id string, l *lease.Lease, wakeupAt time.Time, timer timerWakeup, cancelledTimerNoOp *bool, ops *deliveryOperations) error {
	var records []journal.Record
	var tail, baseIndex uint64
	var resumed *journal.CheckpointRead
	var checkpointInfo wf.CheckpointInfo
	if w.graphJournal == nil && w.continuations[typ] == nil {
		started := ops.begin()
		readCtx, stop := context.WithTimeout(ctx, 15*time.Second)
		var err error
		records, tail, err = w.jrn.Read(readCtx, typ, id)
		stop()
		ops.finish(started, "journal_read", 0, "", err)
		if err != nil {
			return err
		}
	}
	invocationPort := w.invocationPort
	if invocationPort == nil {
		invocationPort = jetStreamInvocationPort{js: w.js}
	}
	lookupStarted := ops.begin()
	lookupCtx, stopLookup := context.WithTimeout(ctx, 5*time.Second)
	input, err := invocationPort.LastInvocation(lookupCtx, identity.InvocationSubject(typ, id))
	stopLookup()
	ops.finish(lookupStarted, "invocation_read", 0, "", err)
	if errors.Is(err, jetstream.ErrMsgNotFound) {
		return nil
	} // Purged invocation: wakeup is a no-op.
	if err != nil {
		return err
	}
	if input == nil || input.Sequence == 0 {
		return wf.ErrCorruptJournal
	}
	if input.Header.Get(journal.GraphStartTokenHeader) != "" && (w.graphJournal == nil || !w.graphJournal.CanonicalStarts()) {
		return wf.ErrCorruptJournal
	}
	var graph *graphDelivery
	if w.graphJournal != nil {
		if !validGraphHash(input.Header.Get("Wf-Input-SHA256")) {
			return wf.ErrCorruptJournal
		}
		readCtx, stopRead := context.WithTimeout(ctx, 15*time.Second)
		graph, err = openGraphDelivery(readCtx, w.graphJournal, typ, id, input.Sequence)
		stopRead()
		if err != nil {
			return err
		}
		defer func() {
			cleanup, stop := context.WithTimeout(context.WithoutCancel(ctx), 3*time.Second)
			defer stop()
			_ = graph.close(cleanup)
		}()
		graph.invocationPort = invocationPort
		records, tail = graph.records, graph.view.Tail()
	}
	if w.continuations[typ] != nil {
		readStarted := ops.begin()
		readCtx, stopRead := context.WithTimeout(ctx, 15*time.Second)
		resumed, err = w.jrn.ReadCheckpoint(readCtx, typ, id, input.Sequence)
		if err == nil && resumed != nil {
			r := resumed.Snapshot.Runtime
			_, checkpointInfo, err = wf.NewCheckpointContext(ctx, nil, nil, resumed.Frame, wf.CheckpointLocation{Type: typ, ID: id, InvSeq: input.Sequence, Index: r.Index, Epoch: r.Epoch, Hash: r.SHA256})
			if err == nil && w.continuations[typ][checkpointInfo.Stage] == nil {
				err = wf.ErrUnknownContinuation
			}
			records = append([]journal.Record{resumed.Anchor}, resumed.Records...)
			tail, baseIndex = resumed.Tail, resumed.Anchor.Index
		}
		if err == nil && resumed == nil {
			records, tail, err = w.jrn.Read(readCtx, typ, id)
		}
		stopRead()
		ops.finish(readStarted, "journal_read", 0, "", err)
		if err != nil {
			return err
		}
	}
	if resumed != nil && records[len(records)-1].Index >= w.maxEntries {
		return journal.ErrTooLong
	}
	if w.continuations[typ] == nil {
		for _, record := range records {
			if record.Kind != journal.StepRequested {
				continue
			}
			var declaration struct {
				Kind string `json:"kind"`
			}
			if json.Unmarshal(record.Payload, &declaration) == nil && declaration.Kind == "checkpoint" {
				return wf.ErrContinuationUnsupported
			}
		}
	}
	nextIndex := func() uint64 { return baseIndex + uint64(len(records)) }
	if timer.scheduled && timer.generation != input.Sequence {
		return nil
	}
	if timer.scheduled && (timerWasCancelled(records, timer.step) || containsTimer(checkpointInfo.CancelledTimers, timer.step)) {
		*cancelledTimerNoOp = true
	}
	inputData := input.Data
	if graph != nil && w.graphJournal.CanonicalStarts() {
		if err = graph.view.ValidateStartInvocation(ctx, input); err != nil {
			return err
		}
		inputData, err = graph.view.StartInput(ctx)
		if err != nil {
			return err
		}
	} else if graph != nil && len(records) > 0 {
		inputData, err = graph.GetBytes(ctx, "input:"+input.Header.Get("Wf-Input-SHA256"))
		if err != nil {
			return err
		}
	} else if key := input.Header.Get("Wf-Input-Ref"); key != "" {
		inputData, err = invocationPort.InputBlob(ctx, key)
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
	// A canceled timer can still repair a terminal outcome above. For an
	// active invocation, acknowledge it without reentering the handler.
	if *cancelledTimerNoOp {
		return nil
	}
	if graph != nil {
		graph.input = bytes.Clone(inputData)
	}
	writeEntry := func(kind journal.Kind, payload json.RawMessage) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		started := ops.begin()
		renewCtx, stopRenew := context.WithTimeout(ctx, 3*time.Second)
		_, timing, err := ops.renew(renewCtx, l, 0)
		stopRenew()
		ops.finish(started, "lease_renew_append", nextIndex(), kind, err, timing)
		if err != nil {
			return err
		}
		started = ops.begin()
		entry := journal.Entry{Epoch: l.Epoch(), Index: nextIndex(), Kind: kind, Payload: payload, WorkerID: w.ID}
		var seq uint64
		if graph != nil {
			if kind == journal.Started {
				entry.Payload, _ = json.Marshal(struct {
					InputSHA256 string `json:"input_sha256"`
				}{graphHash(inputData)})
				payload = entry.Payload
			}
			appendCtx, stopAppend := context.WithTimeout(ctx, 15*time.Second)
			for attempt := 0; attempt < 16; attempt++ {
				if attempt > 0 {
					// A definite graph CAS loss can be caused by reader pins,
					// without advancing the journal. Retain this entry's result
					// and recheck the lease before preparing a fresh append.
					// GraphStore revalidates generation, tail/index and epoch;
					// unknown publication and revoked grants are not retried.
					renewStarted := ops.begin()
					retryCtx, stopRetry := context.WithTimeout(appendCtx, 3*time.Second)
					_, retryTiming, retryErr := ops.renew(retryCtx, l, 0)
					stopRetry()
					ops.finish(renewStarted, "lease_renew_append_retry", nextIndex(), kind, retryErr, retryTiming)
					if retryErr != nil {
						err = retryErr
						break
					}
				}
				seq, err = graph.append(appendCtx, entry, tail)
				if !errors.Is(err, graphpublication.ErrConflict) || errors.Is(err, journal.ErrUnknown) || errors.Is(err, graphpublication.ErrRevoked) || appendCtx.Err() != nil {
					break
				}
			}
			stopAppend()
		} else {
			seq, err = w.jrn.Append(ctx, typ, id, entry, tail)
		}
		ops.finish(started, "journal_append", nextIndex(), kind, err)
		if err != nil {
			return err
		}
		if graph != nil {
			records = graph.records
		} else {
			records = append(records, journal.Record{Entry: journal.Entry{Epoch: l.Epoch(), Index: nextIndex(), Kind: kind, Payload: payload, WorkerID: w.ID}, Sequence: seq})
		}
		tail = seq
		return nil
	}
	appendEntry := func(kind journal.Kind, payload json.RawMessage) error {
		if nextIndex() >= w.maxEntries {
			return journal.ErrTooLong
		}
		// A new request needs a completion and a terminal slot. Other
		// nonterminal entries also leave one slot for a durable failure.
		if kind != journal.Completed && kind != journal.Failed &&
			(nextIndex() >= w.maxEntries-1 || kind == journal.StepRequested && nextIndex() >= w.maxEntries-2) {
			outcome := wf.Outcome{InvSeq: input.Sequence, Error: journal.ErrTooLong.Error()}
			if kind == journal.StepRequested {
				outcome.LimitRequest = payload
			} else if kind == journal.Attempt || kind == journal.Suspended || kind == journal.SignalConsumed {
				outcome.LimitEntry = &wf.LimitEntry{Kind: string(kind), Payload: payload}
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
	attempts := int(checkpointInfo.PanicAttempts)
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
		if lastPanic == "" {
			lastPanic = "workflow panic attempts exhausted"
		}
		return failPanic(lastPanic)
	}
	var signals []wf.Signal
	if graph == nil {
		signals, err = w.drainSignalsFrom(ctx, typ, id, input.Sequence, checkpointInfo.SignalCursor, records, appendEntry, ops)
	} else if graph.store.CanonicalSignals() {
		signals, err = w.drainCanonicalSignals(ctx, graph, checkpointInfo.SignalCursor, records, appendEntry, ops)
	} else {
		port := w.signalDrainPort
		if port == nil {
			port = NewSignalDrainPort(w.js)
		}
		port = graphSignalDrain{SignalDrainPort: port, graph: graph}
		if ops != nil {
			port = observedSignalDrain{SignalDrainPort: port, operations: ops}
		}
		signals, err = drainSignalsFromPort(ctx, port, typ, id, input.Sequence, checkpointInfo.SignalCursor, records, appendEntry)
	}
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
	// Diagnostic mapping from SDK logical timer steps to durable journal indices.
	// Suspensions/attempts do not occupy SDK step positions; checkpoints retain
	// their logical offset. This mapping does not alter timer message identities.
	stepOffset := uint64(0)
	traceNativeHints := ops != nil && w.nativeSchedules && w.timerClockBounds != nil
	var requestIndices map[uint64]uint64
	if traceNativeHints {
		requestIndices = make(map[uint64]uint64)
	}
	sdkNext := uint64(len(steps))
	appender := func(ctx context.Context, k wf.Kind, p json.RawMessage) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		index := nextIndex()
		if err := appendEntry(journal.Kind(k), p); err != nil {
			return err
		}
		if traceNativeHints {
			if k == wf.StepRequested {
				requestIndices[stepOffset+sdkNext] = index
			}
			if k == wf.StepRequested || k == wf.StepCompleted {
				sdkNext++
			}
		}
		return nil
	}
	wctx := wf.NewContext(handlerCtx, steps, appender, signals...)
	if resumed != nil {
		r := resumed.Snapshot.Runtime
		wctx, checkpointInfo, err = wf.NewCheckpointContext(handlerCtx, steps, appender, resumed.Frame, wf.CheckpointLocation{Type: typ, ID: id, InvSeq: input.Sequence, Index: r.Index, Epoch: r.Epoch, Hash: r.SHA256}, signals...)
		if err != nil {
			return err
		}
	}
	stepOffset = checkpointInfo.StepPosition
	for i, entry := range steps {
		if traceNativeHints && entry.Kind == wf.StepRequested {
			requestIndices[stepOffset+uint64(i)] = entry.Index
		}
	}
	if stages := w.continuations[typ]; stages != nil {
		wctx.SetContinuationSupport(func(stage string) bool { return stages[stage] != nil }, func(completed uint64, recorded bool) (wf.ContinuationAnchor, error) {
			return continuationAnchor(records, nextIndex(), l.Epoch(), checkpointInfo, completed, recorded)
		})
	}
	resultBlobs := w.resultBlobs()
	if graph != nil {
		resultBlobs = graph
	}
	if resultBlobs != nil {
		resultBlobs = boundedResultBlobPort{ResultBlobPort: resultBlobs, operations: ops}
	}
	wctx.SetResultStore(func(ctx context.Context, data []byte) (string, error) {
		if resultBlobs == nil {
			return "", fmt.Errorf("result blob store unavailable on modeled worker")
		}
		digest := sha256.Sum256(data)
		name := "step-result-" + hex.EncodeToString(digest[:])
		return name, resultBlobs.PutBytes(ctx, name, data)
	}, func(ctx context.Context, name string) ([]byte, error) {
		if resultBlobs == nil {
			return nil, fmt.Errorf("result blob store unavailable on modeled worker")
		}
		return resultBlobs.GetBytes(ctx, name)
	})
	scheduleTimer := func(ctx context.Context, step uint64, fireAt time.Time, domain string) error {
		started := ops.begin()
		renewCtx, stopRenew := context.WithTimeout(ctx, 3*time.Second)
		_, timing, err := ops.renew(renewCtx, l, 0)
		stopRenew()
		ops.finish(started, "lease_renew_timer", step, "", err, timing)
		if err != nil {
			return err
		}
		started = ops.begin()
		if domain == "" {
			err = w.scheduleTimer(ctx, typ, id, input.Sequence, step, fireAt)
		} else {
			err = w.scheduleDomainTimerObserved(ctx, typ, id, input.Sequence, step, fireAt, domain, requestIndices[step], ops)
		}
		ops.finish(started, "timer_schedule", step, "", err)
		if err != nil && domain != "" && w.nativeSchedules {
			return fmt.Errorf("%w: %w", wf.ErrTimerHint, err)
		}
		return err
	}
	wctx.SetTimerSupport(wakeupAt, func(ctx context.Context) (time.Time, error) {
		started := ops.begin()
		serverTime, err := w.serverNow(ctx)
		ops.finishTimerClock(started, nextIndex(), serverTime, err)
		return serverTime, err
	}, func(ctx context.Context, step uint64, fireAt time.Time) error {
		return scheduleTimer(ctx, step, fireAt, "")
	})
	if w.timerClockBounds != nil {
		if err := wctx.SetTimerClockSupport(wf.TimerClockSupport{Domain: w.timerClockDomain, Bounds: func(ctx context.Context) (time.Time, time.Time, error) {
			started := ops.begin()
			lower, upper, err := w.domainTimerBounds(ctx)
			ops.finishDomainClock(started, nextIndex(), w.timerClockDomain, lower, upper, err)
			return lower, upper, err
		}, Schedule: scheduleTimer, ScheduleIsHint: w.nativeSchedules}); err != nil {
			return err
		}
	}
	wctx.SetTimerObserver(w.metrics.recordTimerFired)
	if graph != nil {
		wctx.SetChildResultValidator(graph.validateSelectedChild)
	}
	wctx.SetChildSupport(typ, id, input.Sequence, func(ctx context.Context, childType, childID string, childInput []byte, signalName string) error {
		return w.startChild(ctx, childType, childID, childInput, typ, id, input.Sequence, signalName, ops)
	})
	stopCancelWatch := w.watchRunningCancellation(ctx, typ, id, input.Sequence, cancelHandler)
	outcome, joined := runHandler(handlerCtx, func() (json.RawMessage, error) {
		if resumed != nil {
			return w.continuations[typ][checkpointInfo.Stage](wctx, inputData, checkpointInfo.Data)
		}
		return handler(wctx, inputData)
	})
	cancelSeen := stopCancelWatch()
	if !joined {
		// The abandoned handler still owns its SDK/journal buffers. Retry the
		// delivery; a durable cancel signal is drained before the next handler
		// entry. Do not inspect or mutate those buffers on this goroutine.
		return outcome.err
	}
	result, runErr, panicked := outcome.result, outcome.err, outcome.panicked
	if cancelSeen {
		var current []wf.Signal
		var err error
		if graph != nil && graph.store.CanonicalSignals() {
			current, err = w.drainCanonicalSignals(ctx, graph, checkpointInfo.SignalCursor, records, appendEntry, ops)
		} else {
			current, err = w.drainSignalsFrom(ctx, typ, id, input.Sequence, checkpointInfo.SignalCursor, records, appendEntry, ops)
		}
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
	if failure := wctx.ContinuationFailure(); failure != nil {
		return failure
	}
	if point, ok := wctx.Continuation(); ok {
		// The append closures capture ctx. Assign the publication context so
		// archive, manifest, purge, suspension append and handoff share one
		// deadline; a missing reply cannot hold this delivery indefinitely.
		var stopPublication context.CancelFunc
		ctx, stopPublication = context.WithTimeout(ctx, 15*time.Second)
		defer stopPublication()
		started := ops.begin()
		err := w.publishContinuation(ctx, typ, id, input.Sequence, l, records, point, appendEntry)
		ops.finish(started, "continuation_publish", 0, "", err)
		return err
	}
	if panicked {
		reason := journal.AttemptError(runErr.Error())
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
	if errors.Is(runErr, journal.ErrStale) || errors.Is(runErr, journal.ErrUnknown) || errors.Is(runErr, lease.ErrLost) || errors.Is(runErr, context.Canceled) || errors.Is(runErr, wf.ErrTimerSchedule) || errors.Is(runErr, wf.ErrChildStart) || errors.Is(runErr, ErrResultBlobUnknown) || errors.Is(runErr, ErrResultBlobUnavailable) || errors.Is(runErr, wf.ErrCheckpointStore) {
		return runErr
	}
	out := wf.Outcome{InvSeq: input.Sequence, Result: result}
	kind := journal.Completed
	if runErr != nil {
		kind = journal.Failed
		out = wf.Outcome{InvSeq: input.Sequence, Error: runErr.Error()}
	} else if len(result) > wf.MaxInlineTerminal {
		if resultBlobs == nil {
			return fmt.Errorf("terminal blob store unavailable on modeled worker")
		}
		digest := sha256.Sum256(result)
		name := "terminal-result-" + hex.EncodeToString(digest[:])
		if err := resultBlobs.PutBytes(ctx, name, result); err != nil {
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

func (w *Worker) resultBlobs() ResultBlobPort {
	if w.resultBlobPort != nil {
		return w.resultBlobPort
	}
	if w.js != nil {
		return NewResultBlobPort(w.js)
	}
	return nil
}

func (w *Worker) persistAndNotify(ctx context.Context, typ, id string, invSeq uint64, payload []byte, headers nats.Header) error {
	if err := w.persistOutcome(ctx, typ, id, invSeq, payload); err != nil {
		return err
	}
	if err := NotifyParentWithClient(ctx, w.client, typ, id, invSeq, payload, headers); err != nil {
		return err
	}
	// A failed automatic snapshot must still be repaired on redelivery before
	// future terminal duplicates can skip it. Continuations own their snapshots.
	w.terminalHints.add(identity.Key(typ, id), w.graphJournal != nil || !w.jrn.HasSnapshotTransport() || w.continuations[typ] != nil)
	return nil
}

// NotifyParentWithClient sends a terminal child outcome only to the parent
// generation that started it. A retry uses the same child-derived signal key.
func NotifyParentWithClient(ctx context.Context, c *client.Client, typ, id string, invSeq uint64, payload []byte, headers nats.Header) error {
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
	// Metadata or signal replies can disappear while heartbeats still succeed.
	// Bound this worker-owned operation so redelivery can repair notification
	// without holding a completed child's lease for the workflow lifetime.
	notifyCtx, stopNotify := context.WithTimeout(ctx, 15*time.Second)
	defer stopNotify()
	_, err = c.SignalToGeneration(notifyCtx, parentType, parentID, signalName, payload, key, parentInvSeq)
	if errors.Is(err, client.ErrStaleGeneration) || errors.Is(err, client.ErrPurged) {
		return nil
	}
	return err
}

func (w *Worker) scheduleTimer(ctx context.Context, typ, id string, invSeq, step uint64, fireAt time.Time) error {
	port := w.timerSchedulePort
	if port == nil && w.js != nil {
		port = NewTimerSchedulePort(w.js)
	}
	if port == nil {
		return fmt.Errorf("timer schedule unavailable on modeled worker")
	}
	newPublish, err := ScheduleTimerWithPort(ctx, port, w.nativeSchedules, typ, id, invSeq, step, fireAt)
	if newPublish {
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
	if w.timerNowPort != nil {
		return w.timerNowPort(ctx)
	}
	if w.js == nil {
		return time.Time{}, fmt.Errorf("server time unavailable on modeled worker")
	}
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

// ObserveCancellationNotification delivers a core signal notification to a
// modeled worker after its signal transport has committed the message.
func (w *Worker) ObserveCancellationNotification(message *nats.Msg) {
	w.observeCancel(message)
}

// PollRunningCancellation checks retained WF_SIG state for a missed core
// notification. The real worker calls the same decision on registration and
// every 15 seconds; modeled workers can call it at virtual-time ticks.
func (w *Worker) PollRunningCancellation(ctx context.Context, typ, id string, invSeq uint64) (bool, error) {
	return w.pollRunningCancellation(ctx, typ, id, strconv.FormatUint(invSeq, 10), nil)
}

func (w *Worker) pollRunningCancellation(ctx context.Context, typ, id, generation string, expected *cancelWaiter) (bool, error) {
	if w.cancelPollPort == nil {
		return false, fmt.Errorf("cancellation poll transport unavailable")
	}
	subject := "wf.sig." + typ + "." + id + "." + client.CancelSignalName
	latest, err := w.cancelPollPort.LastGeneration(ctx, subject)
	if errors.Is(err, jetstream.ErrMsgNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if latest != generation {
		return false, nil
	}
	w.markRunningCancellation(cancelKey(typ, id, generation), expected)
	return true, nil
}

// watchRunningCancellation observes the core signal notification and checks
// the durable stream at registration and after gaps. A handler that cannot join
// after cancellation leaves its durable cancel signal for the next delivery.
func (w *Worker) watchRunningCancellation(ctx context.Context, typ, id string, invSeq uint64, cancelHandler context.CancelFunc) func() bool {
	if w.cancelStream == nil && !w.modeledCancelNotifications && w.cancelPollPort == nil {
		return func() bool { return false }
	}
	generation := strconv.FormatUint(invSeq, 10)
	key := cancelKey(typ, id, generation)
	waiter := &cancelWaiter{cancel: cancelHandler}
	w.cancelMu.Lock()
	w.cancelWaiters[key] = waiter
	w.cancelMu.Unlock()
	watchCtx, stop := context.WithCancel(ctx)
	done := make(chan struct{})
	if w.cancelStream == nil {
		close(done)
	} else {
		go func() {
			defer close(done)
			ticker := time.NewTicker(15 * time.Second)
			defer ticker.Stop()
			for watchCtx.Err() == nil {
				lookupCtx, finish := context.WithTimeout(watchCtx, 2*time.Second)
				found, _ := w.pollRunningCancellation(lookupCtx, typ, id, generation, waiter)
				finish()
				if found {
					return
				}
				select {
				case <-watchCtx.Done():
					return
				case <-ticker.C:
				}
			}
		}()
	}
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

func (w *Worker) drainSignals(ctx context.Context, typ, id string, invSeq uint64, records []journal.Record, appendEntry func(journal.Kind, json.RawMessage) error, ops *deliveryOperations) ([]wf.Signal, error) {
	return w.drainSignalsFrom(ctx, typ, id, invSeq, 0, records, appendEntry, ops)
}

func (w *Worker) drainSignalsFrom(ctx context.Context, typ, id string, invSeq, cursor uint64, records []journal.Record, appendEntry func(journal.Kind, json.RawMessage) error, ops *deliveryOperations) ([]wf.Signal, error) {
	port := w.signalDrainPort
	if port == nil {
		port = NewSignalDrainPort(w.js)
	}
	if ops != nil {
		port = observedSignalDrain{SignalDrainPort: port, operations: ops}
	}
	return drainSignalsFromPort(ctx, port, typ, id, invSeq, cursor, records, appendEntry)
}

// DrainSignalsWithPort runs the production signal drain against a supplied
// retained-read transport. appendEntry is the worker's fenced journal append.
func DrainSignalsWithPort(ctx context.Context, port SignalDrainPort, typ, id string, invSeq uint64, records []journal.Record, appendEntry func(journal.Kind, json.RawMessage) error) ([]wf.Signal, error) {
	return drainSignalsFromPort(ctx, port, typ, id, invSeq, 0, records, appendEntry)
}

func drainSignalsFromPort(ctx context.Context, port SignalDrainPort, typ, id string, invSeq, lastSeq uint64, records []journal.Record, appendEntry func(journal.Kind, json.RawMessage) error) ([]wf.Signal, error) {
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
		payload, err := signalPayloadWithPort(ctx, port, event)
		if err != nil {
			return nil, err
		}
		signals = append(signals, wf.Signal{Sequence: event.Sequence, Name: event.Name, Payload: payload})
	}
	lookupCtx, stopLookup := context.WithTimeout(ctx, 5*time.Second)
	lastSignalSeq, err := port.LastSignalSequence(lookupCtx)
	stopLookup()
	if err != nil {
		return nil, err
	}
	prefix := "wf.sig." + typ + "." + id + "."
	for seq := lastSeq + 1; seq <= lastSignalSeq && seq != 0; {
		lookupCtx, stopLookup = context.WithTimeout(ctx, 5*time.Second)
		m, err := port.NextSignal(lookupCtx, seq, prefix+"*")
		stopLookup()
		if errors.Is(err, jetstream.ErrMsgNotFound) {
			break
		}
		if err != nil {
			return nil, err
		}
		if m.Sequence > lastSignalSeq {
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
		payload, err := signalPayloadWithPort(ctx, port, event)
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

func signalPayloadWithPort(ctx context.Context, port SignalDrainPort, event signalRecord) ([]byte, error) {
	payload := event.Payload
	if event.Ref != "" {
		var err error
		payload, err = port.SignalBlob(ctx, event.Ref)
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
	port := w.outcomePort
	if port == nil {
		port = NewOutcomePort(w.state)
	}
	return PersistOutcomeWithPort(attemptCtx, port, typ, id, invSeq, payload)
}
