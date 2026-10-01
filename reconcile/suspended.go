package reconcile

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"js-wf/client"
	"js-wf/identity"
	"js-wf/journal"

	"github.com/nats-io/nats.go/jetstream"
)

// SuspendedScan inspects retained invocations whose latest journal entry is
// Suspended. It repairs an overdue timer or an available awaited signal.
type SuspendedScan struct {
	Observe func(RepairEvent)
	port    SuspendedScanPort
	Now     func() time.Time
	Grace   time.Duration
}

const suspendedRetryWindow = 10 * time.Second

func NewSuspendedScan(js jetstream.JetStream) *SuspendedScan {
	return NewSuspendedScanWithPort(NewSuspendedScanPort(js))
}

func NewSuspendedScanPort(js jetstream.JetStream) SuspendedScanPort {
	return &jetStreamSuspendedScanPort{js: js, client: client.New(js), jrn: journal.New(js)}
}

// SuspendedScanPort contains the retained reads and wakeup publish used by
// the production suspended-wait repair decision.
type SuspendedScanPort interface {
	LastInvocationSequence(context.Context) (uint64, error)
	GetInvocation(context.Context, uint64) (*jetstream.RawStreamMsg, error)
	ReadJournal(context.Context, string, string) ([]journal.Record, error)
	GetSignalAfter(context.Context, string, uint64) (*jetstream.RawStreamMsg, error)
	EnqueueSuspended(context.Context, string, string, uint64, int64) error
}

func NewSuspendedScanWithPort(port SuspendedScanPort) *SuspendedScan {
	return &SuspendedScan{port: port, Now: time.Now, Grace: time.Second}
}

type jetStreamSuspendedScanPort struct {
	js     jetstream.JetStream
	client *client.Client
	jrn    *journal.Store
	mu     sync.Mutex
	inv    jetstream.Stream
	sig    jetstream.Stream
}

func (p *jetStreamSuspendedScanPort) stream(ctx context.Context, name string) (jetstream.Stream, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if name == "WF_INV" && p.inv != nil {
		return p.inv, nil
	}
	if name == "WF_SIG" && p.sig != nil {
		return p.sig, nil
	}
	stream, err := p.js.Stream(ctx, name)
	if err != nil {
		return nil, err
	}
	if name == "WF_INV" {
		p.inv = stream
	} else {
		p.sig = stream
	}
	return stream, nil
}

func (p *jetStreamSuspendedScanPort) LastInvocationSequence(ctx context.Context) (uint64, error) {
	stream, err := p.stream(ctx, "WF_INV")
	if err != nil {
		return 0, err
	}
	info, err := stream.Info(ctx)
	if err != nil {
		return 0, err
	}
	return info.State.LastSeq, nil
}

func (p *jetStreamSuspendedScanPort) GetInvocation(ctx context.Context, sequence uint64) (*jetstream.RawStreamMsg, error) {
	stream, err := p.stream(ctx, "WF_INV")
	if err != nil {
		return nil, err
	}
	return stream.GetMsg(ctx, sequence)
}

func (p *jetStreamSuspendedScanPort) ReadJournal(ctx context.Context, typ, id string) ([]journal.Record, error) {
	records, _, err := p.jrn.Read(ctx, typ, id)
	return records, err
}

func (p *jetStreamSuspendedScanPort) GetSignalAfter(ctx context.Context, subject string, sequence uint64) (*jetstream.RawStreamMsg, error) {
	stream, err := p.stream(ctx, "WF_SIG")
	if err != nil {
		return nil, err
	}
	return stream.GetMsg(ctx, sequence, jetstream.WithGetMsgSubject(subject))
}

func (p *jetStreamSuspendedScanPort) EnqueueSuspended(ctx context.Context, typ, id string, journalSeq uint64, retryWindow int64) error {
	return p.client.Enqueue(ctx, typ, id, fmt.Sprintf("reconcile:%s:%s:%d:%d", typ, id, journalSeq, retryWindow))
}

// Scan advances a stream-sequence cursor. The budget counts holes as well as
// retained invocations, so a heavily purged stream cannot monopolize a scan.
// In dry-run mode Candidates reports the messages that would be published.
func (s *SuspendedScan) Scan(ctx context.Context, next uint64, budget int, dryRun bool) (ScanResult, error) {
	if budget < 1 || s.Grace < 0 {
		return ScanResult{}, fmt.Errorf("invalid suspended scan budget or grace")
	}
	if next == 0 {
		next = 1
	}
	last, err := s.port.LastInvocationSequence(ctx)
	if err != nil {
		return ScanResult{}, err
	}
	if next > last {
		return ScanResult{NextSequence: 1}, nil
	}
	// Keep memory and concurrent request pressure bounded even if an operator
	// supplies a very large budget. A scan budget is a maximum, not a promise
	// to inspect every requested sequence in one call.
	count := min(budget, 4096)
	available := last - next + 1
	wrap := available < uint64(count)
	if available < uint64(count) {
		count = int(available)
	}
	type inspected struct {
		retained  bool
		ready     bool
		candidate Candidate
		err       error
	}
	items := make([]inspected, count)
	jobs := make(chan int, 64)
	var workers sync.WaitGroup
	for worker := 0; worker < min(count, 32); worker++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for index := range jobs {
				input, err := s.port.GetInvocation(ctx, next+uint64(index))
				if errors.Is(err, jetstream.ErrMsgNotFound) {
					continue
				}
				if err != nil {
					items[index].err = err
					continue
				}
				items[index].retained = true
				items[index].candidate, items[index].ready, items[index].err = s.inspect(ctx, input)
			}
		}()
	}
	for index := range items {
		jobs <- index
	}
	close(jobs)
	workers.Wait()
	result := ScanResult{NextSequence: next + uint64(count)}
	if wrap {
		result.NextSequence = 1
	}
	var inspectErr error
	for _, item := range items {
		if item.err != nil {
			// A transient read on one invocation must not suppress ready
			// wakeups independently found elsewhere on this page. Keep the
			// cursor here so the failed read is retried on the next pass.
			if inspectErr == nil {
				inspectErr = item.err
			}
			continue
		}
		if item.retained {
			result.Inspected++
		}
		if item.ready {
			result.Candidates = append(result.Candidates, item.candidate)
		}
	}
	if inspectErr != nil {
		result.NextSequence = next
	}
	result.Reenqueued = len(result.Candidates)
	if !dryRun {
		// A prior wakeup may have been delivered while another worker held the
		// invocation lease, then its NAK lost during a route fault. Retrying the
		// same message ID forever leaves recovery to the consumer AckWait.
		// One ID per window bounds repeated wakeups while making a fresh run
		// available before that 20-second fallback.
		retryWindow := s.Now().UnixNano() / int64(suspendedRetryWindow)
		for _, candidate := range result.Candidates {
			err := s.port.EnqueueSuspended(ctx, candidate.Type, candidate.ID, candidate.JournalSeq, retryWindow)
			reportRepair(s.Observe, RepairEvent{Kind: "suspended", Type: candidate.Type, ID: candidate.ID, Reason: candidate.Reason, JournalSequence: candidate.JournalSeq, RetryWindow: retryWindow}, false, err)
			if err != nil {
				return result, err
			}
		}
	} else {
		for _, candidate := range result.Candidates {
			reportRepair(s.Observe, RepairEvent{Kind: "suspended", Type: candidate.Type, ID: candidate.ID, Reason: candidate.Reason, JournalSequence: candidate.JournalSeq}, true, nil)
		}
	}
	return result, inspectErr
}

func (s *SuspendedScan) inspect(ctx context.Context, input *jetstream.RawStreamMsg) (Candidate, bool, error) {
	parts := strings.Split(input.Subject, ".")
	if len(parts) != 4 || parts[0] != "wf" || parts[1] != "inv" || identity.Validate(parts[2], parts[3]) != nil {
		return Candidate{}, false, fmt.Errorf("invalid invocation subject %q", input.Subject)
	}
	typ, id := parts[2], parts[3]
	records, err := s.port.ReadJournal(ctx, typ, id)
	if err != nil {
		return Candidate{}, false, err
	}
	if len(records) == 0 {
		return Candidate{}, false, nil
	}
	last := records[len(records)-1]
	// A committed boundary is enabled even before its manifest or suspension.
	if last.Kind == journal.StepCompleted && len(records) >= 2 {
		request := records[len(records)-2]
		var declaration struct {
			Kind string `json:"kind"`
			Name string `json:"name"`
		}
		if request.Kind == journal.StepRequested && json.Unmarshal(request.Payload, &declaration) == nil && declaration.Kind == "checkpoint" {
			if identity.ValidateToken(declaration.Name) != nil {
				return Candidate{}, false, fmt.Errorf("invalid checkpoint stage")
			}
			return Candidate{Type: typ, ID: id, Reason: "continuation", JournalSeq: last.Sequence}, true, nil
		}
	}
	if last.Kind != journal.Suspended {
		return Candidate{}, false, nil
	}
	var suspended struct {
		WaitingOn string `json:"waiting_on"`
	}
	if err := json.Unmarshal(last.Payload, &suspended); err != nil {
		return Candidate{}, false, err
	}
	var ready bool
	var reason string
	switch {
	case strings.HasPrefix(suspended.WaitingOn, "continuation:"):
		name := strings.TrimPrefix(suspended.WaitingOn, "continuation:")
		if identity.ValidateToken(name) != nil {
			return Candidate{}, false, fmt.Errorf("invalid continuation wait %q", suspended.WaitingOn)
		}
		ready, reason = true, "continuation"
	case strings.HasPrefix(suspended.WaitingOn, "timer:"):
		name := strings.TrimPrefix(suspended.WaitingOn, "timer:")
		if identity.ValidateToken(name) != nil {
			return Candidate{}, false, fmt.Errorf("invalid timer wait %q", suspended.WaitingOn)
		}
		ready, err = s.timerDue(records, name)
		reason = "timer"
	case strings.HasPrefix(suspended.WaitingOn, "signal:"):
		name := strings.TrimPrefix(suspended.WaitingOn, "signal:")
		if identity.ValidateToken(name) != nil {
			return Candidate{}, false, fmt.Errorf("invalid signal wait %q", suspended.WaitingOn)
		}
		ready, err = signalAvailable(ctx, s.port, typ, id, name, input.Sequence, records)
		reason = "signal"
	case suspended.WaitingOn == "select_many":
		ready, err = s.selectReady(ctx, typ, id, input.Sequence, records)
		reason = "select"
	case strings.HasPrefix(suspended.WaitingOn, "select:"):
		parts := strings.Split(suspended.WaitingOn, ":")
		if len(parts) != 3 || identity.ValidateToken(parts[1]) != nil || identity.ValidateToken(parts[2]) != nil {
			return Candidate{}, false, fmt.Errorf("invalid select wait %q", suspended.WaitingOn)
		}
		ready, err = signalAvailable(ctx, s.port, typ, id, parts[2], input.Sequence, records)
		if err == nil && !ready {
			ready, err = s.timerDue(records, parts[1])
		}
		reason = "select"
	default:
		return Candidate{}, false, fmt.Errorf("unknown suspended wait %q", suspended.WaitingOn)
	}
	if err != nil || !ready {
		return Candidate{}, false, err
	}
	return Candidate{Type: typ, ID: id, Reason: reason, JournalSeq: last.Sequence}, true, nil
}

// A multi-case wait carries its complete durable case list in the pending
// request. Any ready case justifies a repair wakeup; a failed signal read must
// not suppress an independently due timer or another available signal.
func (s *SuspendedScan) selectReady(ctx context.Context, typ, id string, invSeq uint64, records []journal.Record) (bool, error) {
	var pending *journal.Record
	for i := range records {
		switch records[i].Kind {
		case journal.StepRequested:
			pending = &records[i]
		case journal.StepCompleted:
			pending = nil
		}
	}
	if pending == nil {
		return false, fmt.Errorf("multi-case suspension has no pending request")
	}
	var request struct {
		Kind  string `json:"kind"`
		Cases []struct {
			Kind   string    `json:"kind"`
			Name   string    `json:"name"`
			FireAt time.Time `json:"fire_at"`
		} `json:"cases"`
	}
	if err := json.Unmarshal(pending.Payload, &request); err != nil {
		return false, err
	}
	if request.Kind != "select_many" || len(request.Cases) == 0 {
		return false, fmt.Errorf("invalid multi-case request")
	}
	for _, c := range request.Cases {
		if identity.ValidateToken(c.Name) != nil || c.Kind != "timer" && c.Kind != "signal" && c.Kind != "promise" {
			return false, fmt.Errorf("invalid multi-case awaitable")
		}
	}
	var readErr error
	for _, c := range request.Cases {
		if c.Kind == "timer" {
			if c.FireAt.IsZero() || !s.Now().Before(c.FireAt.Add(s.Grace)) {
				return true, nil
			}
			continue
		}
		ready, err := signalAvailable(ctx, s.port, typ, id, c.Name, invSeq, records)
		if err != nil {
			if readErr == nil {
				readErr = err
			}
			continue
		}
		if ready {
			return true, nil
		}
	}
	return false, readErr
}

func (s *SuspendedScan) timerDue(records []journal.Record, name string) (bool, error) {
	var pending *journal.Record
	for i := range records {
		switch records[i].Kind {
		case journal.StepRequested:
			pending = &records[i]
		case journal.StepCompleted:
			pending = nil
		}
	}
	if pending == nil {
		return false, nil
	}
	var req struct {
		Kind      string    `json:"kind"`
		Name      string    `json:"name"`
		TimerName string    `json:"timer_name"`
		FireAt    time.Time `json:"fire_at"`
	}
	if err := json.Unmarshal(pending.Payload, &req); err != nil {
		return false, err
	}
	if req.Kind == "timer_signal_select" {
		return req.TimerName == name && !req.FireAt.IsZero() && !s.Now().Before(req.FireAt.Add(s.Grace)), nil
	}
	if (req.Kind != "timer" && req.Kind != "timer_await") || req.Name != name || req.FireAt.IsZero() {
		return false, nil
	}
	return !s.Now().Before(req.FireAt.Add(s.Grace)), nil
}

func signalAvailable(ctx context.Context, port SuspendedScanPort, typ, id, name string, invSeq uint64, records []journal.Record) (bool, error) {
	used := map[uint64]bool{}
	var consumed []uint64
	var lastConsumed uint64
	for _, record := range records {
		switch record.Kind {
		case journal.StepCompleted:
			var done struct {
				SignalSeq uint64 `json:"signal_seq"`
			}
			if err := json.Unmarshal(record.Payload, &done); err != nil {
				return false, err
			}
			if done.SignalSeq != 0 {
				used[done.SignalSeq] = true
			}
		case journal.SignalConsumed:
			var event struct {
				Sequence uint64 `json:"sig_seq"`
				Name     string `json:"name"`
			}
			if err := json.Unmarshal(record.Payload, &event); err != nil || event.Sequence == 0 {
				return false, fmt.Errorf("invalid consumed signal in journal")
			}
			if event.Sequence > lastConsumed {
				lastConsumed = event.Sequence
			}
			if event.Name == name {
				consumed = append(consumed, event.Sequence)
			}
		}
	}
	for _, seq := range consumed {
		if !used[seq] {
			return true, nil
		}
	}
	subject := "wf.sig." + typ + "." + id + "." + name
	for seq := lastConsumed + 1; seq != 0; {
		message, err := port.GetSignalAfter(ctx, subject, seq)
		if errors.Is(err, jetstream.ErrMsgNotFound) {
			return false, nil
		}
		if err != nil {
			return false, err
		}
		seq = message.Sequence + 1
		if generation := message.Header.Get("Wf-Inv-Seq"); generation != "" && generation != strconv.FormatUint(invSeq, 10) {
			continue
		}
		if !used[message.Sequence] {
			return true, nil
		}
	}
	return false, nil
}

// RunSuspendedLoop uses the same KV leader lease and persisted cursor as the
// other reconcilers, under its own suspended scan key.
func RunSuspendedLoop(ctx context.Context, js jetstream.JetStream, workerID string, interval time.Duration, budget int) error {
	return RunSuspendedLoopObserved(ctx, js, workerID, interval, budget, nil)
}

// RunSuspendedLoopObserved reports each completed scan attempt while using
// the same production scanner and fenced cursor loop. The observer must not
// block the reconciler.
func RunSuspendedLoopObserved(ctx context.Context, js jetstream.JetStream, workerID string, interval time.Duration, budget int, observe func(uint64, ScanResult, error)) error {
	return RunSuspendedLoopWithObservers(ctx, js, workerID, interval, budget, observe, nil)
}

// RunSuspendedLoopWithObservers records both each scan result and each actual
// repair attempt, including uncertain publications before a scan error.
func RunSuspendedLoopWithObservers(ctx context.Context, js jetstream.JetStream, workerID string, interval time.Duration, budget int, observe func(uint64, ScanResult, error), repair func(RepairEvent)) error {
	scanner := NewSuspendedScan(js)
	scanner.Observe = repair
	scan := scanner.Scan
	if observe == nil {
		return runLoop(ctx, js, workerID, "suspended", interval, budget, scan)
	}
	return runLoop(ctx, js, workerID, "suspended", interval, budget, func(attempt context.Context, next uint64, budget int, dryRun bool) (ScanResult, error) {
		result, err := scan(attempt, next, budget, dryRun)
		observe(next, result, err)
		return result, err
	})
}
