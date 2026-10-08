package reconcile

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"js-wf/client"
	"js-wf/identity"
	"js-wf/journal"

	"github.com/nats-io/nats.go/jetstream"
)

type TimerScan struct {
	graph     *journal.GraphStore
	port      TimerScanPort
	Now       func() time.Time
	DomainNow TimerDomainClock
	Observe   func(RepairEvent)
}

func NewTimerScan(js jetstream.JetStream) *TimerScan {
	return NewTimerScanWithPort(&jetStreamTimerScanPort{js: js, client: client.New(js), jrn: journal.New(js)})
}

// TimerScanPort contains the retained invocation and journal reads and the
// wakeup enqueue used by the timer repair decision.
type TimerScanPort interface {
	GetInvocation(context.Context, uint64) (*jetstream.RawStreamMsg, error)
	LastInvocationSequence(context.Context) (uint64, error)
	ReadJournal(context.Context, string, string) ([]journal.Record, error)
	EnqueueTimer(context.Context, string, string, uint64) error
}

func NewTimerScanWithPort(port TimerScanPort) *TimerScan {
	return &TimerScan{port: port, Now: time.Now}
}

type jetStreamTimerScanPort struct {
	js     jetstream.JetStream
	client *client.Client
	jrn    *journal.Store
	mu     sync.Mutex
	inv    jetstream.Stream
}

func (p *jetStreamTimerScanPort) stream(ctx context.Context) (jetstream.Stream, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.inv != nil {
		return p.inv, nil
	}
	stream, err := p.js.Stream(ctx, "WF_INV")
	if err != nil {
		return nil, err
	}
	p.inv = stream
	return stream, nil
}

func (p *jetStreamTimerScanPort) GetInvocation(ctx context.Context, sequence uint64) (*jetstream.RawStreamMsg, error) {
	stream, err := p.stream(ctx)
	if err != nil {
		return nil, err
	}
	return stream.GetMsg(ctx, sequence)
}

func (p *jetStreamTimerScanPort) LastInvocationSequence(ctx context.Context) (uint64, error) {
	stream, err := p.stream(ctx)
	if err != nil {
		return 0, err
	}
	info, err := stream.Info(ctx)
	if err != nil {
		return 0, err
	}
	return info.State.LastSeq, nil
}

func (p *jetStreamTimerScanPort) ReadJournal(ctx context.Context, typ, id string) ([]journal.Record, error) {
	records, _, err := p.jrn.Read(ctx, typ, id)
	return records, err
}

func (p *jetStreamTimerScanPort) EnqueueTimer(ctx context.Context, typ, id string, sequence uint64) error {
	return p.client.Enqueue(ctx, typ, id, fmt.Sprintf("timer-reconcile:%s:%s:%d", typ, id, sequence))
}

// Scan enqueues a fresh run for each invocation whose journal still has an
// uncompleted sleep, timer creation, or timer await past its fire time. It
// repairs missing schedules: a due timer can use this wakeup's server time.
func (s *TimerScan) Scan(ctx context.Context, next uint64, budget int, dryRun bool) (result ScanResult, scanErr error) {
	if budget < 1 {
		return ScanResult{}, fmt.Errorf("scan budget must be positive")
	}
	if next == 0 {
		next = 1
	}
	result = ScanResult{NextSequence: next}
	initial, confirmed := next, next
	defer func() {
		if scanErr != nil && confirmed > initial {
			result.RetrySequence = confirmed
		}
	}()
	for scanned := 0; scanned < budget; scanned++ {
		m, err := s.port.GetInvocation(ctx, next)
		if errors.Is(err, jetstream.ErrMsgNotFound) {
			last, infoErr := s.port.LastInvocationSequence(ctx)
			if infoErr != nil {
				return result, infoErr
			}
			if next > last {
				result.NextSequence = 1
				return result, nil
			}
			next++
			result.NextSequence = next
			confirmed = next
			continue
		}
		if err != nil {
			return result, err
		}
		next = m.Sequence + 1
		result.NextSequence = next
		result.Inspected++
		parts := strings.Split(m.Subject, ".")
		if len(parts) != 4 || parts[0] != "wf" || parts[1] != "inv" || identity.Validate(parts[2], parts[3]) != nil {
			return result, fmt.Errorf("invalid invocation subject %q", m.Subject)
		}
		typ, id := parts[2], parts[3]
		records, err := readRepairJournal(ctx, s.graph, s.port, typ, id, m.Sequence)
		if err != nil {
			return result, err
		}
		if len(records) == 0 {
			confirmed = next
			continue
		}
		if records[len(records)-1].Kind == journal.Completed || records[len(records)-1].Kind == journal.Failed {
			if port, ok := s.port.(NativeTimerRetirePort); ok {
				removed, err := RetireNativeTimerHints(ctx, port, typ, id, m.Sequence, dryRun)
				result.Removed += removed
				if err != nil {
					return result, err
				}
			}
			confirmed = next
			continue
		}
		var pending *journal.Record
		for i := range records {
			r := &records[i]
			switch r.Kind {
			case journal.StepRequested:
				pending = r
			case journal.StepCompleted:
				pending = nil
			}
		}
		if pending == nil {
			confirmed = next
			continue
		}
		var req struct {
			Kind        string    `json:"kind"`
			FireAt      time.Time `json:"fire_at"`
			ClockDomain string    `json:"clock_domain"`
		}
		if err := json.Unmarshal(pending.Payload, &req); err != nil {
			return result, err
		}
		if req.Kind != "timer" && req.Kind != "timer_start" && req.Kind != "timer_await" || req.FireAt.IsZero() {
			confirmed = next
			continue
		}
		due, err := timerDeadlineDue(ctx, s.DomainNow, func() (time.Time, error) { return s.Now(), nil }, req.ClockDomain, req.FireAt, 0)
		if err != nil {
			return result, err
		}
		if !due {
			confirmed = next
			continue
		}
		result.Reenqueued++
		var enqueueErr error
		if !dryRun {
			enqueueErr = s.port.EnqueueTimer(ctx, typ, id, pending.Sequence)
		}
		reportRepair(s.Observe, RepairEvent{Kind: "timer", Type: typ, ID: id, Reason: req.Kind, SourceSequence: m.Sequence, InvocationSequence: m.Sequence, JournalSequence: pending.Sequence, FireAt: &req.FireAt, ClockDomain: req.ClockDomain}, dryRun, enqueueErr)
		if enqueueErr != nil {
			return result, enqueueErr
		}
		confirmed = next
	}
	return result, nil
}

func RunTimerLoop(ctx context.Context, js jetstream.JetStream, workerID string, interval time.Duration, budget int) error {
	return runLoop(ctx, js, workerID, "timer", interval, budget, NewTimerScan(js).Scan)
}
