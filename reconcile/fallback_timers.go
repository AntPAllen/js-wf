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

	"js-wf/identity"
	"js-wf/internal/natsutil"
	"js-wf/provision"
	"js-wf/retention"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// FallbackTimerScan moves due WF_TIMER records into WF_RUN. It deletes a timer
// record only after WF_RUN acknowledges the publish; a crash at either side
// of that boundary is safe because the wakeup has a stable message ID and the
// worker treats duplicate wakeups as no-ops.
type FallbackTimerScan struct {
	port      FallbackTimerScanPort
	Now       func(context.Context) (time.Time, error)
	DomainNow TimerDomainClock
	Observe   func(RepairEvent)
}

// FallbackTimerScanPort is the retained timer, state, and run-publish boundary
// used by the production fallback repair decision.
type FallbackTimerScanPort interface {
	LastTimerSequence(context.Context) (uint64, error)
	GetTimer(context.Context, uint64) (*jetstream.RawStreamMsg, error)
	StateValue(context.Context, string) ([]byte, error)
	PublishWakeup(context.Context, *nats.Msg, string) error
	DeleteTimer(context.Context, uint64) error
}

type jetStreamFallbackTimerScanPort struct {
	js     jetstream.JetStream
	mu     sync.Mutex
	timers jetstream.Stream
	state  jetstream.KeyValue
}

func (p *jetStreamFallbackTimerScanPort) timerStream(ctx context.Context) (jetstream.Stream, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.timers != nil {
		return p.timers, nil
	}
	stream, err := p.js.Stream(ctx, "WF_TIMER")
	if err == nil {
		p.timers = stream
	}
	return stream, err
}

func (p *jetStreamFallbackTimerScanPort) stateBucket(ctx context.Context) (jetstream.KeyValue, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.state != nil {
		return p.state, nil
	}
	state, err := p.js.KeyValue(ctx, "WF_STATE")
	if err == nil {
		p.state = state
	}
	return state, err
}

func (p *jetStreamFallbackTimerScanPort) LastTimerSequence(ctx context.Context) (uint64, error) {
	stream, err := p.timerStream(ctx)
	if err != nil {
		return 0, err
	}
	info, err := stream.Info(ctx)
	if err != nil {
		return 0, err
	}
	return info.State.LastSeq, nil
}

func (p *jetStreamFallbackTimerScanPort) GetTimer(ctx context.Context, sequence uint64) (*jetstream.RawStreamMsg, error) {
	stream, err := p.timerStream(ctx)
	if err != nil {
		return nil, err
	}
	return stream.GetMsg(ctx, sequence)
}

func (p *jetStreamFallbackTimerScanPort) StateValue(ctx context.Context, key string) ([]byte, error) {
	state, err := p.stateBucket(ctx)
	if err != nil {
		return nil, err
	}
	entry, err := state.Get(ctx, key)
	if err != nil {
		return nil, err
	}
	return entry.Value(), nil
}

func (p *jetStreamFallbackTimerScanPort) PublishWakeup(ctx context.Context, message *nats.Msg, messageID string) error {
	_, err := p.js.PublishMsg(ctx, message, jetstream.WithMsgID(messageID))
	return err
}

func (p *jetStreamFallbackTimerScanPort) DeleteTimer(ctx context.Context, sequence uint64) error {
	if _, err := p.timerStream(ctx); err != nil {
		return err
	}
	return natsutil.DeleteStreamMessage(ctx, p.js, "WF_TIMER", sequence)
}

func NewFallbackTimerScan(js jetstream.JetStream) *FallbackTimerScan {
	return NewFallbackTimerScanWithPort(NewFallbackTimerScanPort(js), func(ctx context.Context) (time.Time, error) {
		run, err := js.Stream(ctx, "WF_RUN")
		if err != nil {
			return time.Time{}, err
		}
		info, err := run.Info(ctx)
		if err != nil {
			return time.Time{}, err
		}
		if info.TimeStamp.IsZero() {
			return time.Time{}, fmt.Errorf("server did not provide stream timestamp")
		}
		return info.TimeStamp, nil
	})
}

func NewFallbackTimerScanPort(js jetstream.JetStream) FallbackTimerScanPort {
	return &jetStreamFallbackTimerScanPort{js: js}
}

func NewFallbackTimerScanWithPort(port FallbackTimerScanPort, now func(context.Context) (time.Time, error)) *FallbackTimerScan {
	return &FallbackTimerScan{port: port, Now: now}
}

func (s *FallbackTimerScan) Scan(ctx context.Context, next uint64, budget int, dryRun bool) (result ScanResult, scanErr error) {
	if budget < 1 {
		return ScanResult{}, fmt.Errorf("scan budget must be positive")
	}
	if next == 0 {
		next = 1
	}
	last, err := s.port.LastTimerSequence(ctx)
	if err != nil {
		return ScanResult{}, err
	}
	var now time.Time
	var legacyRead bool
	legacyNow := func() (time.Time, error) {
		if !legacyRead {
			var err error
			now, err = s.Now(ctx)
			if err != nil {
				return time.Time{}, err
			}
			legacyRead = true
		}
		return now, nil
	}
	result = ScanResult{NextSequence: next}
	initial, confirmed := next, next
	defer func() {
		if scanErr != nil && confirmed > initial {
			result.RetrySequence = confirmed
		}
	}()
	for scanned := 0; scanned < budget; scanned++ {
		if next > last {
			result.NextSequence = 1
			return result, nil
		}
		message, err := s.port.GetTimer(ctx, next)
		if errors.Is(err, jetstream.ErrMsgNotFound) {
			next++
			result.NextSequence = next
			confirmed = next
			continue
		}
		if err != nil {
			return result, err
		}
		next = message.Sequence + 1
		result.NextSequence = next
		result.Inspected++
		parts := strings.Split(message.Subject, ".")
		if len(parts) != 6 || parts[0] != "wf" || parts[1] != "timer" || identity.Validate(parts[2], parts[3]) != nil {
			return result, fmt.Errorf("invalid fallback timer subject %q", message.Subject)
		}
		generation, genErr := strconv.ParseUint(parts[4], 10, 64)
		step, stepErr := strconv.ParseUint(parts[5], 10, 64)
		if genErr != nil || generation == 0 || stepErr != nil {
			return result, fmt.Errorf("invalid fallback timer sequence in %q", message.Subject)
		}
		var timer struct {
			FireAt      time.Time `json:"fire_at"`
			ClockDomain string    `json:"clock_domain"`
		}
		if json.Unmarshal(message.Data, &timer) != nil || timer.FireAt.IsZero() {
			return result, fmt.Errorf("invalid fallback timer payload at %d", message.Sequence)
		}
		retired, err := fallbackTimerRetired(ctx, s.port, parts[2], parts[3], generation)
		if err != nil {
			return result, err
		}
		if retired {
			result.Removed++
			if !dryRun {
				if err := s.port.DeleteTimer(ctx, message.Sequence); err != nil {
					return result, err
				}
			}
			confirmed = next
			continue
		}
		due, err := timerDeadlineDue(ctx, s.DomainNow, legacyNow, timer.ClockDomain, timer.FireAt, 0)
		if err != nil {
			return result, err
		}
		if !due {
			confirmed = next
			continue
		}
		result.Reenqueued++
		event := RepairEvent{Kind: "fallback-timer", Type: parts[2], ID: parts[3], Reason: "due_fallback_timer", SourceSequence: message.Sequence, InvocationSequence: generation, FireAt: &timer.FireAt, ClockDomain: timer.ClockDomain, TimerStep: &step}
		if dryRun {
			reportRepair(s.Observe, event, true, nil)
			confirmed = next
			continue
		}
		wakeup := &nats.Msg{Subject: identity.RunSubject(parts[2], parts[3], provision.Partitions), Data: []byte(identity.Key(parts[2], parts[3])), Header: nats.Header{}}
		wakeup.Header.Set(identity.TimerInvSeqHeader, parts[4])
		wakeup.Header.Set(identity.TimerStepHeader, parts[5])
		messageID := fmt.Sprintf("fallback-timer:%s:%s:%d:%d", parts[2], parts[3], generation, step)
		publishErr := s.port.PublishWakeup(ctx, wakeup, messageID)
		// A later timer deletion failure cannot turn an acknowledged publication
		// into uncertainty; retain the publication result at its actual boundary.
		reportRepair(s.Observe, event, false, publishErr)
		if publishErr != nil {
			return result, publishErr
		}
		if err := s.port.DeleteTimer(ctx, message.Sequence); err != nil {
			return result, err
		}
		confirmed = next
	}
	return result, nil
}

func fallbackTimerRetired(ctx context.Context, port FallbackTimerScanPort, typ, id string, generation uint64) (bool, error) {
	key := identity.Key(typ, id)
	purging, err := port.StateValue(ctx, "purging."+key)
	if err == nil {
		seq, parseErr := strconv.ParseUint(string(purging), 10, 64)
		if parseErr != nil || seq == 0 {
			return false, fmt.Errorf("invalid purge marker for %s", key)
		}
		if seq == generation {
			return true, nil
		}
	} else if !errors.Is(err, jetstream.ErrKeyNotFound) {
		return false, err
	}
	value, err := port.StateValue(ctx, key)
	if errors.Is(err, jetstream.ErrKeyNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	marker, tomb, err := retention.Decode(value)
	if err != nil {
		return false, err
	}
	return tomb && marker.InvSeq == generation, nil
}

// RunFallbackTimerLoop elects one scanner and persists its sequence cursor.
// It must run when WF_RUN was provisioned without native schedules.
func RunFallbackTimerLoop(ctx context.Context, js jetstream.JetStream, workerID string, interval time.Duration, budget int) error {
	return runLoop(ctx, js, workerID, "fallback-timer", interval, budget, NewFallbackTimerScan(js).Scan)
}
