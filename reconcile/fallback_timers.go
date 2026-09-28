package reconcile

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"js-wf/identity"
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
	js  jetstream.JetStream
	Now func(context.Context) (time.Time, error)
}

func NewFallbackTimerScan(js jetstream.JetStream) *FallbackTimerScan {
	return &FallbackTimerScan{js: js, Now: func(ctx context.Context) (time.Time, error) {
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
	}}
}

func (s *FallbackTimerScan) Scan(ctx context.Context, next uint64, budget int, dryRun bool) (ScanResult, error) {
	if budget < 1 {
		return ScanResult{}, fmt.Errorf("scan budget must be positive")
	}
	if next == 0 {
		next = 1
	}
	timers, err := s.js.Stream(ctx, "WF_TIMER")
	if err != nil {
		return ScanResult{}, err
	}
	info, err := timers.Info(ctx)
	if err != nil {
		return ScanResult{}, err
	}
	state, err := s.js.KeyValue(ctx, "WF_STATE")
	if err != nil {
		return ScanResult{}, err
	}
	now, err := s.Now(ctx)
	if err != nil {
		return ScanResult{}, err
	}
	result := ScanResult{NextSequence: next}
	for scanned := 0; scanned < budget; scanned++ {
		if next > info.State.LastSeq {
			result.NextSequence = 1
			return result, nil
		}
		message, err := timers.GetMsg(ctx, next)
		if errors.Is(err, jetstream.ErrMsgNotFound) {
			next++
			result.NextSequence = next
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
			FireAt time.Time `json:"fire_at"`
		}
		if json.Unmarshal(message.Data, &timer) != nil || timer.FireAt.IsZero() {
			return result, fmt.Errorf("invalid fallback timer payload at %d", message.Sequence)
		}
		retired, err := fallbackTimerRetired(ctx, state, parts[2], parts[3], generation)
		if err != nil {
			return result, err
		}
		if retired {
			result.Removed++
			if !dryRun {
				if err := timers.DeleteMsg(ctx, message.Sequence); err != nil {
					return result, err
				}
			}
			continue
		}
		if now.Before(timer.FireAt) {
			continue
		}
		result.Reenqueued++
		if dryRun {
			continue
		}
		wakeup := &nats.Msg{Subject: identity.RunSubject(parts[2], parts[3], provision.Partitions), Data: []byte(identity.Key(parts[2], parts[3])), Header: nats.Header{}}
		wakeup.Header.Set(identity.TimerInvSeqHeader, parts[4])
		wakeup.Header.Set(identity.TimerStepHeader, parts[5])
		messageID := fmt.Sprintf("fallback-timer:%s:%s:%d:%d", parts[2], parts[3], generation, step)
		if _, err := s.js.PublishMsg(ctx, wakeup, jetstream.WithMsgID(messageID)); err != nil {
			return result, err
		}
		if err := timers.DeleteMsg(ctx, message.Sequence); err != nil {
			return result, err
		}
	}
	return result, nil
}

func fallbackTimerRetired(ctx context.Context, state jetstream.KeyValue, typ, id string, generation uint64) (bool, error) {
	key := identity.Key(typ, id)
	purging, err := state.Get(ctx, "purging."+key)
	if err == nil {
		seq, parseErr := strconv.ParseUint(string(purging.Value()), 10, 64)
		if parseErr != nil || seq == 0 {
			return false, fmt.Errorf("invalid purge marker for %s", key)
		}
		if seq == generation {
			return true, nil
		}
	} else if !errors.Is(err, jetstream.ErrKeyNotFound) {
		return false, err
	}
	value, err := state.Get(ctx, key)
	if errors.Is(err, jetstream.ErrKeyNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	marker, tomb, err := retention.Decode(value.Value())
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
