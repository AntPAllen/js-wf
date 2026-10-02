package worker

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"js-wf/identity"
)

type domainScheduleCapture struct {
	message   *nats.Msg
	payload   []byte
	id        string
	duplicate bool
	err       error
}

func (p *domainScheduleCapture) PublishFallback(_ context.Context, _ string, payload []byte, id string) (bool, error) {
	p.payload = append([]byte(nil), payload...)
	p.id = id
	return p.duplicate, p.err
}
func (p *domainScheduleCapture) PublishNative(_ context.Context, message *nats.Msg, id string) (bool, error) {
	p.message = message
	p.id = id
	return p.duplicate, p.err
}

func TestWorkerDomainTimerTranslatesNativeHintAndPreservesDeadline(t *testing.T) {
	base := time.Date(2026, 10, 2, 0, 0, 0, 0, time.UTC)
	for _, native := range []bool{false, true} {
		for _, offset := range []time.Duration{-time.Minute, time.Minute} {
			p := &domainScheduleCapture{}
			w := &Worker{nativeSchedules: native, timerSchedulePort: p, timerNowPort: func(context.Context) (time.Time, error) {
				if !native {
					t.Fatal("fallback read physical clock")
				}
				return base.Add(offset), nil
			}}
			if err := WithTimerClock("utc-quorum-v1", func(context.Context) (time.Time, time.Time, error) { return base, base.Add(time.Millisecond), nil })(w); err != nil {
				t.Fatal(err)
			}
			due := base.Add(time.Second)
			if err := w.scheduleDomainTimer(context.Background(), "test", "one", 1, 2, due, "utc-quorum-v1"); err != nil {
				t.Fatal(err)
			}
			if p.id != "timer:test.one:1:2" {
				t.Fatalf("unstable timer identity %q", p.id)
			}
			if native {
				if p.message.Header.Get(jetstream.ScheduleHeader) != "@at "+due.Add(offset).Format(time.RFC3339Nano) || p.message.Header.Get(identity.TimerClockDomainHeader) != "utc-quorum-v1" || p.message.Header.Get(identity.TimerDeadlineHeader) != due.Format(time.RFC3339Nano) {
					t.Fatalf("hint/domain headers=%v", p.message.Header)
				}
			} else {
				var record struct {
					FireAt      time.Time `json:"fire_at"`
					ClockDomain string    `json:"clock_domain"`
				}
				if err := json.Unmarshal(p.payload, &record); err != nil || !record.FireAt.Equal(due) || record.ClockDomain != "utc-quorum-v1" {
					t.Fatalf("fallback deadline=%+v err=%v", record, err)
				}
			}
			if got := w.metrics.timersScheduled.Load(); got != 1 {
				t.Fatalf("scheduled=%d", got)
			}
			p.duplicate = true
			if err := w.scheduleDomainTimer(context.Background(), "test", "one", 1, 2, due, "utc-quorum-v1"); err != nil || w.metrics.timersScheduled.Load() != 1 {
				t.Fatal("duplicate counted as a new schedule")
			}
		}
	}
}

func TestWorkerDomainClockFailureDoesNotPublish(t *testing.T) {
	at := time.Now().UTC()
	for _, mode := range []string{"unknown", "empty", "reversed", "unavailable", "canceled"} {
		t.Run(mode, func(t *testing.T) {
			p := &domainScheduleCapture{}
			w := &Worker{nativeSchedules: true, timerSchedulePort: p, timerNowPort: func(context.Context) (time.Time, error) { return at, nil }}
			WithTimerClock("utc-quorum-v1", func(context.Context) (time.Time, time.Time, error) {
				switch mode {
				case "empty":
					return time.Time{}, at, nil
				case "reversed":
					return at.Add(time.Second), at, nil
				case "unavailable":
					return time.Time{}, time.Time{}, nats.ErrTimeout
				default:
					return at, at, nil
				}
			})(w)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if mode == "canceled" {
				cancel()
			}
			domain := "utc-quorum-v1"
			if mode == "unknown" {
				domain = "foreign"
			}
			if err := w.scheduleDomainTimer(ctx, "test", "one", 1, 2, at.Add(time.Second), domain); err == nil || p.message != nil || w.metrics.timersScheduled.Load() != 0 {
				t.Fatalf("invalid clock published: err=%v message=%v", err, p.message)
			}
		})
	}
	p := &domainScheduleCapture{err: nats.ErrTimeout}
	published, err := ScheduleTimerDeadlineWithPort(context.Background(), p, false, "test", "one", 1, 0, TimerDeadline{FireAt: at, ClockDomain: "utc-quorum-v1"})
	if published || !errors.Is(err, nats.ErrTimeout) {
		t.Fatal("lost acknowledgement became success")
	}
	if err := WithTimerClock("", nil)(&Worker{}); err == nil {
		t.Fatal("invalid domain accepted")
	}
}
