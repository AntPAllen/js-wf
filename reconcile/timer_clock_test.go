package reconcile

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"js-wf/journal"
)

type domainTimerPort struct {
	TimerScanPort
	FallbackTimerScanPort
	records           []journal.Record
	timer             *jetstream.RawStreamMsg
	enqueued, deleted int
}

func (p *domainTimerPort) GetInvocation(context.Context, uint64) (*jetstream.RawStreamMsg, error) {
	return &jetstream.RawStreamMsg{Subject: "wf.inv.test.one", Sequence: 1}, nil
}
func (p *domainTimerPort) ReadJournal(context.Context, string, string) ([]journal.Record, error) {
	return p.records, nil
}
func (p *domainTimerPort) EnqueueTimer(context.Context, string, string, uint64) error {
	p.enqueued++
	return nil
}
func (p *domainTimerPort) LastTimerSequence(context.Context) (uint64, error) { return 1, nil }
func (p *domainTimerPort) GetTimer(context.Context, uint64) (*jetstream.RawStreamMsg, error) {
	return p.timer, nil
}
func (p *domainTimerPort) StateValue(context.Context, string) ([]byte, error) {
	return nil, jetstream.ErrKeyNotFound
}
func (p *domainTimerPort) PublishWakeup(context.Context, *nats.Msg, string) error {
	p.enqueued++
	return nil
}
func (p *domainTimerPort) DeleteTimer(context.Context, uint64) error { p.deleted++; return nil }

func TestRepairScannersRespectDomainClockAndFailClosed(t *testing.T) {
	at := time.Date(2026, 10, 2, 0, 0, 0, 0, time.UTC)
	for _, kind := range []string{"timer", "timer_start", "timer_await", "fallback"} {
		for _, mode := range []string{"future", "due", "missing", "failed", "empty"} {
			t.Run(kind+"/"+mode, func(t *testing.T) {
				payload, _ := json.Marshal(map[string]any{"kind": kind, "fire_at": at, "clock_domain": "utc-quorum-v1"})
				port := &domainTimerPort{records: []journal.Record{{Entry: journal.Entry{Kind: journal.StepRequested, Payload: payload}, Sequence: 2}}, timer: &jetstream.RawStreamMsg{Subject: "wf.timer.test.one.1.0", Sequence: 1, Data: payload}}
				clock := TimerDomainClock(func(ctx context.Context, domain string) (time.Time, error) {
					if domain != "utc-quorum-v1" {
						t.Fatal(domain)
					}
					if deadline, ok := ctx.Deadline(); !ok || time.Until(deadline) > 3*time.Second {
						t.Fatal("unbounded clock request")
					}
					switch mode {
					case "future":
						return at.Add(-time.Nanosecond), nil
					case "failed":
						return time.Time{}, context.DeadlineExceeded
					case "empty":
						return time.Time{}, nil
					default:
						return at, nil
					}
				})
				if mode == "missing" {
					clock = nil
				}
				var events []RepairEvent
				observe := func(e RepairEvent) { events = append(events, e) }
				var result ScanResult
				var err error
				if kind == "fallback" {
					s := NewFallbackTimerScanWithPort(port, func(context.Context) (time.Time, error) {
						t.Fatal("domain fallback queried legacy clock")
						return time.Time{}, nil
					})
					s.DomainNow = clock
					s.Observe = observe
					result, err = s.Scan(context.Background(), 1, 1, false)
				} else {
					s := NewTimerScanWithPort(port)
					s.Now = func() time.Time { t.Fatal("domain timer queried worker clock"); return time.Time{} }
					s.DomainNow = clock
					s.Observe = observe
					result, err = s.Scan(context.Background(), 1, 1, false)
				}
				if mode == "missing" || mode == "failed" || mode == "empty" {
					if !errors.Is(err, ErrTimerClock) || port.enqueued != 0 || port.deleted != 0 || len(events) != 0 {
						t.Fatalf("failed clock wrote: result=%+v port=%+v err=%v", result, port, err)
					}
					return
				}
				if err != nil {
					t.Fatal(err)
				}
				want := 0
				if mode == "due" {
					want = 1
				}
				if port.enqueued != want || result.Reenqueued != want {
					t.Fatalf("due decision: %+v %+v", result, port)
				}
				if kind == "fallback" && port.deleted != want {
					t.Fatal("fallback deleted before due")
				}
				if want == 1 && (len(events) != 1 || events[0].ClockDomain != "utc-quorum-v1" || events[0].Outcome != "acknowledged") {
					t.Fatalf("domain evidence=%+v", events)
				}
			})
		}
	}
}

func TestSuspendedDomainTimersIncludeGrace(t *testing.T) {
	at := time.Date(2026, 10, 2, 0, 0, 0, 0, time.UTC)
	for _, kind := range []string{"timer", "timer_await", "timer_signal_select", "select_many"} {
		t.Run(kind, func(t *testing.T) {
			payload, _ := json.Marshal(map[string]any{"kind": kind, "name": "wait", "timer_name": "wait", "fire_at": at, "clock_domain": "utc-quorum-v1", "cases": []map[string]any{{"kind": "timer", "name": "wait", "fire_at": at, "clock_domain": "utc-quorum-v1"}}})
			records := []journal.Record{{Entry: journal.Entry{Kind: journal.StepRequested, Payload: payload}}}
			s := NewSuspendedScanWithPort(nil)
			s.Grace = time.Second
			s.Now = func() time.Time { t.Fatal("tagged suspended timer read worker clock"); return time.Time{} }
			read := func() (bool, error) {
				if kind == "select_many" {
					return s.selectReady(context.Background(), "test", "one", 1, records)
				}
				return s.timerDue(context.Background(), records, "wait")
			}
			if ready, err := read(); ready || !errors.Is(err, ErrTimerClock) {
				t.Fatalf("missing clock=%t %v", ready, err)
			}
			lower := at.Add(time.Second - time.Nanosecond)
			s.DomainNow = func(context.Context, string) (time.Time, error) { return lower, nil }
			if ready, err := read(); ready || err != nil {
				t.Fatalf("grace ignored=%t %v", ready, err)
			}
			lower = at.Add(time.Second)
			if ready, err := read(); !ready || err != nil {
				t.Fatalf("due with grace=%t %v", ready, err)
			}
		})
	}
}

func TestDomainRepairClockHonorsCanceledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	clock := TimerDomainClock(func(context.Context, string) (time.Time, error) {
		t.Fatal("canceled clock invoked")
		return time.Now(), nil
	})
	ready, err := timerDeadlineDue(ctx, clock, nil, "utc-quorum-v1", time.Now(), 0)
	if ready || !errors.Is(err, ErrTimerClock) || !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled clock decision=%t %v", ready, err)
	}
}
