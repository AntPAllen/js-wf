package wf

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"
)

func invokeClockTimer(c *Context, kind string) error {
	if kind == "sleep" {
		return Sleep(c, "wait", 250*time.Millisecond)
	}
	timer, err := c.Timer("wait", 250*time.Millisecond)
	if err != nil {
		return err
	}
	switch kind {
	case "await":
		return timer.Await()
	case "select_signal":
		_, _, err = timer.SelectSignal("go")
	case "select_many":
		_, _, err = Select(c, timer)
	default:
		panic(kind)
	}
	return err
}

func TestDomainTimersUseUpperOriginAndLowerDueAcrossDeliveryClocks(t *testing.T) {
	for _, kind := range []string{"sleep", "await", "select_signal", "select_many"} {
		for _, skew := range []time.Duration{-time.Minute, time.Minute} {
			t.Run(fmt.Sprint(kind, "/", skew), func(t *testing.T) {
				origin := time.Date(2026, 10, 2, 0, 0, 0, 0, time.UTC)
				deadline := origin.Add(260 * time.Millisecond)
				var entries []Entry
				appendEntry := func(_ context.Context, k Kind, p json.RawMessage) error {
					entries = append(entries, Entry{Index: uint64(len(entries) + 1), Kind: k, Payload: p})
					return nil
				}
				lower, upper := origin, origin.Add(10*time.Millisecond)
				schedules := 0
				setup := func() *Context {
					c := NewContext(context.Background(), entries, appendEntry)
					c.SetTimerSupport(origin.Add(skew), func(context.Context) (time.Time, error) {
						t.Fatal("domain origin used legacy clock")
						return time.Time{}, nil
					}, func(context.Context, uint64, time.Time) error {
						t.Fatal("domain schedule used legacy transport")
						return nil
					})
					err := c.SetTimerClockSupport(TimerClockSupport{Domain: "utc-quorum-v1", Bounds: func(context.Context) (time.Time, time.Time, error) { return lower, upper, nil }, Schedule: func(_ context.Context, _ uint64, at time.Time, domain string) error {
						if domain != "utc-quorum-v1" || !at.Equal(deadline) {
							t.Fatalf("lost deadline domain: %s %s", at, domain)
						}
						schedules++
						return nil
					}})
					if err != nil {
						t.Fatal(err)
					}
					return c
				}
				if err := invokeClockTimer(setup(), kind); !errors.Is(err, ErrSuspended) {
					t.Fatalf("fresh timer=%v", err)
				}
				var req request
				if err := json.Unmarshal(entries[0].Payload, &req); err != nil || req.ClockDomain != "utc-quorum-v1" || !req.FireAt.Equal(deadline) {
					t.Fatalf("durable request=%+v err=%v", req, err)
				}
				lower, upper = origin.Add(250*time.Millisecond), origin.Add(270*time.Millisecond)
				if err := invokeClockTimer(setup(), kind); !errors.Is(err, ErrSuspended) {
					t.Fatalf("interval straddles due; timer=%v", err)
				}
				// A legacy-only or differently configured replacement must refuse
				// this pending durable timer even with a far-ahead delivery time.
				before := len(entries)
				c := NewContext(context.Background(), entries, appendEntry)
				c.SetTimerSupport(origin.Add(time.Minute), nil, nil)
				if err := invokeClockTimer(c, kind); !errors.Is(err, ErrTimerSchedule) {
					t.Fatalf("missing domain support=%v", err)
				}
				if len(entries) != before {
					t.Fatal("missing clock changed journal")
				}
				c = NewContext(context.Background(), entries, appendEntry)
				c.SetTimerSupport(origin.Add(time.Minute), nil, nil)
				if err := c.SetTimerClockSupport(TimerClockSupport{Domain: "other-domain", Bounds: func(context.Context) (time.Time, time.Time, error) {
					t.Fatal("mismatched clock queried")
					return lower, upper, nil
				}}); err != nil {
					t.Fatal(err)
				}
				if err := invokeClockTimer(c, kind); !errors.Is(err, ErrTimerSchedule) {
					t.Fatalf("mismatched domain support=%v", err)
				}
				if len(entries) != before {
					t.Fatal("mismatched clock changed journal")
				}
				lower, upper = deadline, deadline.Add(20*time.Millisecond)
				c = setup()
				var observed time.Time
				c.SetTimerObserver(func(_, at time.Time) { observed = at })
				if err := invokeClockTimer(c, kind); err != nil {
					t.Fatalf("due canonical replay=%v", err)
				}
				if !observed.Equal(lower) {
					t.Fatalf("timer metric used delivery clock=%s want=%s", observed, lower)
				}
				if err := c.CheckComplete(); err != nil {
					t.Fatal(err)
				}
				// Completed choices are journal decisions, so neither clock nor
				// live scheduling is required to replay them.
				c = NewContext(context.Background(), entries, appendEntry)
				if err := invokeClockTimer(c, kind); err != nil {
					t.Fatalf("completed replay=%v", err)
				}
				wantSchedules := 1
				if kind == "sleep" {
					wantSchedules = 2
				}
				if schedules != wantSchedules {
					t.Fatalf("schedules=%d want=%d", schedules, wantSchedules)
				}
			})
		}
	}
}

func TestDomainClockRejectsInvalidBoundsAndPreservesLegacy(t *testing.T) {
	origin := time.Now().UTC()
	for _, mode := range []string{"zero", "reversed", "unavailable"} {
		t.Run(mode, func(t *testing.T) {
			writes := 0
			c := NewContext(context.Background(), nil, func(context.Context, Kind, json.RawMessage) error { writes++; return nil })
			err := c.SetTimerClockSupport(TimerClockSupport{Domain: "utc-quorum-v1", Bounds: func(context.Context) (time.Time, time.Time, error) {
				switch mode {
				case "zero":
					return time.Time{}, origin, nil
				case "reversed":
					return origin.Add(time.Second), origin, nil
				default:
					return time.Time{}, time.Time{}, context.DeadlineExceeded
				}
			}})
			if err != nil {
				t.Fatal(err)
			}
			if err := Sleep(c, "wait", time.Second); !errors.Is(err, ErrTimerSchedule) {
				t.Fatalf("invalid clock=%v", err)
			}
			if writes != 0 {
				t.Fatal("invalid clock committed a timer")
			}
		})
	}
	var entries []Entry
	appendEntry := func(_ context.Context, k Kind, p json.RawMessage) error {
		entries = append(entries, Entry{Index: uint64(len(entries) + 1), Kind: k, Payload: p})
		return nil
	}
	c := NewContext(context.Background(), nil, appendEntry)
	c.SetTimerSupport(origin, func(context.Context) (time.Time, error) { return origin, nil }, func(context.Context, uint64, time.Time) error { return nil })
	if err := Sleep(c, "old", time.Second); !errors.Is(err, ErrSuspended) {
		t.Fatal(err)
	}
	// Installing a new clock does not reinterpret an existing legacy deadline.
	c = NewContext(context.Background(), entries, appendEntry)
	c.SetTimerSupport(origin.Add(time.Second), nil, nil)
	if err := c.SetTimerClockSupport(TimerClockSupport{Domain: "utc-quorum-v1", Bounds: func(context.Context) (time.Time, time.Time, error) {
		t.Fatal("legacy timer queried new domain")
		return time.Time{}, time.Time{}, nil
	}}); err != nil {
		t.Fatal(err)
	}
	if err := Sleep(c, "old", time.Second); err != nil {
		t.Fatal(err)
	}
}

// Scheduling is optional only for explicitly repairable domain-tagged waits.
// Legacy/fallback support and cancellation retain their fail-closed behavior.
func TestDomainNativeHintFailureUsesDurableRepair(t *testing.T) {
	for _, kind := range []string{"sleep", "await", "select_signal", "select_many"} {
		for _, mode := range []string{"hint", "required", "cancelled", "missing_scheduler"} {
			t.Run(kind+"/"+mode, func(t *testing.T) {
				origin := time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)
				base, cancel := context.WithCancel(context.Background())
				defer cancel()
				var entries []Entry
				appendEntry := func(_ context.Context, k Kind, p json.RawMessage) error {
					entries = append(entries, Entry{Index: uint64(len(entries) + 1), Kind: k, Payload: p})
					return nil
				}
				c := NewContext(base, nil, appendEntry)
				support := TimerClockSupport{Domain: "utc-quorum-v1", ScheduleIsHint: mode != "required", Bounds: func(context.Context) (time.Time, time.Time, error) { return origin, origin, nil }, Schedule: func(context.Context, uint64, time.Time, string) error {
					if mode == "cancelled" {
						cancel()
						return context.Canceled
					}
					return context.DeadlineExceeded
				}}
				if mode == "missing_scheduler" {
					support.Schedule = nil
				}
				if err := c.SetTimerClockSupport(support); err != nil {
					t.Fatal(err)
				}
				err := invokeClockTimer(c, kind)
				if mode != "hint" {
					if !errors.Is(err, ErrTimerSchedule) || len(entries) != 1 {
						t.Fatalf("required schedule failure: entries=%d err=%v", len(entries), err)
					}
					return
				}
				if !errors.Is(err, ErrSuspended) || c.WaitingOn() == "" {
					t.Fatalf("repairable hint did not suspend: err=%v", err)
				}
				// Due is still proved by domain bounds, never by a shifted
				// delivery timestamp or failure of the scheduling hint.
				c = NewContext(context.Background(), entries, appendEntry)
				support.Bounds = func(context.Context) (time.Time, time.Time, error) {
					return origin.Add(250 * time.Millisecond), origin.Add(250 * time.Millisecond), nil
				}
				if err := c.SetTimerClockSupport(support); err != nil {
					t.Fatal(err)
				}
				if err := invokeClockTimer(c, kind); err != nil {
					t.Fatalf("due repair replay failed: %v", err)
				}
			})
		}
	}
}
