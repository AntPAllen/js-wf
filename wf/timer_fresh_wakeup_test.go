package wf

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"
)

// The delivery timestamp can come from a different leader clock than TimerNow.
// A positive timer created in this delivery must not consume that older wakeup.
func TestFreshPositiveTimersIgnoreCurrentDeliveryWakeup(t *testing.T) {
	for _, kind := range []string{"sleep", "await", "select_signal", "select_many"} {
		t.Run(kind, func(t *testing.T) {
			base := time.Date(2026, 10, 1, 23, 18, 57, 0, time.UTC)
			origin := base.Add(-time.Minute)
			var entries []Entry
			appendEntry := func(_ context.Context, k Kind, p json.RawMessage) error {
				entries = append(entries, Entry{Index: uint64(len(entries) + 1), Kind: k, Payload: p})
				return nil
			}
			scheduled := 0
			invoke := func(c *Context) error {
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
					return err
				case "select_many":
					_, _, err = Select(c, timer)
					return err
				}
				panic("unknown test case")
			}
			c := NewContext(context.Background(), nil, appendEntry)
			c.SetTimerSupport(base, func(context.Context) (time.Time, error) { return origin, nil }, func(_ context.Context, _ uint64, at time.Time) error {
				scheduled++
				if !at.Equal(origin.Add(250 * time.Millisecond)) {
					t.Fatalf("due=%s", at)
				}
				return nil
			})
			if err := invoke(c); !errors.Is(err, ErrSuspended) {
				t.Fatalf("fresh positive timer consumed old wakeup: %v", err)
			}
			if scheduled != 1 {
				t.Fatalf("schedules=%d", scheduled)
			}
			last := entries[len(entries)-1]
			if last.Kind != StepRequested {
				t.Fatalf("fresh wait completed: %+v", last)
			}
			c = NewContext(context.Background(), entries, appendEntry)
			c.SetTimerSupport(origin.Add(250*time.Millisecond), nil, nil)
			if err := invoke(c); err != nil {
				t.Fatalf("due replay: %v", err)
			}
			if err := c.CheckComplete(); err != nil {
				t.Fatal(err)
			}
			if scheduled != 1 {
				t.Fatalf("replay rescheduled: %d", scheduled)
			}
		})
	}
}
