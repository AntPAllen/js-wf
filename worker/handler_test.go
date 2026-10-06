package worker

import (
	"context"
	"encoding/json"
	"errors"
	"runtime"
	"strings"
	"testing"
	"time"
)

func TestHandlerReturnPanicAndGoexit(t *testing.T) {
	expected := errors.New("user error")
	cases := []struct {
		name     string
		fn       func() (json.RawMessage, error)
		panicked bool
		message  string
	}{
		{"return", func() (json.RawMessage, error) { return json.RawMessage(`42`), expected }, false, "user error"},
		{"panic", func() (json.RawMessage, error) { panic("broken") }, true, "workflow panic: broken"},
		{"goexit", func() (json.RawMessage, error) { runtime.Goexit(); return nil, nil }, true, "workflow exited via runtime.Goexit"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			outcome, joined := runHandler(ctx, tc.fn)
			if !joined || outcome.panicked != tc.panicked || outcome.err == nil || outcome.err.Error() != tc.message {
				t.Fatalf("outcome=%+v joined=%t", outcome, joined)
			}
			if tc.name == "return" && (string(outcome.result) != "42" || !errors.Is(outcome.err, expected)) {
				t.Fatalf("returned value/error lost: %+v", outcome)
			}
		})
	}
}

func TestHandlerCancellationAbandonsLateReturn(t *testing.T) {
	for _, late := range []string{"return", "panic", "goexit"} {
		t.Run(late, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			entered, release, finished := make(chan struct{}), make(chan struct{}), make(chan struct{})
			type result struct {
				outcome handlerOutcome
				joined  bool
			}
			done := make(chan result, 1)
			go func() {
				outcome, joined := runHandler(ctx, func() (json.RawMessage, error) {
					defer close(finished)
					close(entered)
					<-release // Deliberately ignore context cancellation.
					switch late {
					case "panic":
						panic("late panic")
					case "goexit":
						runtime.Goexit()
					}
					return json.RawMessage(`99`), nil
				})
				done <- result{outcome, joined}
			}()
			<-entered
			cancel()
			select {
			case got := <-done:
				if got.joined || !errors.Is(got.outcome.err, context.Canceled) || len(got.outcome.result) != 0 || got.outcome.panicked {
					t.Fatalf("abandoned outcome=%+v joined=%t", got.outcome, got.joined)
				}
			case <-time.After(time.Second):
				t.Fatal("ignored cancellation trapped handler boundary")
			}
			close(release)
			select {
			case <-finished:
			case <-time.After(time.Second):
				t.Fatal("late handler could not finish")
			}
		})
	}
}

func TestAlreadyCancelledHandlerDoesNotEnterUserCode(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	outcome, joined := runHandler(ctx, func() (json.RawMessage, error) { t.Error("cancelled handler entered"); return nil, nil })
	if joined || !errors.Is(outcome.err, context.Canceled) || strings.Contains(outcome.err.Error(), "panic") {
		t.Fatalf("outcome=%+v joined=%t", outcome, joined)
	}
}

func TestHandlerCancellationWinsReturnedValue(t *testing.T) {
	for i := 0; i < 1000; i++ {
		ctx, cancel := context.WithCancel(context.Background())
		outcome, joined := runHandler(ctx, func() (json.RawMessage, error) {
			cancel()
			return json.RawMessage(`99`), nil
		})
		cancel()
		if joined || !errors.Is(outcome.err, context.Canceled) || len(outcome.result) != 0 {
			t.Fatalf("cancelled completion was accepted: %+v joined=%t", outcome, joined)
		}
	}
}
