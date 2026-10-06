package worker

import (
	"context"
	"encoding/json"
	"fmt"
)

type handlerOutcome struct {
	result   json.RawMessage
	err      error
	panicked bool
}

// runHandler lets a delivery stop waiting for user code after cancellation.
// A false joined result means the caller must not inspect the SDK context or
// journal buffers still owned by that goroutine. Its late result is discarded.
// Go cannot stop user code that ignores cancellation; external effects still
// need idempotency and every durable append retains its lease/context guards.
func runHandler(ctx context.Context, fn func() (json.RawMessage, error)) (handlerOutcome, bool) {
	if err := ctx.Err(); err != nil {
		return handlerOutcome{err: err}, false
	}
	done := make(chan handlerOutcome, 1)
	go func() {
		returned := false
		outcome := handlerOutcome{}
		defer func() {
			if p := recover(); p != nil {
				outcome.err, outcome.panicked = fmt.Errorf("workflow panic: %v", p), true
			} else if !returned {
				outcome.err, outcome.panicked = fmt.Errorf("workflow exited via runtime.Goexit"), true
			}
			done <- outcome
		}()
		outcome.result, outcome.err = fn()
		returned = true
	}()
	select {
	case outcome := <-done:
		return outcome, true
	case <-ctx.Done():
		return handlerOutcome{err: ctx.Err()}, false
	}
}
