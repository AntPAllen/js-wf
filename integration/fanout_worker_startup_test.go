package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"js-wf/internal/natsutil"
	"js-wf/worker"
)

type fanoutStartupAttempt struct {
	Started   time.Time `json:"started"`
	Finished  time.Time `json:"finished"`
	Error     string    `json:"error,omitempty"`
	Retryable bool      `json:"retryable"`
}

// Worker.New bounds each startup to five seconds. Metadata leaders may still
// be recovering after the journal alone catches up. The caller retries only
// recognized transient errors under the unchanged original case deadline.
func retryFanoutWorkerStartup(ctx context.Context, build func(context.Context) (*worker.Worker, error)) (*worker.Worker, []fanoutStartupAttempt, error) {
	var attempts []fanoutStartupAttempt
	for {
		if err := ctx.Err(); err != nil {
			return nil, attempts, err
		}
		attempt := fanoutStartupAttempt{Started: time.Now().UTC()}
		w, err := build(ctx)
		if ctx.Err() != nil && err == nil {
			err = ctx.Err()
			if w != nil {
				err = errors.Join(err, w.Close())
			}
		}
		attempt.Finished = time.Now().UTC()
		if err != nil {
			attempt.Error = err.Error()
		}
		attempt.Retryable = err != nil && ctx.Err() == nil && (errors.Is(err, context.DeadlineExceeded) || errors.Is(err, nats.ErrTimeout) || errors.Is(err, nats.ErrNoResponders) || natsutil.IsUnavailable(err))
		attempts = append(attempts, attempt)
		if err == nil {
			return w, attempts, nil
		}
		if ctx.Err() != nil {
			return nil, attempts, ctx.Err()
		}
		if !attempt.Retryable {
			return nil, attempts, err
		}
		timer := time.NewTimer(100 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, attempts, ctx.Err()
		case <-timer.C:
		}
	}
}

func newFanoutWorkerAfterRestart(t *testing.T, ctx context.Context, js jetstream.JetStream, id string, handlers map[string]worker.Handler, root string, options ...worker.Option) (*worker.Worker, error) {
	t.Helper()
	w, attempts, err := retryFanoutWorkerStartup(ctx, func(attempt context.Context) (*worker.Worker, error) {
		return worker.New(attempt, js, id, handlers, options...)
	})
	if root != "" {
		deadline, _ := ctx.Deadline()
		data, marshalErr := json.MarshalIndent(map[string]any{"worker": id, "attempts": attempts, "original_case_deadline": deadline, "error": fmt.Sprint(err), "scope": "bounded Worker.New attempts; retries only typed transient errors within original case context"}, "", "  ")
		if marshalErr == nil {
			marshalErr = os.WriteFile(filepath.Join(root, "worker-start-"+id+".json"), append(data, '\n'), 0600)
		}
		if marshalErr != nil {
			return nil, marshalErr
		}
	}
	t.Logf("FANOUT_WORKER_START worker=%s attempts=%d error=%v", id, len(attempts), err)
	return w, err
}
