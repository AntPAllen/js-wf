package worker

import (
	"context"
	"errors"
	"time"

	"js-wf/client"
)

// Bound the complete create/read/enqueue sequence, including client retries.
// An uncertain outcome leaves the durable child request for a later delivery
// to retry with the same child identity and parent invocation generation.
const childStartBudget = 5 * time.Second

func (w *Worker) startChild(ctx context.Context, childType, childID string, input []byte, parentType, parentID string, parentInvSeq uint64, signalName string, ops *deliveryOperations) error {
	attempt, stop := context.WithTimeout(ctx, childStartBudget)
	defer stop()
	started := ops.begin()
	_, err := w.client.StartChild(attempt, childType, childID, input, parentType, parentID, parentInvSeq, signalName)
	ops.finish(started, "child_start", 0, "", err)
	if errors.Is(err, client.ErrAlreadyStarted) {
		return nil
	}
	return err
}
