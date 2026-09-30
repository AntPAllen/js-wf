//go:build linux

package integration_test

import (
	"context"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"js-wf/testcluster"
)

func stallMatrixBlockDisk(ctx context.Context, nc *nats.Conn, js jetstream.JetStream, disk *testcluster.BlockDisk, scheduled time.Time) (matrixLeaderFault, error) {
	event := matrixLeaderFault{Scheduled: scheduled, Node: 2}
	bound, done := context.WithTimeout(ctx, 30*time.Second)
	defer done()
	// Move the workflow leaders back to the stalled filesystem before each cut;
	// a prior stall may have elected a different leader.
	if err := preferMatrixNodeTwoLeaders(bound, nc, js); err != nil {
		return event, err
	}
	proof, err := disk.Stall(bound, 5*time.Second)
	event.BlockStall = &proof
	event.Killed = proof.Suspended
	if err != nil {
		return event, err
	}
	if proof.Resumed.Sub(proof.Suspended) < 5*time.Second || proof.SyncReturned.Sub(proof.Suspended) < 5*time.Second {
		return event, fmt.Errorf("unverified five-second block stall: %+v", proof)
	}
	if err := waitMatrixWorkflowReplicas(bound, js); err != nil {
		return event, err
	}
	event.Healed = time.Now()
	return event, nil
}
