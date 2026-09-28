package integration_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"js-wf/identity"
	"js-wf/integrity"
	"js-wf/journal"

	"github.com/nats-io/nats.go"
)

func TestIntegrityRejectsTwoWorkersInOneEpoch(t *testing.T) {
	all, _ := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if _, err := all[0].PublishMsg(ctx, &nats.Msg{Subject: identity.InvocationSubject("test", "shared-epoch"), Data: []byte(`null`)}); err != nil {
		t.Fatal(err)
	}
	j := journal.New(all[0])
	seq, err := j.Append(ctx, "test", "shared-epoch", journal.Entry{Index: 0, Kind: journal.Started}, 0)
	if err != nil {
		t.Fatal(err)
	}
	seq, err = j.Append(ctx, "test", "shared-epoch", journal.Entry{Epoch: 1, Index: 1, Kind: journal.StepRequested, WorkerID: "worker-a"}, seq)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := j.Append(ctx, "test", "shared-epoch", journal.Entry{Epoch: 1, Index: 2, Kind: journal.StepCompleted, WorkerID: "worker-b"}, seq); err != nil {
		t.Fatal(err)
	}
	if _, err := integrity.Check(ctx, all[1]); err == nil || !strings.Contains(err.Error(), "epoch 1 used by workers") {
		t.Fatalf("shared epoch survived real-state checker: %v", err)
	}
}
