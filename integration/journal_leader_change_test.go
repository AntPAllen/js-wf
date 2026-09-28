package integration_test

import (
	"context"
	"testing"
	"time"

	"js-wf/journal"

	"github.com/nats-io/nats.go/jetstream"
)

// TestJournalLeaderChangeBetweenReadAndCAS holds a single writer after its
// tail read, moves the stream leader, then submits the unchanged expected
// sequence. A leader change alone must not make that sequence stale.
func TestJournalLeaderChangeBetweenReadAndCAS(t *testing.T) {
	all, cluster := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	first, err := journal.New(all[0]).Append(ctx, "cas", "leader-change", journal.Entry{Epoch: 1, Index: 0, Kind: journal.Started}, 0)
	if err != nil {
		t.Fatal(err)
	}
	stream, err := all[0].Stream(ctx, "WF_JRN")
	if err != nil {
		t.Fatal(err)
	}
	info, err := stream.Info(ctx)
	if err != nil || info.Cluster == nil || info.Cluster.Leader == "" {
		t.Fatalf("journal leader: info=%+v err=%v", info, err)
	}
	oldLeader := info.Cluster.Leader
	leader := -1
	for i, s := range cluster.Servers {
		if s.Name() == oldLeader {
			leader = i
			break
		}
	}
	if leader < 0 {
		t.Fatalf("unknown leader %q", oldLeader)
	}
	writer := (leader + 1) % 3
	survivor, err := all[writer].Stream(ctx, "WF_JRN")
	if err != nil {
		t.Fatal(err)
	}
	arrived := make(chan struct{}, 1)
	release := make(chan struct{})
	released := false
	defer func() {
		if !released {
			close(release)
		}
	}()
	type outcome struct {
		seq uint64
		err error
	}
	done := make(chan outcome, 1)
	go func() {
		gated := &casPublishGate{JetStream: all[writer], subject: "wf.jrn.cas.leader-change", arrived: arrived, release: release}
		seq, err := journal.New(gated).Append(ctx, "cas", "leader-change", journal.Entry{Epoch: 1, Index: 1, Kind: journal.StepRequested, WorkerID: "single"}, first)
		done <- outcome{seq, err}
	}()
	select {
	case <-arrived:
	case result := <-done:
		t.Fatalf("writer exited before publish barrier: %+v", result)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	prekillUntil := time.Now().Add(10 * time.Second)
	for {
		attempt, stop := context.WithTimeout(ctx, time.Second)
		current, infoErr := survivor.Info(attempt)
		stop()
		ready := infoErr == nil && current.Cluster != nil && current.State.Msgs == 1 && len(current.Cluster.Replicas) == 2
		if ready {
			for _, peer := range current.Cluster.Replicas {
				ready = ready && peer.Current && !peer.Offline
			}
		}
		if ready {
			break
		}
		if time.Now().After(prekillUntil) {
			t.Fatalf("replicas not current before leader kill: info=%+v err=%v", current, infoErr)
		}
		time.Sleep(25 * time.Millisecond)
	}
	cluster.KillNode(leader)
	deadline := time.Now().Add(15 * time.Second)
	var newLeader string
	for {
		attempt, stop := context.WithTimeout(ctx, time.Second)
		current, infoErr := survivor.Info(attempt)
		stop()
		if infoErr == nil && current.Cluster != nil && current.Cluster.Leader != "" && current.Cluster.Leader != oldLeader && current.State.Msgs == 1 {
			newLeader = current.Cluster.Leader
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("leader did not move from %s: info=%+v err=%v", oldLeader, current, infoErr)
		}
		time.Sleep(25 * time.Millisecond)
	}
	close(release)
	released = true
	var result outcome
	select {
	case result = <-done:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if result.err != nil || result.seq <= first {
		last, lastErr := survivor.GetLastMsgForSubject(ctx, "wf.jrn.cas.leader-change")
		t.Fatalf("append after leader %s -> %s: expected=%d result=%+v last=%+v last_err=%v", oldLeader, newLeader, first, result, last, lastErr)
	}
	if err := cluster.RestartNode(leader); err != nil {
		t.Fatal(err)
	}
	all[leader], err = jetstream.New(cluster.Clients[leader])
	if err != nil {
		t.Fatal(err)
	}
	catchupUntil := time.Now().Add(15 * time.Second)
	for {
		attempt, stop := context.WithTimeout(ctx, time.Second)
		current, infoErr := survivor.Info(attempt)
		stop()
		if infoErr == nil && current.Cluster != nil && current.State.Msgs == 2 && len(current.Cluster.Replicas) == 2 && current.Cluster.Replicas[0].Current && current.Cluster.Replicas[1].Current {
			break
		}
		if time.Now().After(catchupUntil) {
			t.Fatalf("restarted replica not current: info=%+v err=%v", current, infoErr)
		}
		time.Sleep(25 * time.Millisecond)
	}
	records, tail, err := journal.New(all[leader]).Read(ctx, "cas", "leader-change")
	if err != nil || len(records) != 2 || tail != result.seq || records[1].WorkerID != "single" {
		t.Fatalf("journal after leader change: records=%v tail=%d err=%v", records, tail, err)
	}
	t.Logf("leader %s -> %s, append sequence %d", oldLeader, newLeader, result.seq)
}
