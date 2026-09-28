package integration_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"js-wf/journal"
	"js-wf/testcluster"

	"github.com/nats-io/nats.go/jetstream"
)

// casPublishGate lets both contenders read the same tail before either CAS
// publish reaches JetStream. The server, rather than the SDK's pre-read, then
// decides the winner of each round.
type casPublishGate struct {
	jetstream.JetStream
	subject string
	arrived chan<- struct{}
	release <-chan struct{}
}

func (g *casPublishGate) Publish(ctx context.Context, subject string, payload []byte, opts ...jetstream.PublishOpt) (*jetstream.PubAck, error) {
	if subject == g.subject {
		select {
		case g.arrived <- struct{}{}:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		select {
		case <-g.release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return g.JetStream.Publish(ctx, subject, payload, opts...)
}

type casRoundOutcome struct {
	worker string
	seq    uint64
	err    error
}

func casRaceRound(ctx context.Context, all [2]jetstream.JetStream, index, expected uint64) ([2]casRoundOutcome, error) {
	var outcomes [2]casRoundOutcome
	arrived := make(chan struct{}, 2)
	release := make(chan struct{})
	results := make(chan casRoundOutcome, 2)
	var wg sync.WaitGroup
	for writer := 0; writer < 2; writer++ {
		wg.Add(1)
		go func(writer int) {
			defer wg.Done()
			name := "a"
			if writer == 1 {
				name = "b"
			}
			kind := journal.StepRequested
			if index%2 == 0 {
				kind = journal.StepCompleted
			}
			js := &casPublishGate{JetStream: all[writer], subject: "wf.jrn.cas.scale", arrived: arrived, release: release}
			seq, err := journal.New(js).Append(ctx, "cas", "scale", journal.Entry{Epoch: 1, Index: index, Kind: kind, WorkerID: name}, expected)
			results <- casRoundOutcome{worker: name, seq: seq, err: err}
		}(writer)
	}
	ready := 0
	var early *casRoundOutcome
	for ready < 2 && early == nil {
		select {
		case <-arrived:
			ready++
		case outcome := <-results:
			early = &outcome
		case <-ctx.Done():
			close(release)
			wg.Wait()
			return outcomes, ctx.Err()
		}
	}
	close(release)
	if early != nil {
		outcomes[0] = *early
		outcomes[1] = <-results
		wg.Wait()
		return outcomes, fmt.Errorf("writer %s exited before the publish barrier: %v", early.worker, early.err)
	}
	outcomes[0], outcomes[1] = <-results, <-results
	wg.Wait()
	return outcomes, nil
}

func restartJournalLeader(ctx context.Context, all *[3]jetstream.JetStream, cluster *testcluster.Cluster, wantMessages uint64) error {
	prekillUntil := time.Now().Add(15 * time.Second)
	var info *jetstream.StreamInfo
	var err error
	for {
		attempt, stop := context.WithTimeout(ctx, time.Second)
		var stream jetstream.Stream
		stream, err = all[0].Stream(attempt, "WF_JRN")
		if err == nil {
			info, err = stream.Info(attempt)
		}
		stop()
		ready := err == nil && info != nil && info.Cluster != nil && info.Cluster.Leader != "" && info.State.Msgs == wantMessages && len(info.Cluster.Replicas) == 2
		if ready {
			for _, peer := range info.Cluster.Replicas {
				ready = ready && peer.Current && !peer.Offline
			}
		}
		if ready {
			break
		}
		if time.Now().After(prekillUntil) {
			return fmt.Errorf("journal replicas not current before kill: info=%+v err=%v", info, err)
		}
		time.Sleep(25 * time.Millisecond)
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
		return fmt.Errorf("unknown journal leader %q", oldLeader)
	}
	observer := (leader + 1) % 3
	attempt, stop := context.WithTimeout(ctx, 2*time.Second)
	survivor, err := all[observer].Stream(attempt, "WF_JRN")
	stop()
	if err != nil {
		return err
	}
	cluster.KillNode(leader)
	electionUntil := time.Now().Add(15 * time.Second)
	for {
		attempt, stop := context.WithTimeout(ctx, time.Second)
		current, infoErr := survivor.Info(attempt)
		stop()
		if infoErr == nil && current.Cluster != nil && current.Cluster.Leader != "" && current.Cluster.Leader != oldLeader && current.State.Msgs == wantMessages {
			break
		}
		if time.Now().After(electionUntil) {
			return fmt.Errorf("journal leader did not move from %s; last info=%+v err=%v", oldLeader, current, infoErr)
		}
		time.Sleep(25 * time.Millisecond)
	}
	if err := cluster.RestartNode(leader); err != nil {
		return err
	}
	all[leader], err = jetstream.New(cluster.Clients[leader])
	if err != nil {
		return err
	}
	catchupUntil := time.Now().Add(15 * time.Second)
	for {
		attempt, stop := context.WithTimeout(ctx, time.Second)
		current, infoErr := survivor.Info(attempt)
		stop()
		if infoErr == nil && current.Cluster != nil && current.State.Msgs == wantMessages && len(current.Cluster.Replicas) == 2 {
			allCurrent := true
			for _, peer := range current.Cluster.Replicas {
				allCurrent = allCurrent && peer.Current && !peer.Offline
			}
			if allCurrent {
				return nil
			}
		}
		if time.Now().After(catchupUntil) {
			return fmt.Errorf("journal replica did not catch up after restarting %s; last info=%+v err=%v", oldLeader, current, infoErr)
		}
		time.Sleep(25 * time.Millisecond)
	}
}

// This is the Phase 2 CAS proof: 10,000 server-side races, with the current
// WF_JRN leader killed and restarted after every 500 winning appends.
func TestJournalCASTenThousandRacesWithLeaderRestarts(t *testing.T) {
	rounds := 10000
	if raw := os.Getenv("WF_CAS_RACE_ROUNDS"); raw != "" {
		var err error
		rounds, err = strconv.Atoi(raw)
		if err != nil || rounds < 1 || rounds > 10000 {
			t.Fatalf("invalid WF_CAS_RACE_ROUNDS %q", raw)
		}
	}
	allSlice, cluster := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	var all [3]jetstream.JetStream
	copy(all[:], allSlice)
	seq, err := journal.New(all[0]).Append(ctx, "cas", "scale", journal.Entry{Epoch: 1, Index: 0, Kind: journal.Started}, 0)
	if err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	var leaderKills int
	for index := uint64(1); index <= uint64(rounds); index++ {
		attempt, stop := context.WithTimeout(ctx, 15*time.Second)
		outcomes, err := casRaceRound(attempt, [2]jetstream.JetStream{all[0], all[1]}, index, seq)
		stop()
		if err != nil {
			t.Fatalf("round %d expected_seq=%d outcomes=%+v: %v", index, seq, outcomes, err)
		}
		var wins, stales int
		var next uint64
		for _, outcome := range outcomes {
			switch {
			case outcome.err == nil:
				wins++
				next = outcome.seq
			case errors.Is(outcome.err, journal.ErrStale):
				stales++
			default:
				t.Fatalf("round %d writer %s: %v", index, outcome.worker, outcome.err)
			}
		}
		if wins != 1 || stales != 1 || next <= seq {
			t.Fatalf("round %d expected_seq=%d outcomes=%+v wins=%d stales=%d", index, seq, outcomes, wins, stales)
		}
		seq = next
		if index%500 == 0 {
			if err := restartJournalLeader(ctx, &all, cluster, index+1); err != nil {
				t.Fatalf("after round %d: %v", index, err)
			}
			leaderKills++
		}
	}
	stream, err := all[0].Stream(ctx, "WF_JRN")
	if err != nil {
		t.Fatal(err)
	}
	info, err := stream.Info(ctx)
	if err != nil || info.State.Msgs != uint64(rounds+1) || info.State.NumSubjects != 1 {
		t.Fatalf("retained journal: info=%+v err=%v", info, err)
	}
	records, tail, err := journal.New(all[0]).Read(ctx, "cas", "scale")
	if err != nil || len(records) != rounds+1 || tail != seq {
		t.Fatalf("journal read: records=%d tail=%d want_tail=%d err=%v", len(records), tail, seq, err)
	}
	for index, record := range records {
		if record.Index != uint64(index) || index > 0 && record.WorkerID != "a" && record.WorkerID != "b" {
			t.Fatalf("record %d: %+v", index, record)
		}
	}
	t.Logf("server-side CAS races=%d leader_kills=%d elapsed=%s", rounds, leaderKills, time.Since(started))
}
