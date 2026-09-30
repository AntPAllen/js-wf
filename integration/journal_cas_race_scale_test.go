package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
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
	stream  jetstream.Stream
	arrived chan<- struct{}
	release <-chan struct{}
	reached atomic.Bool
}

func (g *casPublishGate) Stream(ctx context.Context, name string) (jetstream.Stream, error) {
	if name == "WF_JRN" && g.stream != nil {
		return g.stream, nil
	}
	return g.JetStream.Stream(ctx, name)
}

func (g *casPublishGate) Publish(ctx context.Context, subject string, payload []byte, opts ...jetstream.PublishOpt) (*jetstream.PubAck, error) {
	if subject == g.subject {
		first := g.reached.CompareAndSwap(false, true)
		if first {
			select {
			case g.arrived <- struct{}{}:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
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
	worker           string
	seq              uint64
	err              error
	preflightRetries int
}

type failOnceLastStream struct {
	jetstream.Stream
	failed atomic.Bool
}

func (s *failOnceLastStream) GetLastMsgForSubject(ctx context.Context, subject string) (*jetstream.RawStreamMsg, error) {
	if s.failed.CompareAndSwap(false, true) {
		return nil, context.DeadlineExceeded
	}
	return s.Stream.GetLastMsgForSubject(ctx, subject)
}

func TestCASRaceRetriesPrePublishTailLookup(t *testing.T) {
	all, _ := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	first, err := journal.New(all[0]).Append(ctx, "cas", "scale", journal.Entry{Epoch: 1, Index: 0, Kind: journal.Started}, 0)
	if err != nil {
		t.Fatal(err)
	}
	streams, err := casRaceStreams(ctx, [2]jetstream.JetStream{all[0], all[1]})
	if err != nil {
		t.Fatal(err)
	}
	failing := &failOnceLastStream{Stream: streams[0]}
	streams[0] = failing
	outcomes, err := casRaceRound(ctx, [2]jetstream.JetStream{all[0], all[1]}, streams, 1, first)
	if err != nil {
		t.Fatal(err)
	}
	var wins, stales, retries int
	for _, outcome := range outcomes {
		retries += outcome.preflightRetries
		if outcome.err == nil {
			wins++
		} else if errors.Is(outcome.err, journal.ErrStale) {
			stales++
		} else {
			t.Fatalf("writer %s: %v", outcome.worker, outcome.err)
		}
	}
	if !failing.failed.Load() || retries != 1 || wins != 1 || stales != 1 {
		t.Fatalf("pre-publish retry outcomes=%+v retries=%d", outcomes, retries)
	}
	journalRecords, _, err := journal.New(all[2]).Read(ctx, "cas", "scale")
	if err != nil || len(journalRecords) != 2 || journalRecords[1].Index != 1 {
		t.Fatalf("retained journal after retry: records=%v err=%v", journalRecords, err)
	}
}

func casRaceRound(ctx context.Context, all [2]jetstream.JetStream, streams [2]jetstream.Stream, index, expected uint64) ([2]casRoundOutcome, error) {
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
			var outcome casRoundOutcome
			outcome.worker = name
			for attempt := 0; attempt < 3; attempt++ {
				js := &casPublishGate{JetStream: all[writer], subject: "wf.jrn.cas.scale", stream: streams[writer], arrived: arrived, release: release}
				outcome.seq, outcome.err = journal.New(js).Append(ctx, "cas", "scale", journal.Entry{Epoch: 1, Index: index, Kind: kind, WorkerID: name}, expected)
				if js.reached.Load() || !errors.Is(outcome.err, journal.ErrUnknown) || ctx.Err() != nil {
					break
				}
				outcome.preflightRetries++
			}
			results <- outcome
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

// An unknown publish response cannot establish whether its CAS committed.
// Resolve only from the retained subject tail, then let the next round and
// final full-journal audit verify that no second physical append occurred.
func resolveCASRaceUnknown(ctx context.Context, all [2]jetstream.JetStream, index, expected uint64) (uint64, string, error) {
	until := time.Now().Add(5 * time.Second)
	var lastErr error
	var lastSeen uint64
	for {
		for _, js := range all {
			attempt, stop := context.WithTimeout(ctx, time.Second)
			stream, err := js.Stream(attempt, "WF_JRN")
			var message *jetstream.RawStreamMsg
			if err == nil {
				message, err = stream.GetLastMsgForSubject(attempt, "wf.jrn.cas.scale")
			}
			stop()
			if err != nil {
				lastErr = err
				continue
			}
			if message.Sequence == expected {
				lastSeen = message.Sequence
				continue
			}
			if message.Sequence != expected+1 {
				return 0, "", fmt.Errorf("round %d expected one physical append after %d, found tail %d", index, expected, message.Sequence)
			}
			var entry journal.Entry
			if err := json.Unmarshal(message.Data, &entry); err != nil {
				return 0, "", err
			}
			kind := journal.StepRequested
			if index%2 == 0 {
				kind = journal.StepCompleted
			}
			if entry.Index != index || entry.Epoch != 1 || entry.Kind != kind || entry.WorkerID != "a" && entry.WorkerID != "b" {
				return 0, "", fmt.Errorf("round %d unexpected retained winner: %+v", index, entry)
			}
			return message.Sequence, entry.WorkerID, nil
		}
		if time.Now().After(until) || ctx.Err() != nil {
			return 0, "", fmt.Errorf("round %d no retained winner after unknown reply at tail %d: last seen %d, last read: %v", index, expected, lastSeen, lastErr)
		}
		time.Sleep(25 * time.Millisecond)
	}
}

func TestCASRaceUnknownOutcomeResolvesRetainedWinner(t *testing.T) {
	all, _ := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	first, err := journal.New(all[0]).Append(ctx, "cas", "scale", journal.Entry{Epoch: 1, Index: 0, Kind: journal.Started}, 0)
	if err != nil {
		t.Fatal(err)
	}
	lost := &ackLossJS{JetStream: all[0], subject: "wf.jrn.cas.scale", lostErr: context.DeadlineExceeded}
	if _, err := journal.New(lost).Append(ctx, "cas", "scale", journal.Entry{Epoch: 1, Index: 1, Kind: journal.StepRequested, WorkerID: "a"}, first); !errors.Is(err, journal.ErrUnknown) || !lost.fired.Load() {
		t.Fatalf("hidden committed acknowledgment: fired=%v err=%v", lost.fired.Load(), err)
	}
	next, winner, err := resolveCASRaceUnknown(ctx, [2]jetstream.JetStream{all[1], all[2]}, 1, first)
	if err != nil || next != first+1 || winner != "a" {
		t.Fatalf("resolved committed winner: seq=%d want=%d worker=%q err=%v", next, first+1, winner, err)
	}
}

func casRaceStreams(ctx context.Context, all [2]jetstream.JetStream) ([2]jetstream.Stream, error) {
	var streams [2]jetstream.Stream
	for node, js := range all {
		until := time.Now().Add(5 * time.Second)
		for {
			attempt, stop := context.WithTimeout(ctx, time.Second)
			stream, err := js.Stream(attempt, "WF_JRN")
			stop()
			if err == nil {
				streams[node] = stream
				break
			}
			if ctx.Err() != nil || time.Now().After(until) {
				return streams, fmt.Errorf("node %d journal stream preflight: %w", node, err)
			}
			time.Sleep(25 * time.Millisecond)
		}
	}
	return streams, nil
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
	streams, err := casRaceStreams(ctx, [2]jetstream.JetStream{all[0], all[1]})
	if err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	var leaderKills int
	var unknownRounds int
	var preflightRetries int
	for index := uint64(1); index <= uint64(rounds); index++ {
		attempt, stop := context.WithTimeout(ctx, 15*time.Second)
		outcomes, err := casRaceRound(attempt, [2]jetstream.JetStream{all[0], all[1]}, streams, index, seq)
		stop()
		if err != nil {
			t.Fatalf("round %d expected_seq=%d outcomes=%+v: %v", index, seq, outcomes, err)
		}
		var wins, stales, unknowns int
		var next uint64
		for _, outcome := range outcomes {
			preflightRetries += outcome.preflightRetries
			switch {
			case outcome.err == nil:
				wins++
				next = outcome.seq
			case errors.Is(outcome.err, journal.ErrStale):
				stales++
			case errors.Is(outcome.err, journal.ErrUnknown):
				unknowns++
			default:
				t.Fatalf("round %d writer %s: %v", index, outcome.worker, outcome.err)
			}
		}
		if unknowns == 0 {
			if wins != 1 || stales != 1 || next <= seq {
				t.Fatalf("round %d expected_seq=%d outcomes=%+v wins=%d stales=%d", index, seq, outcomes, wins, stales)
			}
		} else {
			if wins > 1 {
				t.Fatalf("round %d two acknowledged winners: %+v", index, outcomes)
			}
			resolved, winner, err := resolveCASRaceUnknown(ctx, [2]jetstream.JetStream{all[0], all[1]}, index, seq)
			if err != nil || wins == 1 && next != resolved {
				t.Fatalf("round %d expected_seq=%d outcomes=%+v retained_seq=%d winner=%s err=%v", index, seq, outcomes, resolved, winner, err)
			}
			for _, outcome := range outcomes {
				if outcome.err == nil && outcome.worker != winner {
					t.Fatalf("round %d acknowledged winner %s differs from retained %s", index, outcome.worker, winner)
				}
			}
			next = resolved
			unknownRounds++
			t.Logf("round %d resolved %d unknown reply(s) to retained winner %s at %d", index, unknowns, winner, next)
		}
		seq = next
		if index%500 == 0 {
			if err := restartJournalLeader(ctx, &all, cluster, index+1); err != nil {
				t.Fatalf("after round %d: %v", index, err)
			}
			streams, err = casRaceStreams(ctx, [2]jetstream.JetStream{all[0], all[1]})
			if err != nil {
				t.Fatalf("after round %d refresh journal handles: %v", index, err)
			}
			leaderKills++
		}
	}
	// Audit every retained CAS winner through an independent ordered consumer.
	// The production reconstructed journal read is checked separately below.
	auditCtx, stopAudit := context.WithTimeout(context.Background(), 3*time.Minute)
	defer stopAudit()
	stream, err := all[0].Stream(auditCtx, "WF_JRN")
	if err != nil {
		t.Fatal(err)
	}
	info, err := stream.Info(auditCtx)
	if err != nil || info.State.Msgs != uint64(rounds+1) || info.State.NumSubjects != 1 {
		t.Fatalf("retained journal: info=%+v err=%v", info, err)
	}
	openAudit := func(from uint64) (casAuditCursor, error) {
		var auditErr error
		for attempt := 0; attempt < 18 && auditCtx.Err() == nil; attempt++ {
			node := attempt % len(all)
			request, stop := context.WithTimeout(auditCtx, 10*time.Second)
			candidate, openErr := all[node].Stream(request, "WF_JRN")
			if openErr == nil {
				created, createErr := candidate.OrderedConsumer(request, jetstream.OrderedConsumerConfig{
					FilterSubjects: []string{"wf.jrn.cas.scale"}, DeliverPolicy: jetstream.DeliverByStartSequencePolicy, OptStartSeq: from,
				})
				openErr = createErr
				if openErr == nil {
					stop()
					return created, nil
				}
			}
			stop()
			if !casAuditTransient(openErr) && !(errors.Is(openErr, context.DeadlineExceeded) && auditCtx.Err() == nil) {
				return nil, openErr
			}
			auditErr = openErr
			t.Logf("journal audit consumer attempt %d via node %d from sequence %d: %v", attempt+1, node, from, openErr)
		}
		return nil, fmt.Errorf("create journal audit consumer after trying all nodes: %w (context: %v)", auditErr, auditCtx.Err())
	}
	count, tail, err := auditCASJournal(auditCtx, rounds+1, openAudit)
	if err != nil {
		t.Fatalf("journal audit after %d entries at seq %d: %v", count, tail, err)
	}
	if tail != seq || count != rounds+1 {
		t.Fatalf("journal audit count=%d tail=%d want_count=%d want_tail=%d", count, tail, rounds+1, seq)
	}
	readCtx, stopRead := context.WithTimeout(context.Background(), 15*time.Second)
	readStarted := time.Now()
	records, readTail, err := journal.New(all[0]).Read(readCtx, "cas", "scale")
	stopRead()
	if err != nil || len(records) != rounds+1 || readTail != seq {
		t.Fatalf("production journal read after CAS races: records=%d tail=%d want=%d err=%v", len(records), readTail, seq, err)
	}
	t.Logf("production journal read of %d entries took %s", len(records), time.Since(readStarted))
	t.Logf("server-side CAS races=%d leader_kills=%d unknown_rounds=%d pre_publish_retries=%d elapsed=%s", rounds, leaderKills, unknownRounds, preflightRetries, time.Since(started))
}
