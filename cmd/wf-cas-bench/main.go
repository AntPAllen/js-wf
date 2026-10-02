// wf-cas-bench measures journal.Append on a three-replica file-backed cluster.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"runtime"
	"strconv"
	"sync"
	"time"

	"js-wf/journal"
	"js-wf/provision"
	"js-wf/testcluster"

	"github.com/nats-io/nats.go/jetstream"
)

type result struct {
	Invocations int     `json:"invocations"`
	EntriesEach int     `json:"entries_each"`
	Appends     int     `json:"appends"`
	ElapsedMS   float64 `json:"elapsed_ms"`
	AppendsSec  float64 `json:"appends_per_second"`
}

type topology struct {
	Leader         string `json:"leader"`
	ClientServer   string `json:"client_server"`
	ClientIsLeader bool   `json:"client_is_leader"`
}

type report struct {
	NATSServerVersion string   `json:"nats_server_version"`
	GoVersion         string   `json:"go_version"`
	Replicas          int      `json:"replicas"`
	Storage           string   `json:"storage"`
	ParallelWorkers   int      `json:"parallel_workers"`
	Hot               result   `json:"hot"`
	Parallel          result   `json:"parallel"`
	JournalMessages   uint64   `json:"journal_messages"`
	JournalSubjects   uint64   `json:"journal_subjects"`
	HotPlacement      string   `json:"hot_placement"`
	HotBefore         topology `json:"hot_topology_before"`
	HotAfter          topology `json:"hot_topology_after"`
}

func main() {
	hotCount := flag.Int("hot", 10000, "entries in one hot invocation")
	parallelCount := flag.Int("invocations", 1000, "distinct parallel invocations")
	entriesEach := flag.Int("entries", 100, "entries per parallel invocation")
	workers := flag.Int("workers", 1000, "maximum concurrent parallel invocations")
	output := flag.String("output", "", "write JSON report to this file")
	baseline := flag.String("baseline", "", "compare throughput with a baseline JSON report")
	minRatio := flag.Float64("min-ratio", 0.8, "minimum allowed fraction of baseline throughput")
	placement := flag.String("hot-placement", "pinned", "hot client placement: pinned (node 0), leader, or follower")
	probe := flag.Bool("probe-hot-topology", false, "diagnostic only: measure hot appends through each pinned node on one cluster")
	root := flag.String("root", "", "file store directory; default temporary")
	flag.Parse()
	if *placement != "pinned" && *placement != "leader" && *placement != "follower" {
		fmt.Fprintln(os.Stderr, "invalid hot placement")
		os.Exit(2)
	}
	if *hotCount < 1 || *hotCount > journal.MaxEntries || *parallelCount < 1 || *entriesEach < 1 || *entriesEach > journal.MaxEntries || *workers < 1 || *workers > *parallelCount || *minRatio <= 0 || *minRatio > 1 {
		fmt.Fprintln(os.Stderr, "invalid benchmark sizes")
		os.Exit(2)
	}
	if *root == "" {
		var err error
		*root, err = os.MkdirTemp("", "wf-cas-bench-")
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		defer os.RemoveAll(*root)
	}
	if *probe {
		if *baseline != "" {
			fmt.Fprintln(os.Stderr, "topology probe cannot compare release throughput")
			os.Exit(2)
		}
		rep, err := runTopologyProbe(*root, *hotCount)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		data, err := json.MarshalIndent(rep, "", "  ")
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		data = append(data, '\n')
		if *output != "" {
			if err := os.WriteFile(*output, data, 0644); err != nil {
				fmt.Fprintln(os.Stderr, err)
				os.Exit(1)
			}
		}
		_, _ = os.Stdout.Write(data)
		return
	}
	rep, err := runWithPlacement(*root, *hotCount, *parallelCount, *entriesEach, *workers, *placement)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	data, err := json.MarshalIndent(rep, "", "  ")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	data = append(data, '\n')
	if *output != "" {
		if err := os.WriteFile(*output, data, 0644); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	}
	_, _ = os.Stdout.Write(data)
	if *baseline != "" {
		baselineData, err := os.ReadFile(*baseline)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		var previous report
		if err := json.Unmarshal(baselineData, &previous); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		if err := compareBaseline(rep, previous, *minRatio); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	}
}

func compareBaseline(current, baseline report, minRatio float64) error {
	if minRatio <= 0 || minRatio > 1 {
		return fmt.Errorf("invalid minimum ratio %g", minRatio)
	}
	currentPlacement, baselinePlacement := current.HotPlacement, baseline.HotPlacement
	if currentPlacement == "" {
		currentPlacement = "pinned"
	}
	if baselinePlacement == "" {
		baselinePlacement = "pinned"
	}
	if currentPlacement != baselinePlacement || current.NATSServerVersion != baseline.NATSServerVersion || current.GoVersion != baseline.GoVersion || current.Replicas != baseline.Replicas || current.Storage != baseline.Storage || current.ParallelWorkers != baseline.ParallelWorkers || current.Hot.Invocations != baseline.Hot.Invocations || current.Hot.EntriesEach != baseline.Hot.EntriesEach || current.Parallel.Invocations != baseline.Parallel.Invocations || current.Parallel.EntriesEach != baseline.Parallel.EntriesEach {
		return fmt.Errorf("baseline workload or runtime differs from current run")
	}
	for _, check := range []struct {
		name string
		now  float64
		base float64
	}{{"hot", current.Hot.AppendsSec, baseline.Hot.AppendsSec}, {"parallel", current.Parallel.AppendsSec, baseline.Parallel.AppendsSec}} {
		if check.base <= 0 || check.now < check.base*minRatio {
			return fmt.Errorf("%s throughput %.1f appends/s is below %.0f%% of baseline %.1f appends/s", check.name, check.now, minRatio*100, check.base)
		}
	}
	return nil
}

func run(root string, hotCount, parallelCount, entriesEach, workers int) (report, error) {
	return runWithPlacement(root, hotCount, parallelCount, entriesEach, workers, "pinned")
}

func runWithPlacement(root string, hotCount, parallelCount, entriesEach, workers int, placement string) (report, error) {
	var rep report
	c, err := testcluster.Start(root, 3)
	if err != nil {
		return rep, err
	}
	defer c.Close()
	var all [3]jetstream.JetStream
	for i, nc := range c.Clients {
		all[i], err = jetstream.New(nc)
		if err != nil {
			return rep, err
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	defer cancel()
	readyUntil := time.Now().Add(30 * time.Second)
	for {
		attempt, stop := context.WithTimeout(ctx, 3*time.Second)
		err := provision.Ensure(attempt, all[0], 3)
		stop()
		if err == nil {
			break
		}
		if time.Now().After(readyUntil) {
			return rep, fmt.Errorf("provision: %w", err)
		}
		time.Sleep(50 * time.Millisecond)
	}
	rep.NATSServerVersion = c.Clients[0].ConnectedServerVersion()
	rep.GoVersion = runtime.Version()
	rep.Replicas, rep.Storage, rep.ParallelWorkers = 3, "file", workers
	stores := [3]*journal.Store{journal.New(all[0]), journal.New(all[1]), journal.New(all[2])}
	node := 0
	rep.HotPlacement = placement
	initial, err := captureTopology(ctx, all[0], c.Clients[0].ConnectedServerName())
	if err != nil {
		return rep, err
	}
	if placement != "pinned" {
		node = -1
		for i, nc := range c.Clients {
			if (nc.ConnectedServerName() == initial.Leader) == (placement == "leader") {
				node = i
				break
			}
		}
		if node < 0 {
			return rep, fmt.Errorf("cannot locate %s client", placement)
		}
	}
	rep.HotBefore, err = captureTopology(ctx, all[node], c.Clients[node].ConnectedServerName())
	if err != nil {
		return rep, err
	}
	if placement != "pinned" && rep.HotBefore.ClientIsLeader != (placement == "leader") {
		return rep, fmt.Errorf("hot placement changed before sample")
	}
	rep.Hot, err = appendJournal(ctx, stores[node], all[node], "hot", hotCount)
	if err != nil {
		return rep, err
	}
	rep.HotAfter, err = captureTopology(ctx, all[node], c.Clients[node].ConnectedServerName())
	if err != nil {
		return rep, err
	}
	if placement != "pinned" && rep.HotBefore != rep.HotAfter {
		return rep, fmt.Errorf("hot topology changed across sample")
	}
	fmt.Fprintf(os.Stderr, "hot: %.1f appends/s\n", rep.Hot.AppendsSec)
	rep.Parallel, err = appendParallel(ctx, stores, all, parallelCount, entriesEach, workers)
	if err != nil {
		return rep, err
	}
	fmt.Fprintf(os.Stderr, "parallel: %.1f appends/s\n", rep.Parallel.AppendsSec)
	stream, err := all[0].Stream(ctx, "WF_JRN")
	if err != nil {
		return rep, err
	}
	info, err := stream.Info(ctx)
	if err != nil {
		return rep, err
	}
	rep.JournalMessages, rep.JournalSubjects = info.State.Msgs, info.State.NumSubjects
	wantMsgs := uint64(hotCount + parallelCount*entriesEach)
	wantSubjects := uint64(parallelCount + 1)
	if rep.JournalMessages != wantMsgs || rep.JournalSubjects != wantSubjects {
		return rep, fmt.Errorf("journal counts: messages=%d/%d subjects=%d/%d", rep.JournalMessages, wantMsgs, rep.JournalSubjects, wantSubjects)
	}
	return rep, nil
}

func entry(index int) journal.Entry {
	kind := journal.Started
	if index > 0 {
		kind = journal.StepRequested
		if index%2 == 0 {
			kind = journal.StepCompleted
		}
	}
	return journal.Entry{Epoch: 1, Index: uint64(index), Kind: kind, WorkerID: "bench"}
}

func appendJournal(ctx context.Context, store *journal.Store, js jetstream.JetStream, id string, count int) (result, error) {
	start := time.Now()
	var seq uint64
	for index := 0; index < count; index++ {
		attempt, stop := context.WithTimeout(ctx, 10*time.Second)
		next, err := store.Append(attempt, "bench", id, entry(index), seq)
		stop()
		if err != nil {
			if ctx.Err() != nil {
				return result{}, err
			}
			return result{}, diagnoseAppend(js, id, index, seq, err)
		}
		seq = next
	}
	duration := time.Since(start)
	return result{Invocations: 1, EntriesEach: count, Appends: count, ElapsedMS: float64(duration) / float64(time.Millisecond), AppendsSec: float64(count) / duration.Seconds()}, nil
}

func diagnoseAppend(js jetstream.JetStream, id string, index int, expectedSeq uint64, appendErr error) error {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	stream, err := js.Stream(ctx, "WF_JRN")
	if err != nil {
		return fmt.Errorf("journal %s index %d expected_seq=%d: %w; diagnostic stream: %v", id, index, expectedSeq, appendErr, err)
	}
	last, lastErr := stream.GetLastMsgForSubject(ctx, "wf.jrn.bench."+id)
	info, infoErr := stream.Info(ctx)
	var actualSeq, actualIndex uint64
	if lastErr == nil {
		actualSeq = last.Sequence
		var e journal.Entry
		if journal.UnmarshalEntry(last.Data, &e) == nil {
			actualIndex = e.Index
		}
	}
	leader := ""
	if infoErr == nil && info.Cluster != nil {
		leader = info.Cluster.Leader
	}
	return fmt.Errorf("journal %s index %d expected_seq=%d: %w; last_seq=%d last_index=%d last_err=%v leader=%s info_err=%v", id, index, expectedSeq, appendErr, actualSeq, actualIndex, lastErr, leader, infoErr)
}

func appendParallel(ctx context.Context, stores [3]*journal.Store, all [3]jetstream.JetStream, count, entriesEach, workers int) (result, error) {
	jobs := make(chan int, workers)
	startGate := make(chan struct{})
	var wg sync.WaitGroup
	var firstErr error
	var once sync.Once
	stage, cancel := context.WithCancel(ctx)
	defer cancel()
	for worker := 0; worker < workers; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			<-startGate
			for id := range jobs {
				_, err := appendJournal(stage, stores[worker%3], all[worker%3], "p"+strconv.Itoa(id), entriesEach)
				if err != nil {
					once.Do(func() { firstErr = err; cancel() })
					return
				}
			}
		}(worker)
	}
	start := time.Now()
	close(startGate)
producer:
	for id := 0; id < count; id++ {
		select {
		case jobs <- id:
		case <-stage.Done():
			break producer
		}
	}
	close(jobs)
	wg.Wait()
	if firstErr != nil {
		return result{}, firstErr
	}
	if err := ctx.Err(); err != nil {
		return result{}, err
	}
	duration := time.Since(start)
	appends := count * entriesEach
	return result{Invocations: count, EntriesEach: entriesEach, Appends: appends, ElapsedMS: float64(duration) / float64(time.Millisecond), AppendsSec: float64(appends) / duration.Seconds()}, nil
}
