//go:build linux

package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"js-wf/assignment"
	"js-wf/client"
	"js-wf/identity"
	"js-wf/integrity"
	"js-wf/internal/natsutil"
	"js-wf/journal"
	"js-wf/lease"
	"js-wf/provision"
	"js-wf/testcluster"
	"js-wf/wf"
)

func phaseThreeCounterHandler(c *wf.Context, _ json.RawMessage) (json.RawMessage, error) {
	count := 0
	for step := 0; step < 50; step++ {
		next := count + 1
		value, err := wf.Run(c, "increment", count, func(ctx context.Context) (int, error) {
			timer := time.NewTimer(25 * time.Millisecond)
			defer timer.Stop()
			select {
			case <-ctx.Done():
				return 0, ctx.Err()
			case <-timer.C:
				return next, nil
			}
		})
		if err != nil {
			return nil, err
		}
		count = value
	}
	return json.Marshal(count)
}

type phaseThreeFault struct {
	Kind            string                        `json:"kind"`
	Scheduled       time.Time                     `json:"scheduled"`
	Applied         time.Time                     `json:"applied"`
	Confirmed       time.Time                     `json:"confirmed"`
	Healed          time.Time                     `json:"healed"`
	Slot            int                           `json:"slot"`
	Worker          string                        `json:"worker"`
	PID             int                           `json:"pid"`
	ActiveLeases    int                           `json:"active_leases"`
	SignalConfirmed bool                          `json:"signal_confirmed"`
	PausedLeases    []matrixPausedLease           `json:"paused_leases,omitempty"`
	ProxyBefore     *testcluster.ClientProxyStats `json:"proxy_before,omitempty"`
	ProxyBlocked    *testcluster.ClientProxyStats `json:"proxy_blocked,omitempty"`
}

type phaseThreeAssignmentMove struct {
	Partition    uint32    `json:"partition"`
	From         string    `json:"from"`
	To           string    `json:"to"`
	Revision     uint64    `json:"revision"`
	At           time.Time `json:"at"`
	ActiveLeases int       `json:"active_leases"`
}

type phaseThreeWorkerReport struct {
	MoveErrors  []string                   `json:"move_errors,omitempty"`
	Moves       []phaseThreeAssignmentMove `json:"moves,omitempty"`
	RouteCut    time.Time                  `json:"route_cut,omitempty"`
	RouteHeal   time.Time                  `json:"route_heal,omitempty"`
	RouteCounts [3]int                     `json:"route_counts"`
	Seed        int64                      `json:"seed"`
	Started     time.Time                  `json:"started"`
	CompletedAt time.Time                  `json:"completed_at"`
	Finished    time.Time                  `json:"finished"`
	Completed   int                        `json:"completed"`
	Workers     int                        `json:"workers"`
	Steps       int                        `json:"steps"`
	IDs         []string                   `json:"ids"`
	Faults      []phaseThreeFault          `json:"faults"`
	Integrity   integrity.Report           `json:"integrity"`
	Failed      bool                       `json:"failed"`
}

// The Phase3 cohort uses six actual worker processes and the production lease,
// journal and consumer paths. Fault holds are asynchronous so a 45-second pause
// does not turn the required two-second fault schedule into a single cut.
func TestPhaseThreeTwoHundredCountersWithRepeatedWorkerFaults(t *testing.T) {
	if os.Getenv("WF_PHASE3_REPEATED_FAULTS") != "1" {
		t.Skip("set WF_PHASE3_REPEATED_FAULTS=1 for the original 200/50/6 repeated-worker-fault proof")
	}
	runPhaseThreeRepeatedFaults(t, false)
}

func TestPhaseThreeCountersRebalanceWithRepeatedFaults(t *testing.T) {
	if os.Getenv("WF_PHASE3_REBALANCE_FAULTS") != "1" {
		t.Skip("set WF_PHASE3_REBALANCE_FAULTS=1 for four busy partitions moving during repeated process and route faults")
	}
	runPhaseThreeRepeatedFaults(t, true)
}

func runPhaseThreeRepeatedFaults(t *testing.T, rebalance bool) {
	t.Helper()
	seed := int64(1)
	if raw := os.Getenv("FAULT_SEED"); raw != "" {
		var err error
		seed, err = strconv.ParseInt(raw, 10, 64)
		if err != nil {
			t.Fatal(err)
		}
	}
	root := t.TempDir()
	if parent := os.Getenv("WF_PHASE3_ARTIFACT_PARENT"); parent != "" {
		if err := os.MkdirAll(parent, 0700); err != nil {
			t.Fatal(err)
		}
		var err error
		root, err = os.MkdirTemp(parent, "phase3-")
		if err != nil {
			t.Fatal(err)
		}
	}
	report := phaseThreeWorkerReport{Seed: seed, Workers: 6, Steps: 50}
	defer func() {
		report.Failed = t.Failed()
		report.Finished = time.Now().UTC()
		data, err := json.MarshalIndent(report, "", "  ")
		if err == nil {
			err = os.WriteFile(filepath.Join(root, "report.json"), data, 0600)
		}
		if err != nil {
			t.Error(err)
		}
	}()
	cluster, err := testcluster.Start(filepath.Join(root, "stores"), 3)
	if err != nil {
		t.Fatal(err)
	}
	all, cluster := setupCluster(t, cluster)
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	t.Setenv("WF_PHASE3_COUNTER_CHILD", "1")
	if rebalance {
		t.Setenv("WF_PHASE3_REBALANCE_CHILD", "1")
	}
	proxies := make([]*testcluster.ClientProxy, 6)
	fleet := make([]*matrixProcessWorker, 6)
	holds := make([]int, 6)
	for i := range holds {
		holds[i] = -1
	}
	defer func() {
		for i, p := range fleet {
			if proxies[i] != nil {
				proxies[i].Heal()
			}
			if p != nil {
				_ = p.cmd.Process.Signal(syscall.SIGCONT)
				stopMatrixProcessWorker(p)
			}
		}
		for _, p := range proxies {
			if p != nil {
				p.Close()
			}
		}
	}()
	for i := range fleet {
		proxies[i], err = testcluster.NewClientProxy(cluster.Servers[i%3].ClientURL())
		if err != nil {
			t.Fatal(err)
		}
		fleet[i], err = startMatrixProcessWorker(ctx, root, []string{proxies[i].URL()}, i, 0)
		if err != nil {
			t.Fatal(err)
		}
	}
	var assignments *assignment.Store
	if rebalance {
		assignments, err = assignment.New(ctx, all[0])
		if err != nil {
			t.Fatal(err)
		}
		for partition := uint32(0); partition < 4; partition++ {
			if _, err := assignments.Assign(ctx, partition, fleet[partition].id, 0); err != nil {
				t.Fatal(err)
			}
		}
	}
	partitionCounts := [4]int{}
	c := client.New(all[0])
	for candidate := 0; len(report.IDs) < 200; candidate++ {
		id := fmt.Sprintf("counter-%06d", candidate)
		partition := identity.Partition("phase3counter", id, provision.Partitions)
		if (!rebalance && partition == 0) || (rebalance && partition < 4 && partitionCounts[partition] < 50) {
			partitionCounts[partition]++
			report.IDs = append(report.IDs, id)
		}
	}
	report.Started = time.Now().UTC()
	cohort, stopCohort := context.WithDeadline(ctx, report.Started.Add(5*time.Minute))
	defer stopCohort()
	ticks := time.NewTicker(2 * time.Second)
	defer ticks.Stop()
	tickC := ticks.C
	cohortDone := cohort.Done()
	for _, id := range report.IDs {
		if _, err := c.Start(cohort, "phase3counter", id, []byte(`null`)); err != nil {
			t.Fatal(err)
		}
	}
	type counterResult struct {
		id    string
		value int
		err   error
		at    time.Time
	}
	results := make(chan counterResult, 200)
	go func() {
		for _, id := range report.IDs {
			raw, err := c.Await(cohort, "phase3counter", id)
			var value int
			if err == nil {
				err = json.Unmarshal(raw, &value)
			}
			results <- counterResult{id, value, err, time.Now().UTC()}
			if err != nil {
				return
			}
		}
	}()
	polling := time.NewTicker(50 * time.Millisecond)
	defer polling.Stop()
	rng := rand.New(rand.NewSource(seed))
	counts := map[string]int{}
	inFlight := map[string]int{}
	pendingHolds := 0
	type moveResult struct {
		move phaseThreeAssignmentMove
		err  error
	}
	moveResults := make(chan moveResult, 256)
	moveCtx, stopMoves := context.WithCancel(cohort)
	defer stopMoves()
	movesDone := make(chan struct{})
	if rebalance {
		owners := make([]string, len(fleet))
		for i, p := range fleet {
			owners[i] = p.id
		}
		go func() {
			defer close(movesDone)
			ticker := time.NewTicker(5 * time.Second)
			defer ticker.Stop()
			cycle := 0
			for {
				select {
				case <-moveCtx.Done():
					return
				case <-ticker.C:
				}
				cycle++
				for partition := uint32(0); partition < 4; partition++ {
					attempt, done := context.WithTimeout(moveCtx, time.Second)
					owner, revision, err := assignments.Get(attempt, partition)
					done()
					active := 0
					if err == nil {
						for slot, candidate := range owners {
							if candidate != owner {
								continue
							}
							paths, _ := filepath.Glob(filepath.Join(root, fmt.Sprintf("matrix-process-%d-generation-*-active.json", slot)))
							sort.Slice(paths, func(i, j int) bool {
								generation := func(p string) int {
									n, _ := strconv.Atoi(strings.TrimSuffix(strings.Split(p, "-generation-")[1], "-active.json"))
									return n
								}
								return generation(paths[i]) < generation(paths[j])
							})
							if len(paths) > 0 {
								var sample struct {
									Active int `json:"active_leases"`
								}
								if data, readErr := os.ReadFile(paths[len(paths)-1]); readErr == nil {
									if decodeErr := json.Unmarshal(data, &sample); decodeErr != nil {
										err = decodeErr
									}
									active = sample.Active
								}
							}
						}
					}
					to := owners[(cycle+int(partition))%len(owners)]
					if to == owner {
						to = owners[(cycle+int(partition)+1)%len(owners)]
					}
					var next uint64
					if err == nil {
						attempt, done = context.WithTimeout(moveCtx, time.Second)
						next, err = assignments.Assign(attempt, partition, to, revision)
						done()
					}
					result := moveResult{phaseThreeAssignmentMove{partition, owner, to, next, time.Now().UTC(), active}, err}
					select {
					case moveResults <- result:
					case <-moveCtx.Done():
						return
					}
				}
			}
		}()
	} else {
		close(movesDone)
	}
	defer func() { stopMoves(); <-movesDone }()
	defer cluster.RouteMesh().Heal()
	routeHeld := false
	for report.Completed < 200 || pendingHolds > 0 || routeHeld {
		if rebalance && routeHeld && time.Since(report.RouteCut) >= 45*time.Second {
			cluster.RouteMesh().Heal()
			report.RouteHeal = time.Now().UTC()
			routeHeld = false
		}
		for i, p := range fleet {
			select {
			case exitErr := <-p.exited:
				t.Fatalf("worker %s exited unexpectedly: %v; log=%s", p.id, exitErr, p.base+".log")
			default:
			}
			if holds[i] >= 0 && time.Since(report.Faults[holds[i]].Confirmed) >= 45*time.Second {
				event := &report.Faults[holds[i]]
				if event.Kind == "pause" {
					if err := p.cmd.Process.Signal(syscall.SIGCONT); err != nil {
						t.Fatal(err)
					}
					if err := waitMatrixProcessState(ctx, p.cmd.Process.Pid, false); err != nil {
						t.Fatal(err)
					}
				} else {
					proxies[i].Heal()
				}
				event.Healed = time.Now().UTC()
				holds[i] = -1
				pendingHolds--
			}
		}
		select {
		case result := <-results:
			if result.err != nil || result.value != 50 {
				t.Fatalf("result %s=%d err=%v", result.id, result.value, result.err)
			}
			report.Completed++
			if report.Completed == 200 {
				report.CompletedAt = result.at
				ticks.Stop()
				tickC = nil
				stopMoves()
				cohortDone = nil
			}
		case result := <-moveResults:
			if result.err != nil {
				if !errors.Is(result.err, context.DeadlineExceeded) && !errors.Is(result.err, context.Canceled) && !errors.Is(result.err, jetstream.ErrNoStreamResponse) && !errors.Is(result.err, assignment.ErrConflict) && !natsutil.IsUnavailable(result.err) {
					t.Fatalf("assignment move: %v", result.err)
				}
				report.MoveErrors = append(report.MoveErrors, result.err.Error())
			} else {
				report.Moves = append(report.Moves, result.move)
			}
		case scheduled := <-tickC:
			if rebalance && report.RouteCut.IsZero() && time.Since(report.Started) >= 10*time.Second {
				if err := cluster.RouteMesh().PartitionNode(2); err != nil {
					t.Fatal(err)
				}
				until := time.Now().Add(2 * time.Second)
				for cluster.Servers[2].NumRoutes() != 0 || cluster.Servers[0].NumRoutes() == 0 || cluster.Servers[1].NumRoutes() == 0 {
					if time.Now().After(until) {
						t.Fatal("route cut not observed")
					}
					time.Sleep(10 * time.Millisecond)
				}
				report.RouteCounts = [3]int{cluster.Servers[0].NumRoutes(), cluster.Servers[1].NumRoutes(), cluster.Servers[2].NumRoutes()}
				report.RouteCut = time.Now().UTC()
				routeHeld = true
			}
			available := []int{}
			active := []int{}
			activeCounts := map[int]int{}
			for i, p := range fleet {
				if holds[i] >= 0 {
					continue
				}
				available = append(available, i)
				var sample struct {
					Active int `json:"active_leases"`
				}
				if data, err := os.ReadFile(p.base + "-active.json"); err == nil {
					if err := json.Unmarshal(data, &sample); err != nil {
						t.Fatal(err)
					}
					if sample.Active > 0 {
						active = append(active, i)
						activeCounts[i] = sample.Active
					}
				}
			}
			if len(available) == 0 {
				t.Fatal("no unfaulted worker available")
			}
			eligible := available
			if len(active) > 0 {
				eligible = active
			}
			i := eligible[rng.Intn(len(eligible))]
			p := fleet[i]
			kind := []string{"kill", "pause", "partition"}[len(report.Faults)%3]
			// Preserve at least two executable members while long holds overlap. A
			// kill still executes in this slot and replaces the selected OS process.
			if len(available) <= 2 {
				kind = "kill"
			}
			event := phaseThreeFault{Kind: kind, Scheduled: scheduled.UTC(), Applied: time.Now().UTC(), Slot: i, Worker: p.id, PID: p.cmd.Process.Pid, ActiveLeases: activeCounts[i]}
			switch kind {
			case "kill":
				observation, err := killMatrixProcessWorker(ctx, root, []string{proxies[i].URL()}, fleet, i, scheduled)
				if err != nil {
					t.Fatal(err)
				}
				event.SignalConfirmed = observation.WorkerKillConfirmed
				event.Confirmed = observation.Killed.UTC()
				event.Healed = observation.Healed.UTC()
				if !event.SignalConfirmed {
					t.Fatal("worker SIGKILL not confirmed")
				}
			case "pause":
				if err := p.cmd.Process.Signal(syscall.SIGSTOP); err != nil {
					t.Fatal(err)
				}
				if err := waitMatrixProcessState(ctx, event.PID, true); err != nil {
					t.Fatal(err)
				}
				event.SignalConfirmed = true
				event.Confirmed = time.Now().UTC()
				keys, err := matrixWorkerActiveLeaseKeys(p.base + "-dispatch.jsonl")
				if err != nil {
					t.Fatal(err)
				}
				kv, err := all[0].KeyValue(ctx, "WF_LEASE")
				if err != nil {
					t.Fatal(err)
				}
				for _, key := range keys {
					entry, err := kv.Get(ctx, key)
					if err != nil {
						continue
					}
					var value lease.Value
					if err := json.Unmarshal(entry.Value(), &value); err != nil {
						t.Fatal(err)
					}
					if value.Worker == p.id && value.Epoch != 0 {
						event.PausedLeases = append(event.PausedLeases, matrixPausedLease{Key: key, Worker: value.Worker, Epoch: value.Epoch, Revision: entry.Revision(), Created: entry.Created(), Observed: time.Now().UTC()})
					}
				}
				holds[i] = len(report.Faults)
				pendingHolds++
			case "partition":
				before := proxies[i].Stats()
				event.ProxyBefore = &before
				proxies[i].Block()
				blocked := proxies[i].Stats()
				until := time.Now().Add(time.Second)
				for blocked.Active != 0 && time.Now().Before(until) && ctx.Err() == nil {
					time.Sleep(5 * time.Millisecond)
					blocked = proxies[i].Stats()
				}
				if before.Active == 0 || blocked.Active != 0 {
					t.Fatalf("worker partition not observed: before=%+v blocked=%+v", before, blocked)
				}
				event.ProxyBlocked = &blocked
				event.Confirmed = time.Now().UTC()
				holds[i] = len(report.Faults)
				pendingHolds++
			}
			counts[kind]++
			if event.ActiveLeases > 0 {
				inFlight[kind]++
			}
			report.Faults = append(report.Faults, event)
		case <-polling.C:
		case <-cohortDone:
			if report.Completed < 200 {
				t.Fatalf("five-minute completion target missed: completed=%d faults=%d: %v", report.Completed, len(report.Faults), cohort.Err())
			}
			t.Fatal("five-minute completion deadline reached")
		case <-ctx.Done():
			t.Fatal("fixture cleanup budget exhausted: ", ctx.Err())
		}
	}
	if rebalance {
		activeMoves := 0
		for _, move := range report.Moves {
			if move.ActiveLeases > 0 {
				activeMoves++
			}
		}
		if len(report.Moves) < 8 || activeMoves == 0 || report.RouteCut.IsZero() || report.RouteHeal.Sub(report.RouteCut) < 45*time.Second {
			t.Fatalf("combined rebalance evidence: moves=%d active=%d route_cut=%s route_heal=%s", len(report.Moves), activeMoves, report.RouteCut, report.RouteHeal)
		}
	}
	ownedPauses := 0
	for _, event := range report.Faults {
		if event.Kind == "pause" && len(event.PausedLeases) > 0 {
			ownedPauses++
		}
	}
	if ownedPauses == 0 {
		t.Fatal("no stopped worker had a confirmed live lease")
	}
	for _, kind := range []string{"kill", "pause", "partition"} {
		if counts[kind] == 0 || inFlight[kind] == 0 {
			t.Fatalf("missing active %s fault: counts=%v in_flight=%v", kind, counts, inFlight)
		}
	}
	for i, p := range fleet {
		if err := p.cmd.Process.Signal(syscall.SIGTERM); err != nil {
			t.Fatal(err)
		}
		select {
		case err := <-p.exited:
			if err != nil {
				t.Fatalf("worker %s graceful exit: %v; log=%s", p.id, err, p.base+".log")
			}
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
		fleet[i] = nil
	}
	store := journal.New(all[1])
	for _, id := range report.IDs {
		records, _, err := store.Read(ctx, "phase3counter", id)
		if err != nil {
			t.Fatal(err)
		}
		requested, completed := 0, 0
		for _, record := range records {
			if record.Kind == journal.StepRequested {
				requested++
			}
			if record.Kind == journal.StepCompleted {
				completed++
			}
		}
		if requested != 50 || completed != 50 {
			t.Fatalf("journal %s requested=%d completed=%d", id, requested, completed)
		}
		data, err := json.Marshal(records)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.MkdirAll(filepath.Join(root, "journals"), 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(root, "journals", id+".json"), data, 0600); err != nil {
			t.Fatal(err)
		}
		var outcome wf.Outcome
		if err := json.Unmarshal(records[len(records)-1].Payload, &outcome); err != nil {
			t.Fatal(err)
		}
		replayed, err := wf.Replay(data, func(c *wf.Context) (json.RawMessage, error) { return phaseThreeCounterHandler(c, nil) }, wf.ReplayOptions{Type: "phase3counter", ID: id, InvSeq: outcome.InvSeq})
		if err != nil || string(replayed) != "50" {
			t.Fatalf("offline replay %s=%s err=%v", id, replayed, err)
		}
	}
	report.Integrity, err = integrity.Check(ctx, all[2])
	if err != nil || report.Integrity.Invocations != 200 || report.Integrity.Journals != 200 || report.Integrity.Terminal != 200 {
		t.Fatalf("integrity=%+v err=%v", report.Integrity, err)
	}
	t.Logf("completed=%d steps=50 workers=6 fault_interval=2s faults=%v active_faults=%v elapsed=%s integrity=%+v artifacts=%s", report.Completed, counts, inFlight, report.CompletedAt.Sub(report.Started), report.Integrity, root)
}
