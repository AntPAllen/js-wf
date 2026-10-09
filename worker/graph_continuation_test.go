package worker

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"js-wf/client"
	"js-wf/internal/checkpoint"
	"js-wf/journal"
	"js-wf/lease"
	"js-wf/provision"
	"js-wf/reconcile"
	"js-wf/testcluster"
	"js-wf/wf"
)

// Admission remains closed; exercise the production handoff boundary directly
// while compaction and full stage execution are still being migrated.
func TestNativeGraphContinuationHandoff(t *testing.T) {
	for _, domain := range []string{"", "WFCONTINUATION"} {
		name := "R1"
		if domain != "" {
			name = "R3Domain"
		}
		t.Run(name, func(t *testing.T) { testNativeGraphContinuationHandoff(t, domain, false) })
	}
}
func TestNativeGraphUnpublishedContinuationHandoff(t *testing.T) {
	for _, domain := range []string{"", "WFCONTINUATION"} {
		name := "R1"
		if domain != "" {
			name = "R3Domain"
		}
		t.Run(name, func(t *testing.T) { testNativeGraphContinuationHandoff(t, domain, true) })
	}
}
func testNativeGraphContinuationHandoff(t *testing.T, domain string, unpublished bool) {
	replicas := 1
	if domain != "" {
		replicas = 3
	}
	var cluster *testcluster.Cluster
	var err error
	if domain == "" {
		cluster, err = testcluster.Start(t.TempDir(), replicas)
	} else {
		cluster, err = testcluster.StartWithDomain(t.TempDir(), replicas, domain)
	}
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if replicas > 1 {
		for ctx.Err() == nil {
			ready := false
			for _, server := range cluster.Servers {
				ready = ready || server.JetStreamIsLeader() && len(server.JetStreamClusterPeers()) == replicas
			}
			if ready {
				break
			}
			time.Sleep(20 * time.Millisecond)
		}
	}
	var js jetstream.JetStream
	if domain == "" {
		js, err = jetstream.New(cluster.Clients[0])
	} else {
		js, err = jetstream.NewWithDomain(cluster.Clients[0], domain)
	}
	if err != nil {
		t.Fatal(err)
	}
	if err := provision.Ensure(ctx, js, replicas); err != nil {
		t.Fatal(err)
	}
	cfg := journal.NativeGraphConfig{AuthorityStream: "CONTINUE_AUTH", AuthorityPrefix: "wf.graph.continue", ObjectBucket: "CONTINUE_OBJECTS", ExpectedReplicas: replicas, CanonicalStarts: true, CanonicalSignals: true, CheckpointIndex: true}
	configs, err := journal.NativeGraphStreamConfigs(cfg, replicas)
	if err != nil {
		t.Fatal(err)
	}
	for _, config := range configs {
		if _, err := js.CreateStream(ctx, config); err != nil {
			t.Fatal(err)
		}
	}
	graph, err := journal.OpenNativeGraphStore(ctx, js, cfg)
	if err != nil {
		t.Fatal(err)
	}
	c, err := client.New(js).WithGraphJournal(graph)
	if err != nil {
		t.Fatal(err)
	}
	h, err := c.Start(ctx, "test", "continue", []byte(`7`))
	if err != nil {
		t.Fatal(err)
	}
	leases, err := lease.New(ctx, js)
	if err != nil {
		t.Fatal(err)
	}
	owner, err := leases.Acquire(ctx, h.Type, h.ID, "handoff")
	if err != nil {
		t.Fatal(err)
	}
	defer owner.Release(context.Background())
	status, err := graph.InspectStart(ctx, h.Type, h.ID)
	if err != nil {
		t.Fatal(err)
	}
	locals := json.RawMessage(`{"count":1}`)
	digest := sha256.Sum256(locals)
	frame := checkpoint.Frame{Version: checkpoint.Version, Identity: checkpoint.Identity{Type: h.Type, ID: h.ID, InvSeq: h.InvSeq}, Stage: "next", Data: locals, Anchor: checkpoint.Anchor{Index: 2, Epoch: owner.Epoch()}, StepPosition: 2, State: map[string]json.RawMessage{"value": json.RawMessage(`42`)}}
	encoded, hash, err := checkpoint.Encode(frame)
	if err != nil {
		t.Fatal(err)
	}
	started, _ := json.Marshal(map[string]string{"input_sha256": status.State.Start.InputSHA256})
	request, _ := json.Marshal(map[string]string{"kind": "checkpoint", "name": "next", "input_hash": hex.EncodeToString(digest[:])})
	completion, _ := json.Marshal(map[string]string{"result_ref": "step-result-" + hash, "result_hash": hash})
	tail, err := graph.Begin(ctx, h.Type, h.ID, h.InvSeq)
	if err != nil {
		t.Fatal(err)
	}
	for i, entry := range []journal.Entry{{Kind: journal.Started, Payload: started}, {Kind: journal.StepRequested, Payload: request}, {Kind: journal.StepCompleted, Payload: completion}} {
		entry.Index = uint64(i)
		entry.Epoch = owner.Epoch()
		var objects [][]byte
		if i == 0 {
			objects = [][]byte{[]byte(`7`)}
		} else if i == 2 {
			objects = [][]byte{encoded}
		}
		tail, err = graph.Append(ctx, h.Type, h.ID, h.InvSeq, entry, tail, objects, nil)
		if err != nil {
			t.Fatal(err)
		}
	}
	records, _, err := graph.ReadExisting(ctx, h.Type, h.ID, h.InvSeq)
	if err != nil {
		t.Fatal(err)
	}
	worker := &Worker{graphJournal: graph, client: c}
	point := wf.ContinuationCheckpoint{ContinuationAnchor: wf.ContinuationAnchor{Index: 2, Epoch: owner.Epoch()}, Stage: "next", Object: "step-result-" + hash, SHA256: hash, StepPosition: 2}
	appendEntry := func(kind journal.Kind, payload json.RawMessage) error {
		if err := owner.Renew(ctx); err != nil {
			return err
		}
		var err error
		tail, err = graph.Append(ctx, h.Type, h.ID, h.InvSeq, journal.Entry{Index: uint64(len(records)), Epoch: owner.Epoch(), Kind: kind, Payload: payload}, tail, nil, nil)
		if err == nil {
			records, _, err = graph.ReadExisting(ctx, h.Type, h.ID, h.InvSeq)
		}
		return err
	}
	runs, err := js.Stream(ctx, "WF_RUN")
	if err != nil {
		t.Fatal(err)
	}
	if err := runs.Purge(ctx); err != nil {
		t.Fatal(err)
	}
	scan, err := reconcile.NewCanonicalContinuationScan(js, graph)
	if err != nil {
		t.Fatal(err)
	}
	unpublishedStatus, err := graph.InspectStart(ctx, h.Type, h.ID)
	if err != nil || unpublishedStatus.Checkpoint != nil || unpublishedStatus.ContinuationRecoverySequence() != records[2].Sequence {
		t.Fatal("unpublished completion recovery hint", unpublishedStatus, err)
	}
	heldUnpublished, err := scan.Scan(ctx, 1, 1, false)
	if err != nil || heldUnpublished.Reenqueued != 0 {
		t.Fatal("unpublished handoff dispatched with held lease", heldUnpublished, err)
	}
	if err := owner.Release(ctx); err != nil {
		t.Fatal(err)
	}
	recoveredUnpublished, err := scan.Scan(ctx, 1, 1, false)
	if err != nil || recoveredUnpublished.Reenqueued != 1 {
		t.Fatal("unpublished handoff not rediscovered", recoveredUnpublished, err)
	}
	owner, err = leases.Acquire(ctx, h.Type, h.ID, "handoff-recovered")
	if err != nil {
		t.Fatal(err)
	}
	defer owner.Release(context.Background())
	if err := runs.Purge(ctx); err != nil {
		t.Fatal(err)
	}
	if !unpublished {
		wrong := point
		wrong.SHA256 = hex.EncodeToString(digest[:])
		wrong.Object = "step-result-" + wrong.SHA256
		if err := worker.publishContinuation(ctx, h.Type, h.ID, h.InvSeq, owner, records, wrong, appendEntry); !errors.Is(err, journal.ErrGap) {
			t.Fatal("unconfirmed checkpoint dispatched", err)
		}
		info, err := runs.Info(ctx)
		if err != nil || info.State.Msgs != 0 {
			t.Fatal("invalid handoff published", info, err)
		}
		if err := worker.publishContinuation(ctx, h.Type, h.ID, h.InvSeq, owner, records, point, appendEntry); err != nil {
			t.Fatal(err)
		}
		if len(records) != 4 || records[3].Kind != journal.Suspended {
			t.Fatal("missing suspension", records)
		}
		first, err := runs.Info(ctx)
		if err != nil || first.State.Msgs != 1 {
			t.Fatal(first, err)
		}
		if err := worker.publishContinuation(ctx, h.Type, h.ID, h.InvSeq, owner, records, point, appendEntry); err != nil {
			t.Fatal(err)
		}
		second, err := runs.Info(ctx)
		if err != nil || second.State.Msgs != 2 || len(records) != 4 {
			t.Fatal("retry suppressed or appended duplicate suspension", second, err)
		}
		held, err := scan.Scan(ctx, 1, 1, false)
		if err != nil || held.Reenqueued != 0 {
			t.Fatal("scanner dispatched while delivery held lease", held, err)
		}
		if err := owner.Release(ctx); err != nil {
			t.Fatal(err)
		}
		if err := worker.publishContinuation(ctx, h.Type, h.ID, h.InvSeq, owner, records, point, appendEntry); !errors.Is(err, lease.ErrLost) {
			t.Fatal("lost lease authorized handoff", err)
		}
		afterLoss, err := runs.Info(ctx)
		if err != nil || afterLoss.State.Msgs != 2 || len(records) != 4 {
			t.Fatal("lost lease mutated handoff", afterLoss, err)
		}
	} else if err := owner.Release(ctx); err != nil {
		t.Fatal(err)
	}
	// Lose all wakeups twice within the native dedup window. Discovery must
	// dispatch freshly each time using only the retained checkpoint metadata.
	for attempt := 0; attempt < 2; attempt++ {
		if err := runs.Purge(ctx); err != nil {
			t.Fatal(err)
		}
		repaired, err := scan.Scan(ctx, 1, 1, false)
		info, infoErr := runs.Info(ctx)
		if err != nil || infoErr != nil || repaired.Reenqueued != 1 || info.State.Msgs != 1 {
			t.Fatal("lost wakeup not recovered", attempt, repaired, info, err, infoErr)
		}
	}
	// Exercise the production fenced leader/cursor loop too, with a joined
	// shutdown before stage execution starts acquiring delivery leases.
	if err := runs.Purge(ctx); err != nil {
		t.Fatal(err)
	}
	loopCtx, stopLoop := context.WithCancel(ctx)
	loopDone := make(chan error, 1)
	recoveryEvents := make(chan reconcile.RepairEvent, 1)
	go func() {
		loopDone <- reconcile.RunRepairLoopWithGraphJournal(loopCtx, js, "continuation-recovery", "graph-continuation", 100*time.Millisecond, 1, graph, func(event reconcile.RepairEvent) {
			select {
			case recoveryEvents <- event:
			default:
			}
		}, nil, nil)
	}()
	var event reconcile.RepairEvent
	select {
	case event = <-recoveryEvents:
	case err := <-loopDone:
		stopLoop()
		t.Fatal("recovery loop ended before dispatch", err)
	case <-ctx.Done():
		stopLoop()
		<-loopDone
		t.Fatal("recovery loop did not dispatch", ctx.Err())
	}
	stopLoop()
	if err := <-loopDone; err != nil && !errors.Is(err, context.Canceled) {
		t.Fatal("recovery loop shutdown", err)
	}
	if event.Outcome != "acknowledged" || event.InvocationSequence != h.InvSeq || event.JournalSequence != records[2].Sequence {
		t.Fatal("recovery loop source identity", event)
	}
	if unpublished {
		status, err := graph.InspectStart(ctx, h.Type, h.ID)
		if err != nil || status.Checkpoint != nil || status.Kind != journal.StepCompleted || event.Reason != "interrupted_completion" {
			t.Fatal("unpublished cut was not preserved through dispatch", status, event, err)
		}
	}
	// Construction admission remains closed. Internal migration tests attach
	// stage registrations after constructing the ordinary graph worker.
	effects, firstStage, finalStage := 0, 0, 0
	large := strings.Repeat("owned", 16384)
	initial := func(*wf.Context, json.RawMessage) (json.RawMessage, error) {
		t.Error("initial handler replayed after checkpoint")
		return nil, wf.ErrCorruptJournal
	}
	runner, err := New(ctx, js, "continuation-stage", map[string]Handler{h.Type: initial}, WithGraphJournal(graph))
	if err != nil {
		t.Fatal(err)
	}
	for _, reverse := range []bool{false, true} {
		options := []Option{WithGraphJournal(graph), WithContinuations(h.Type, map[string]ContinuationHandler{"next": func(*wf.Context, json.RawMessage, json.RawMessage) (json.RawMessage, error) { return nil, nil }})}
		if reverse {
			options[0], options[1] = options[1], options[0]
		}
		if _, err := New(ctx, js, "admission-check", map[string]Handler{h.Type: initial}, options...); err == nil || !strings.Contains(err.Error(), "migration is incomplete") {
			t.Fatal("continuation admission opened prematurely", err)
		}
	}
	runner.continuations = map[string]map[string]ContinuationHandler{h.Type: {
		"next": func(c *wf.Context, input, locals json.RawMessage) (json.RawMessage, error) {
			firstStage++
			if string(input) != "7" || string(locals) != `{"count":1}` {
				return nil, wf.ErrCorruptJournal
			}
			var value int
			ok, err := c.GetState("value", &value)
			if err != nil || !ok || value != 42 {
				return nil, wf.ErrCorruptJournal
			}
			got, err := wf.RunOnce(c, "once", nil, func(context.Context, string) (string, error) { effects++; return large, nil })
			if err != nil || got != large {
				return nil, err
			}
			if err := c.SetState("large", got); err != nil {
				return nil, err
			}
			return nil, wf.Continue(c, "finish", map[string]int{"count": 2})
		},
		"finish": func(c *wf.Context, input, locals json.RawMessage) (json.RawMessage, error) {
			finalStage++
			if string(input) != "7" || string(locals) != `{"count":2}` {
				return nil, wf.ErrCorruptJournal
			}
			var value string
			ok, err := c.GetState("large", &value)
			if err != nil || !ok || value != large {
				return nil, wf.ErrCorruptJournal
			}
			return json.Marshal(value)
		},
	}}
	execute := func() {
		t.Helper()
		lease, err := leases.Acquire(ctx, h.Type, h.ID, "continuation-stage")
		if err != nil {
			t.Fatal(err)
		}
		var noOp bool
		err = runner.execute(ctx, h.Type, h.ID, lease, time.Time{}, timerWakeup{}, &noOp, nil)
		if releaseErr := lease.Release(ctx); err == nil {
			err = releaseErr
		}
		if err != nil {
			t.Fatal("canonical stage execution", err)
		}
	}
	execute()
	// A new execution must read the newly owned frame and run only its stage.
	execute()
	execute() // Terminal duplicate must not reenter either stage or its effect.
	terminal, err := scan.Scan(ctx, 1, 1, false)
	if err != nil || terminal.Reenqueued != 0 {
		t.Fatal("terminal checkpoint redispatched", terminal, err)
	}
	result, err := c.Await(ctx, h.Type, h.ID)
	if err != nil {
		t.Fatal(err)
	}
	var decoded string
	if err := json.Unmarshal(result, &decoded); err != nil || decoded != large || effects != 1 || firstStage != 1 || finalStage != 1 {
		t.Fatal("stage replay/state/result mismatch", err, effects, firstStage, finalStage)
	}
	legacy, err := js.Stream(ctx, "WF_JRN")
	if err != nil {
		t.Fatal(err)
	}
	legacyInfo, err := legacy.Info(ctx)
	if err != nil || legacyInfo.State.Msgs != 0 {
		t.Fatal("handoff used legacy journal", legacyInfo, err)
	}
}
