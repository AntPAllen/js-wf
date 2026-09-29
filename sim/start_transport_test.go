package sim

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strconv"
	"testing"
	"time"

	"js-wf/client"
	"js-wf/identity"
	"js-wf/reconcile"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func TestStartTransportFaultBoundaries(t *testing.T) {
	ctx := context.Background()
	t.Run("dropped publish retries without duplicate", func(t *testing.T) {
		model := NewStartTransport(NewScheduler(1))
		if err := model.QueueFault(StartFault{Operation: "publish_invocation", Kind: "drop_before_commit"}); err != nil {
			t.Fatal(err)
		}
		handle, err := client.NewWithStartPort(model).Start(ctx, "test", "one", []byte(`1`))
		if err != nil || handle.InvSeq == 0 || len(model.Runs()) != 1 {
			t.Fatalf("start after dropped publish: handle=%+v runs=%v err=%v", handle, model.Runs(), err)
		}
	})
	t.Run("lost invocation ack stores once without enqueue", func(t *testing.T) {
		model := NewStartTransport(NewScheduler(2))
		if err := model.QueueFault(StartFault{Operation: "publish_invocation", Kind: "lose_ack_after_commit"}); err != nil {
			t.Fatal(err)
		}
		c := client.NewWithStartPort(model)
		first, err := c.Start(ctx, "test", "one", []byte(`1`))
		if !errors.Is(err, client.ErrAlreadyStarted) || first.InvSeq == 0 || len(model.Runs()) != 0 {
			t.Fatalf("lost invocation ack: handle=%+v runs=%v err=%v", first, model.Runs(), err)
		}
		second, err := c.Start(ctx, "test", "one", []byte(`1`))
		if !errors.Is(err, client.ErrAlreadyStarted) || second.InvSeq != first.InvSeq || len(model.Runs()) != 0 {
			t.Fatalf("matching retry: handle=%+v runs=%v err=%v", second, model.Runs(), err)
		}
		_, err = c.Start(ctx, "test", "one", []byte(`2`))
		if !errors.Is(err, client.ErrInputMismatch) {
			t.Fatalf("changed retry: %v", err)
		}
	})
	t.Run("lost enqueue ack commits exactly once", func(t *testing.T) {
		model := NewStartTransport(NewScheduler(3))
		if err := model.QueueFault(StartFault{Operation: "enqueue_run", Kind: "lose_ack_after_commit"}); err != nil {
			t.Fatal(err)
		}
		handle, err := client.NewWithStartPort(model).Start(ctx, "test", "one", []byte(`1`))
		if !errors.Is(err, client.ErrEnqueueUnknown) || handle.InvSeq == 0 || len(model.Runs()) != 1 {
			t.Fatalf("lost run ack: handle=%+v runs=%v err=%v", handle, model.Runs(), err)
		}
	})
	t.Run("large input retains object and hash", func(t *testing.T) {
		model := NewStartTransport(NewScheduler(4))
		input := bytes.Repeat([]byte("x"), client.MaxInlineInput+1)
		if _, err := client.NewWithStartPort(model).Start(ctx, "test", "large", input); err != nil {
			t.Fatal(err)
		}
		stored, ok := model.Invocation(identity.InvocationSubject("test", "large"))
		if !ok || len(stored.Data) != 0 || !bytes.Equal(model.Input(stored.Header.Get("Wf-Input-Ref")), input) {
			t.Fatal("large input object or invocation reference differs")
		}
	})
	t.Run("stale read after duplicate is retried", func(t *testing.T) {
		model := NewStartTransport(NewScheduler(5))
		c := client.NewWithStartPort(model)
		first, err := c.Start(ctx, "test", "one", []byte(`1`))
		if err != nil {
			t.Fatal(err)
		}
		if err := model.QueueFault(StartFault{Operation: "last_invocation", Kind: "stale_read"}); err != nil {
			t.Fatal(err)
		}
		second, err := c.Start(ctx, "test", "one", []byte(`1`))
		if !errors.Is(err, client.ErrAlreadyStarted) || second.InvSeq != first.InvSeq || len(model.Runs()) != 1 {
			t.Fatalf("stale-read retry: handle=%+v runs=%v err=%v", second, model.Runs(), err)
		}
	})
}

func TestStartSubjectTailCASDefendsAgainstIgnoredSubjectLimit(t *testing.T) {
	ctx := context.Background()
	model := NewStartTransport(NewScheduler(104))
	c := client.NewWithStartPort(model)
	first, err := c.Start(ctx, "test", "same", []byte(`1`))
	if err != nil {
		t.Fatal(err)
	}
	guarded := &nats.Msg{Subject: identity.InvocationSubject("test", "same"), Data: []byte(`1`), Header: nats.Header{}}
	guarded.Header.Set(jetstream.ExpectedLastSubjSeqHeader, "0")
	if _, err := model.PublishInvocation(ctx, guarded); err == nil {
		t.Fatal("normal duplicate unexpectedly passed CAS")
	} else {
		var apiErr *jetstream.APIError
		if !errors.As(err, &apiErr) || apiErr.ErrorCode != jetstream.JSErrCodeStreamWrongLastSequence {
			t.Fatalf("normal duplicate should report CAS rejection first: %v", err)
		}
	}
	if err := model.QueueFault(StartFault{Operation: "publish_invocation", Kind: "bypass_subject_limit"}); err != nil {
		t.Fatal(err)
	}
	retry, err := c.Start(ctx, "test", "same", []byte(`1`))
	stored, ok := model.Invocation(identity.InvocationSubject("test", "same"))
	if !errors.Is(err, client.ErrAlreadyStarted) || retry.InvSeq != first.InvSeq || !ok || stored.Sequence != first.InvSeq {
		t.Fatalf("CAS-protected retry: first=%+v retry=%+v stored=%+v err=%v", first, retry, stored, err)
	}
	// Negative control: strip the CAS header while the server-side subject
	// limit is ignored. The same transport now accepts a new generation.
	if err := model.QueueFault(StartFault{Operation: "publish_invocation", Kind: "bypass_subject_limit"}); err != nil {
		t.Fatal(err)
	}
	unguarded, err := model.PublishInvocation(ctx, &nats.Msg{Subject: identity.InvocationSubject("test", "same"), Data: []byte(`1`), Header: nats.Header{}})
	stored, ok = model.Invocation(identity.InvocationSubject("test", "same"))
	if err != nil || !ok || unguarded <= first.InvSeq || stored.Sequence != unguarded {
		t.Fatalf("missing-CAS mutation was not retained: first=%+v stored=%+v ack=%d err=%v", first, stored, unguarded, err)
	}
}

func TestMissingRunMessageIDMutationRetainsDuplicate(t *testing.T) {
	ctx := context.Background()
	const subject = "wf.run.0"
	withID := NewStartTransport(NewScheduler(101))
	for range 2 {
		if err := withID.EnqueueRun(ctx, subject, []byte("same"), "start:test.same:1"); err != nil {
			t.Fatal(err)
		}
	}
	if got := len(withID.Runs()); got != 1 {
		t.Fatalf("message ID did not deduplicate: runs=%d", got)
	}
	withoutID := NewStartTransport(NewScheduler(101))
	for range 2 {
		if err := withoutID.EnqueueRun(ctx, subject, []byte("same"), ""); err != nil {
			t.Fatal(err)
		}
	}
	if got := len(withoutID.Runs()); got != 2 {
		t.Fatalf("missing message ID mutation escaped duplicate-run check: runs=%d", got)
	}
}

func TestSkippedStartReconcilerMutationIsDetected(t *testing.T) {
	ctx := context.Background()
	model := NewStartTransport(NewScheduler(102))
	const typ, id = "test", "missing-start-run"
	subject := identity.InvocationSubject(typ, id)
	sequence, err := model.PublishInvocation(ctx, &nats.Msg{Subject: subject, Data: []byte(`null`)})
	if err != nil {
		t.Fatal(err)
	}
	missing := func() {
		t.Helper()
		report, err := CheckStartWakeupLiveness(model)
		if err == nil || report.Unstarted != 1 || !reflect.DeepEqual(report.Missing, []string{identity.Key(typ, id)}) {
			t.Fatalf("skipped reconciler escaped liveness check: report=%+v err=%v", report, err)
		}
	}
	missing()
	scan := reconcile.NewStartScanWithPort(model)
	if result, err := scan.Scan(ctx, sequence, 1, false); err != nil || result.Reenqueued != 1 {
		t.Fatalf("repair missing start: result=%+v err=%v", result, err)
	}
	if report, err := CheckStartWakeupLiveness(model); err != nil || report.Unstarted != 1 || len(report.Missing) != 0 {
		t.Fatalf("repaired start liveness: report=%+v err=%v", report, err)
	}
	model.mu.Lock()
	model.runs = nil // Deliberately lose the retained wakeup after repair.
	model.mu.Unlock()
	missing()
	model.MarkJournal(typ, id)
	if report, err := CheckStartWakeupLiveness(model); err != nil || report.Unstarted != 0 {
		t.Fatalf("started invocation still needs a run: report=%+v err=%v", report, err)
	}
}

func TestStartLivenessFencesReusedIDWithOldJournal(t *testing.T) {
	ctx := context.Background()
	model := NewStartTransport(NewScheduler(103))
	const typ, id = "test", "reused-start"
	if _, err := client.NewWithStartPort(model).Start(ctx, typ, id, []byte(`1`)); err != nil {
		t.Fatal(err)
	}
	model.MarkJournal(typ, id)
	model.PurgeInvocation(identity.InvocationSubject(typ, id)) // Mutated order: old journal remains.
	newSequence, err := model.PublishInvocation(ctx, &nats.Msg{Subject: identity.InvocationSubject(typ, id), Data: []byte(`2`)})
	if err != nil {
		t.Fatal(err)
	}
	missing := func() {
		t.Helper()
		report, err := CheckStartWakeupLiveness(model)
		if err == nil || report.Unstarted != 1 || !reflect.DeepEqual(report.Missing, []string{identity.Key(typ, id)}) {
			t.Fatalf("old generation hid missing new run: report=%+v err=%v", report, err)
		}
	}
	missing()
	scan := reconcile.NewStartScanWithPort(model)
	if result, err := scan.Scan(ctx, newSequence, 1, false); err != nil || result.Reenqueued != 0 {
		t.Fatalf("stale journal unexpectedly repaired new start: result=%+v err=%v", result, err)
	}
	missing()
	model.PurgeJournal(typ, id)
	if result, err := scan.Scan(ctx, newSequence, 1, false); err != nil || result.Reenqueued != 1 {
		t.Fatalf("repair after old journal purge: result=%+v err=%v", result, err)
	}
	if report, err := CheckStartWakeupLiveness(model); err != nil || report.Unstarted != 1 || len(report.Missing) != 0 {
		t.Fatalf("new generation liveness: report=%+v err=%v", report, err)
	}
}

func TestStartRunDedupWindowExpiresInVirtualTime(t *testing.T) {
	schedule := NewScheduler(19)
	model := NewStartTransport(schedule)
	if err := model.SetRunDedupWindow(time.Second); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	const subject, messageID = "wf.run.0", "start:test.one:1"
	for i := 0; i < 2; i++ {
		if err := model.EnqueueRun(ctx, subject, []byte("test.one"), messageID); err != nil {
			t.Fatal(err)
		}
	}
	if len(model.Runs()) != 1 {
		t.Fatalf("duplicate inside window retained %d runs", len(model.Runs()))
	}
	if err := model.Wait(ctx, 3*time.Second); err != nil {
		t.Fatal(err)
	}
	if err := model.EnqueueRun(ctx, subject, []byte("test.one"), messageID); err != nil {
		t.Fatal(err)
	}
	if runs := model.Runs(); len(runs) != 2 || runs[0].Sequence == runs[1].Sequence {
		t.Fatalf("expired message ID did not create a new run: %+v", runs)
	}
}

func TestStartScanRepeatsAfterDedupExpiryUntilJournalExists(t *testing.T) {
	ctx := context.Background()
	model := NewStartTransport(NewScheduler(20))
	invocation := &nats.Msg{Subject: identity.InvocationSubject("test", "repair"), Data: []byte("input")}
	sequence, err := model.PublishInvocation(ctx, invocation)
	if err != nil {
		t.Fatal(err)
	}
	scan := reconcile.NewStartScanWithPort(model)
	for i := 0; i < 2; i++ {
		if _, err := scan.Scan(ctx, sequence, 1, false); err != nil {
			t.Fatal(err)
		}
	}
	if len(model.Runs()) != 1 {
		t.Fatalf("repair inside duplicate window retained %d runs", len(model.Runs()))
	}
	if err := model.Wait(ctx, 3*time.Minute); err != nil {
		t.Fatal(err)
	}
	if _, err := scan.Scan(ctx, sequence, 1, false); err != nil {
		t.Fatal(err)
	}
	if len(model.Runs()) != 2 {
		t.Fatalf("repair after duplicate window retained %d runs", len(model.Runs()))
	}
	model.MarkJournal("test", "repair")
	if _, err := scan.Scan(ctx, sequence, 1, false); err != nil {
		t.Fatal(err)
	}
	if len(model.Runs()) != 2 {
		t.Fatalf("repair after journal retained %d runs", len(model.Runs()))
	}
}

func TestStartScanModelCursorAndUncertainEnqueue(t *testing.T) {
	ctx := context.Background()
	model := NewStartTransport(NewScheduler(6))
	first, err := client.NewWithStartPort(model).Start(ctx, "test", "purged", []byte(`one`))
	if err != nil {
		t.Fatal(err)
	}
	model.PurgeInvocation(identity.InvocationSubject("test", "purged"))
	if err := model.QueueFault(StartFault{Operation: "publish_invocation", Kind: "lose_ack_after_commit"}); err != nil {
		t.Fatal(err)
	}
	second, err := client.NewWithStartPort(model).Start(ctx, "test", "repair", []byte(`two`))
	if !errors.Is(err, client.ErrAlreadyStarted) || second.InvSeq <= first.InvSeq || len(model.Runs()) != 1 {
		t.Fatalf("uncertain second start: handle=%+v runs=%v err=%v", second, model.Runs(), err)
	}
	scan := reconcile.NewStartScanWithPort(model)
	dry, err := scan.Scan(ctx, first.InvSeq, 2, true)
	if err != nil || dry.Inspected != 1 || dry.Reenqueued != 1 || dry.NextSequence != second.InvSeq+1 || len(model.Runs()) != 1 {
		t.Fatalf("dry run over sequence hole: result=%+v runs=%v err=%v", dry, model.Runs(), err)
	}
	if err := model.QueueFault(StartFault{Operation: "enqueue_run", Kind: "lose_ack_after_commit"}); err != nil {
		t.Fatal(err)
	}
	partial, err := scan.Scan(ctx, first.InvSeq, 2, false)
	if !errors.Is(err, ErrTransportLost) || partial.NextSequence != second.InvSeq+1 || partial.Reenqueued != 1 || len(model.Runs()) != 2 {
		t.Fatalf("lost repair enqueue ack: result=%+v runs=%v err=%v", partial, model.Runs(), err)
	}
	retry, err := scan.Scan(ctx, first.InvSeq, 2, false)
	if err != nil || retry.Reenqueued != 1 || len(model.Runs()) != 2 {
		t.Fatalf("repeated repair should deduplicate: result=%+v runs=%v err=%v", retry, model.Runs(), err)
	}
	model.MarkJournal("test", "repair")
	final, err := scan.Scan(ctx, first.InvSeq, 2, false)
	if err != nil || final.Reenqueued != 0 || len(model.Runs()) != 2 {
		t.Fatalf("journaled repair should stop: result=%+v runs=%v err=%v", final, model.Runs(), err)
	}
}

func runSeededStartScenario(seed int64, replay *Trace) (trace Trace, runErr error) {
	var schedule *Scheduler
	if replay == nil {
		schedule = NewScheduler(seed)
	} else {
		var err error
		schedule, err = ReplayScheduler(*replay)
		if err != nil {
			return Trace{}, err
		}
	}
	if err := schedule.SetWorkload("client_start_repair_20"); err != nil {
		return Trace{}, err
	}
	defer func() { trace = schedule.Trace() }()
	model := NewStartTransport(schedule)
	c := client.NewWithStartPort(model)
	scanner := reconcile.NewStartScanWithPort(model)
	committedRuns := 0
	for i := 0; i < 20; i++ {
		choice, err := schedule.Choose([]string{"normal", "lost_inv_ack", "lost_run_ack", "drop_run"})
		if err != nil {
			return trace, err
		}
		id := fmt.Sprintf("case-%02d", i)
		switch choice {
		case "lost_inv_ack":
			err = model.QueueFault(StartFault{Operation: "publish_invocation", Kind: "lose_ack_after_commit"})
		case "lost_run_ack":
			err = model.QueueFault(StartFault{Operation: "enqueue_run", Kind: "lose_ack_after_commit"})
		case "drop_run":
			err = model.QueueFault(StartFault{Operation: "enqueue_run", Kind: "drop_before_commit"})
		}
		if err != nil {
			return trace, err
		}
		input := []byte(fmt.Sprintf(`{"case":%d}`, i))
		handle, startErr := c.Start(context.Background(), "test", id, input)
		stored, ok := model.Invocation(identity.InvocationSubject("test", id))
		if !ok || stored.Sequence != handle.InvSeq || handle.InvSeq == 0 {
			return trace, fmt.Errorf("seed %d case %d missing invocation: handle=%+v", seed, i, handle)
		}
		switch choice {
		case "normal":
			committedRuns++
			if startErr != nil {
				return trace, fmt.Errorf("seed %d case %d normal: %w", seed, i, startErr)
			}
		case "lost_inv_ack":
			if !errors.Is(startErr, client.ErrAlreadyStarted) {
				return trace, fmt.Errorf("seed %d case %d lost inv ack: %v", seed, i, startErr)
			}
		case "lost_run_ack":
			committedRuns++
			if !errors.Is(startErr, client.ErrEnqueueUnknown) {
				return trace, fmt.Errorf("seed %d case %d lost run ack: %v", seed, i, startErr)
			}
		case "drop_run":
			if !errors.Is(startErr, client.ErrEnqueueUnknown) {
				return trace, fmt.Errorf("seed %d case %d drop run: %v", seed, i, startErr)
			}
		}
		if got := len(model.Runs()); got != committedRuns {
			return trace, fmt.Errorf("seed %d case %d retained %d runs, want %d", seed, i, got, committedRuns)
		}
		liveness, livenessErr := CheckStartWakeupLiveness(model)
		missingRun := choice == "lost_inv_ack" || choice == "drop_run"
		if liveness.Unstarted != 1 || missingRun && (livenessErr == nil || !reflect.DeepEqual(liveness.Missing, []string{identity.Key("test", id)})) || !missingRun && (livenessErr != nil || len(liveness.Missing) != 0) {
			return trace, fmt.Errorf("seed %d case %d pre-repair liveness: report=%+v err=%v", seed, i, liveness, livenessErr)
		}
		retry, retryErr := c.Start(context.Background(), "test", id, input)
		if !errors.Is(retryErr, client.ErrAlreadyStarted) || retry.InvSeq != handle.InvSeq {
			return trace, fmt.Errorf("seed %d case %d matching retry: handle=%+v err=%v", seed, i, retry, retryErr)
		}
		_, mismatchErr := c.Start(context.Background(), "test", id, []byte(`changed`))
		if !errors.Is(mismatchErr, client.ErrInputMismatch) {
			return trace, fmt.Errorf("seed %d case %d mismatched retry: %v", seed, i, mismatchErr)
		}
		result, err := scanner.Scan(context.Background(), handle.InvSeq, 1, false)
		if err != nil || result.Inspected != 1 || result.Reenqueued != 1 || result.NextSequence != handle.InvSeq+1 {
			return trace, fmt.Errorf("seed %d case %d repair: result=%+v err=%v", seed, i, result, err)
		}
		committedRuns = i + 1
		if got := len(model.Runs()); got != committedRuns {
			return trace, fmt.Errorf("seed %d case %d after repair has %d runs, want %d", seed, i, got, committedRuns)
		}
		if liveness, err := CheckStartWakeupLiveness(model); err != nil || liveness.Unstarted != 1 || len(liveness.Missing) != 0 {
			return trace, fmt.Errorf("seed %d case %d repaired liveness: report=%+v err=%v", seed, i, liveness, err)
		}
		model.MarkJournal("test", id)
		result, err = scanner.Scan(context.Background(), handle.InvSeq, 1, false)
		if err != nil || result.Reenqueued != 0 || len(model.Runs()) != committedRuns {
			return trace, fmt.Errorf("seed %d case %d journaled rescan: result=%+v runs=%d err=%v", seed, i, result, len(model.Runs()), err)
		}
	}
	if err := schedule.Finish(); err != nil {
		return trace, err
	}
	return schedule.Trace(), nil
}

func TestSeededStartModelReplay(t *testing.T) {
	if os.Getenv("SIM_START_TRACE_HELPER") == "1" {
		seed, err := strconv.ParseInt(os.Getenv("FAULT_SEED"), 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		trace, err := runSeededStartScenario(seed, nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := trace.Save(os.Getenv("SIM_START_TRACE_OUT")); err != nil {
			t.Fatal(err)
		}
		return
	}
	for seed, limit := int64(1), seededScheduleLimit(t); seed <= limit; seed++ {
		generated, err := runSeededStartScenario(seed, nil)
		if err != nil {
			path := os.Getenv("FAULT_TRACE_OUT")
			if path == "" {
				dir, dirErr := os.MkdirTemp("", "js-wf-start-sim-failure-")
				if dirErr != nil {
					t.Fatalf("FAULT_SEED=%d: %v; save trace: %v", seed, err, dirErr)
				}
				path = filepath.Join(dir, "trace.json")
			} else {
				path = path[:len(path)-len(filepath.Ext(path))] + "-start.json"
			}
			if saveErr := generated.Save(path); saveErr != nil {
				t.Fatalf("FAULT_SEED=%d: %v; save trace: %v", seed, err, saveErr)
			}
			t.Fatalf("FAULT_SEED=%d FAULT_TRACE=%s: %v", seed, path, err)
		}
		if seed <= 10 {
			replayed, err := runSeededStartScenario(seed, &generated)
			if err != nil || !reflect.DeepEqual(generated, replayed) {
				t.Fatalf("FAULT_SEED=%d start trace replay: %v", seed, err)
			}
		}
	}
	var files [2]string
	for i := range files {
		files[i] = filepath.Join(t.TempDir(), fmt.Sprintf("start-%d.json", i))
		cmd := exec.Command(os.Args[0], "-test.run=^TestSeededStartModelReplay$")
		cmd.Env = append(os.Environ(), "SIM_START_TRACE_HELPER=1", "SIM_START_TRACE_OUT="+files[i], "FAULT_SEED=42")
		if output, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("seeded start child %d: %v: %s", i, err, output)
		}
	}
	first, err := os.ReadFile(files[0])
	if err != nil {
		t.Fatal(err)
	}
	second, err := os.ReadFile(files[1])
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(first, second) {
		t.Fatal("seeded start trace changed across processes")
	}
}

func runTwoStarterRace(seed int64, mismatch bool, replay *Trace) (trace Trace, runErr error) {
	var schedule *Scheduler
	if replay == nil {
		schedule = NewScheduler(seed)
	} else {
		var err error
		schedule, err = ReplayScheduler(*replay)
		if err != nil {
			return Trace{}, err
		}
	}
	workload := "client_two_starter_same"
	if mismatch {
		workload = "client_two_starter_mismatch"
	}
	if err := schedule.SetWorkload(workload); err != nil {
		return Trace{}, err
	}
	defer func() { trace = schedule.Trace() }()
	model := NewStartTransport(schedule)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	handles := [2]client.Handle{}
	actors := make([]StartActor, 0, 2)
	for i, name := range []string{"alpha", "beta"} {
		i, name := i, name
		actors = append(actors, StartActor{Name: name, Run: func(ctx context.Context, port client.StartPort) error {
			input := []byte(`same`)
			if mismatch && i == 1 {
				input = []byte(`different`)
			}
			var err error
			handles[i], err = client.NewWithStartPort(port).Start(ctx, "test", "race", input)
			return err
		}})
	}
	results, err := RunStartActors(ctx, schedule, model, actors)
	if err != nil {
		return trace, err
	}
	stored, ok := model.Invocation(identity.InvocationSubject("test", "race"))
	if !ok || len(model.Runs()) != 1 || handles[0].InvSeq != stored.Sequence || handles[1].InvSeq != stored.Sequence {
		return trace, fmt.Errorf("race state: stored=%+v runs=%v handles=%v", stored, model.Runs(), handles)
	}
	successes, already, changed := 0, 0, 0
	for _, result := range results {
		switch {
		case result == nil:
			successes++
		case errors.Is(result, client.ErrAlreadyStarted):
			already++
		case errors.Is(result, client.ErrInputMismatch):
			changed++
		default:
			return trace, fmt.Errorf("unexpected race result: %v", result)
		}
	}
	if successes != 1 || !mismatch && already != 1 || mismatch && changed != 1 {
		return trace, fmt.Errorf("race results: success=%d already=%d mismatch=%d", successes, already, changed)
	}
	if err := schedule.Finish(); err != nil {
		return trace, err
	}
	return schedule.Trace(), nil
}

func TestCooperativeStartRacesReplay(t *testing.T) {
	for seed := int64(1); seed <= 100; seed++ {
		for _, mismatch := range []bool{false, true} {
			generated, err := runTwoStarterRace(seed, mismatch, nil)
			if err != nil {
				t.Fatalf("FAULT_SEED=%d mismatch=%v: %v", seed, mismatch, err)
			}
			replayed, err := runTwoStarterRace(seed, mismatch, &generated)
			if err != nil || !reflect.DeepEqual(generated, replayed) {
				t.Fatalf("FAULT_SEED=%d mismatch=%v replay: %v", seed, mismatch, err)
			}
		}
	}
}

func runConcurrentStartRepair(seed int64, replay *Trace) (trace Trace, runErr error) {
	var schedule *Scheduler
	if replay == nil {
		schedule = NewScheduler(seed)
	} else {
		var err error
		schedule, err = ReplayScheduler(*replay)
		if err != nil {
			return Trace{}, err
		}
	}
	if err := schedule.SetWorkload("client_start_scan_race"); err != nil {
		return Trace{}, err
	}
	defer func() { trace = schedule.Trace() }()
	model := NewStartTransport(schedule)
	if err := model.QueueFault(StartFault{Operation: "publish_invocation", Kind: "lose_ack_after_commit"}); err != nil {
		return trace, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	actors := []StartAndScanActor{
		{Name: "client", Run: func(ctx context.Context, start client.StartPort, _ reconcile.StartScanPort) error {
			_, err := client.NewWithStartPort(start).Start(ctx, "test", "race", []byte(`input`))
			return err
		}},
		{Name: "scanner", Run: func(ctx context.Context, _ client.StartPort, scan reconcile.StartScanPort) error {
			_, err := reconcile.NewStartScanWithPort(scan).Scan(ctx, 1, 1, false)
			return err
		}},
	}
	results, err := RunStartAndScanActors(ctx, schedule, model, actors)
	if err != nil {
		return trace, err
	}
	if !errors.Is(results["client"], client.ErrAlreadyStarted) || results["scanner"] != nil {
		return trace, fmt.Errorf("concurrent start/scan results: %v", results)
	}
	stored, exists := model.Invocation(identity.InvocationSubject("test", "race"))
	if !exists || stored.Sequence != 1 || len(model.Runs()) > 1 {
		return trace, fmt.Errorf("concurrent state: invocation=%+v runs=%v", stored, model.Runs())
	}
	if _, err := reconcile.NewStartScanWithPort(model).Scan(ctx, 1, 1, false); err != nil {
		return trace, err
	}
	if len(model.Runs()) != 1 {
		return trace, fmt.Errorf("post-race repair has %d runs, want one", len(model.Runs()))
	}
	if err := schedule.Finish(); err != nil {
		return trace, err
	}
	return schedule.Trace(), nil
}

func TestCooperativeStartAndScannerRacesReplay(t *testing.T) {
	for seed := int64(1); seed <= 100; seed++ {
		generated, err := runConcurrentStartRepair(seed, nil)
		if err != nil {
			t.Fatalf("FAULT_SEED=%d start/scan race: %v", seed, err)
		}
		replayed, err := runConcurrentStartRepair(seed, &generated)
		if err != nil || !reflect.DeepEqual(generated, replayed) {
			t.Fatalf("FAULT_SEED=%d start/scan replay: %v", seed, err)
		}
	}
}
