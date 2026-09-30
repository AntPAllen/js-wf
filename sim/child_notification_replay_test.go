package sim

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strconv"
	"testing"

	"js-wf/client"
	"js-wf/identity"
	"js-wf/journal"
	"js-wf/reconcile"
	"js-wf/worker"

	"github.com/nats-io/nats.go"
)

func runSeededChildNotification(seed int64, replay *Trace) (trace Trace, runErr error) {
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
	if err := schedule.SetWorkload("child_notification_20"); err != nil {
		return Trace{}, err
	}
	defer func() { trace = schedule.Trace() }()
	ctx := context.Background()
	model := NewSignalTransport(schedule)
	c := client.NewWithSignalPorts(model, model)
	scan := reconcile.NewSignalScanWithPort(model)
	for i := 0; i < 20; i++ {
		mode, err := schedule.Choose([]string{"normal", "drop_signal", "lost_signal_ack", "drop_wakeup", "lost_wakeup_ack", "parent_reused"})
		if err != nil {
			return trace, err
		}
		parentID := fmt.Sprintf("parent-%02d", i)
		childID := fmt.Sprintf("child-%02d", i)
		parentSeq, err := model.PublishInvocation(ctx, &nats.Msg{Subject: identity.InvocationSubject("test", parentID), Data: []byte("parent")})
		if err != nil {
			return trace, err
		}
		headers := nats.Header{}
		headers.Set(client.ParentTypeHeader, "test")
		headers.Set(client.ParentIDHeader, parentID)
		headers.Set(client.ParentInvSeqHeader, strconv.FormatUint(parentSeq, 10))
		headers.Set(client.ParentSignalHeader, "child_done")
		childSeq, err := model.PublishInvocation(ctx, &nats.Msg{Subject: identity.InvocationSubject("test", childID), Data: []byte("child"), Header: headers})
		if err != nil {
			return trace, err
		}
		payload := []byte(fmt.Sprintf(`{"inv_seq":%d,"result":"done-%02d"}`, childSeq, i))
		model.SetJournal("test", childID, []journal.Record{{Entry: journal.Entry{Kind: journal.Completed, Payload: payload}, Sequence: childSeq * 10}})
		if mode == "parent_reused" {
			model.PurgeInvocation(identity.InvocationSubject("test", parentID))
			if _, err := model.PublishInvocation(ctx, &nats.Msg{Subject: identity.InvocationSubject("test", parentID), Data: []byte("new parent")}); err != nil {
				return trace, err
			}
		}
		switch mode {
		case "drop_signal":
			err = model.QueueSignalFault(SignalDropBeforeCommit)
		case "lost_signal_ack":
			err = model.QueueSignalFault(SignalLoseAckAfterCommit)
		case "drop_wakeup":
			err = model.QueueFault(StartFault{Operation: "enqueue_run", Kind: "drop_before_commit"})
		case "lost_wakeup_ack":
			err = model.QueueFault(StartFault{Operation: "enqueue_run", Kind: "lose_ack_after_commit"})
		}
		if err != nil {
			return trace, err
		}
		if mode != "parent_reused" {
			report, err := CheckChildWakeupLiveness(model)
			if err == nil || len(report.Missing) != 1 {
				return trace, fmt.Errorf("seed %d child %d skipped notification survived: report=%+v err=%v", seed, i, report, err)
			}
		}
		beforeRuns := len(model.Runs())
		firstErr := worker.NotifyParentWithClient(ctx, c, "test", childID, childSeq, payload, headers)
		switch mode {
		case "drop_signal", "lost_signal_ack":
			if !errors.Is(firstErr, client.ErrSignalUnknown) {
				return trace, fmt.Errorf("seed %d child %d %s first signal: %v", seed, i, mode, firstErr)
			}
		case "drop_wakeup", "lost_wakeup_ack":
			if !errors.Is(firstErr, client.ErrEnqueueUnknown) {
				return trace, fmt.Errorf("seed %d child %d %s first wakeup: %v", seed, i, mode, firstErr)
			}
		default:
			if firstErr != nil {
				return trace, fmt.Errorf("seed %d child %d %s first notify: %w", seed, i, mode, firstErr)
			}
		}
		if mode == "drop_signal" || mode == "lost_signal_ack" || mode == "drop_wakeup" {
			report, err := CheckChildWakeupLiveness(model)
			if err == nil || len(report.Missing) != 1 {
				return trace, fmt.Errorf("seed %d child %d lost delivery survived: report=%+v err=%v", seed, i, report, err)
			}
		}
		stored := model.SignalFor("test", parentID, "child_done")
		if mode == "parent_reused" {
			if len(stored) != 0 || len(model.Runs()) != beforeRuns {
				return trace, fmt.Errorf("seed %d child %d notified a reused parent", seed, i)
			}
			continue
		}
		if mode == "lost_signal_ack" || mode == "drop_wakeup" {
			report, checkErr := CheckSignalWakeupLiveness(model)
			if checkErr == nil || len(report.Missing) != 1 {
				return trace, fmt.Errorf("seed %d child %d lost notification not detected: missing=%v err=%v", seed, i, report.Missing, checkErr)
			}
		}
		if secondErr := worker.NotifyParentWithClient(ctx, c, "test", childID, childSeq, payload, headers); secondErr != nil {
			return trace, fmt.Errorf("seed %d child %d %s retry: %w", seed, i, mode, secondErr)
		}
		stored = model.SignalFor("test", parentID, "child_done")
		if len(stored) != 1 || len(model.Runs()) != beforeRuns+1 || !bytes.Equal(stored[0].Data, payload) {
			return trace, fmt.Errorf("seed %d child %d %s retained signals=%d runs=%d", seed, i, mode, len(stored), len(model.Runs())-beforeRuns)
		}
		retained, err := model.GetSignal(ctx, stored[0].Sequence)
		if err != nil || retained.Header.Get("Wf-Inv-Seq") != strconv.FormatUint(parentSeq, 10) {
			return trace, fmt.Errorf("seed %d child %d %s parent generation header: %v", seed, i, mode, err)
		}
		if _, err := scan.Scan(ctx, stored[0].Sequence, 1, false); err != nil || len(model.Runs()) != beforeRuns+1 {
			return trace, fmt.Errorf("seed %d child %d scanner dedup: %v", seed, i, err)
		}
	}
	if report, err := CheckChildWakeupLiveness(model); err != nil || len(report.Missing) != 0 {
		return trace, fmt.Errorf("seed %d child completion liveness: report=%+v err=%v", seed, report, err)
	}
	report, err := CheckSignalWakeupLiveness(model)
	if err != nil || len(report.Missing) != 0 {
		return trace, fmt.Errorf("seed %d child liveness: enabled=%d waiting=%v missing=%v err=%v", seed, report.Enabled, report.Waiting, report.Missing, err)
	}
	waiting, err := json.Marshal(report.Waiting)
	if err != nil {
		return trace, err
	}
	schedule.RecordTransport(TransportEvent{Operation: "check_child_notification_liveness", Sequence: uint64(report.Enabled), DataSHA256: digest(waiting), Outcome: "ok", AtMillis: schedule.NowMillis()})
	if err := schedule.Finish(); err != nil {
		return trace, err
	}
	return schedule.Trace(), nil
}

func TestSeededChildNotificationReplay(t *testing.T) {
	if os.Getenv("SIM_CHILD_NOTIFICATION_HELPER") == "1" {
		seed, err := strconv.ParseInt(os.Getenv("FAULT_SEED"), 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		trace, err := runSeededChildNotification(seed, nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := trace.Save(os.Getenv("SIM_CHILD_NOTIFICATION_OUT")); err != nil {
			t.Fatal(err)
		}
		return
	}
	for seed, limit := int64(1), seededScheduleLimit(t); seed <= limit; seed++ {
		generated, err := runSeededChildNotification(seed, nil)
		if err != nil {
			path := os.Getenv("FAULT_TRACE_OUT")
			if path == "" {
				dir, dirErr := os.MkdirTemp("", "js-wf-child-notification-failure-")
				if dirErr != nil {
					t.Fatalf("FAULT_SEED=%d: %v; save trace: %v", seed, err, dirErr)
				}
				path = filepath.Join(dir, "trace.json")
			} else {
				path = path[:len(path)-len(filepath.Ext(path))] + "-child-notification.json"
			}
			if saveErr := generated.Save(path); saveErr != nil {
				t.Fatalf("FAULT_SEED=%d: %v; save trace: %v", seed, err, saveErr)
			}
			t.Fatalf("FAULT_SEED=%d FAULT_TRACE=%s: %v", seed, path, err)
		}
		if seed <= 10 {
			replayed, err := runSeededChildNotification(seed, &generated)
			if err != nil || !reflect.DeepEqual(generated, replayed) {
				t.Fatalf("FAULT_SEED=%d child notification replay: %v", seed, err)
			}
		}
	}
	var files [2]string
	for i := range files {
		files[i] = filepath.Join(t.TempDir(), fmt.Sprintf("child-notification-%d.json", i))
		cmd := exec.Command(os.Args[0], "-test.run=^TestSeededChildNotificationReplay$")
		cmd.Env = append(os.Environ(), "SIM_CHILD_NOTIFICATION_HELPER=1", "SIM_CHILD_NOTIFICATION_OUT="+files[i], "FAULT_SEED=42")
		if output, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("child %d: %v: %s", i, err, output)
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
		t.Fatal("child notification trace changed across processes")
	}
}
