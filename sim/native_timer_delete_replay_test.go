package sim

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"js-wf/identity"
	"js-wf/journal"
	"js-wf/reconcile"
	"js-wf/worker"
)

type nativeDeleteAPIModel struct {
	*TimerScheduleTransport
	mode  string
	calls int
}

func (p *nativeDeleteAPIModel) DeleteNativeTimer(ctx context.Context, sequence uint64) error {
	return reconcile.DeleteNativeTimerWithPort(ctx, p, sequence)
}

func (p *nativeDeleteAPIModel) DeleteRetainedTimer(ctx context.Context, sequence uint64) error {
	p.calls++
	if p.calls == 1 {
		p.schedule.RecordTransport(TransportEvent{Operation: "native_delete_api", Sequence: sequence, Outcome: p.mode})
		switch p.mode {
		case "unavailable":
			return &nats.APIError{Code: 503, ErrorCode: 10008, Description: "JetStream system temporarily unavailable"}
		case "already_absent", "ordinary_not_found", "lost_reply":
			if err := p.TimerScheduleTransport.DeleteNativeTimer(ctx, sequence); err != nil {
				return err
			}
			if p.mode == "lost_reply" {
				return nats.ErrTimeout
			}
			if p.mode == "ordinary_not_found" {
				return &nats.APIError{Code: 404, ErrorCode: 10037, Description: "no message found"}
			}
			return &nats.APIError{Code: 500, ErrorCode: 10057, Description: "no message found"}
		case "delete_denied":
			return &nats.APIError{Code: 500, ErrorCode: 10057, Description: "message delete not permitted"}
		case "store_failure":
			return &nats.APIError{Code: 500, ErrorCode: 10057, Description: "disk I/O failure"}
		}
	}
	return p.TimerScheduleTransport.DeleteNativeTimer(ctx, sequence)
}

func runNativeTimerDeleteReply(seed int64, replay *Trace) (trace Trace, runErr error) {
	var schedule *Scheduler
	if replay == nil {
		schedule = NewScheduler(seed)
	} else {
		var err error
		schedule, err = ReplayScheduler(*replay)
		if err != nil {
			return trace, err
		}
	}
	if err := schedule.SetWorkload("native_timer_delete_api_reply"); err != nil {
		return trace, err
	}
	defer func() { trace = schedule.Trace() }()
	mode, err := schedule.Choose([]string{"normal", "unavailable", "already_absent", "ordinary_not_found", "lost_reply", "delete_denied", "store_failure", "canceled"})
	if err != nil {
		return trace, err
	}
	kind, err := schedule.Choose([]string{"timer", "suspended"})
	if err != nil {
		return trace, err
	}
	ctx := context.Background()
	base := time.Unix(1700000000, 0).UTC()
	inv := NewSignalTransport(schedule)
	generation, err := inv.PublishInvocation(ctx, &nats.Msg{Subject: identity.InvocationSubject("test", "one"), Data: []byte(`null`)})
	if err != nil {
		return trace, err
	}
	timers := NewTimerScheduleTransport(schedule, base)
	api := &nativeDeleteAPIModel{TimerScheduleTransport: timers, mode: mode}
	if _, err := worker.ScheduleTimerDeadlineWithPort(ctx, timers, true, "test", "one", generation, 1, worker.TimerDeadline{FireAt: base.Add(2 * time.Second), ClockDomain: "utc-quorum-v1", ScheduleAt: base.Add(62 * time.Second)}); err != nil {
		return trace, err
	}
	inv.SetJournal("test", "one", []journal.Record{{Entry: journal.Entry{Kind: journal.Started}}, {Entry: journal.Entry{Kind: journal.Completed}}})
	port := nativeRetirementScanPort{inv, api}
	scan := reconcile.NewTimerScanWithPort(port).Scan
	if kind == "suspended" {
		scan = reconcile.NewSuspendedScanWithPort(port).Scan
	}
	if mode == "canceled" {
		canceled, stop := context.WithCancel(ctx)
		stop()
		if err := reconcile.DeleteNativeTimerWithPort(canceled, api, 1); !errors.Is(err, context.Canceled) || api.calls != 0 {
			return trace, fmt.Errorf("canceled delete contacted transport: %v calls=%d", err, api.calls)
		}
		api.mode = "normal"
	}
	loop := NewLoopTransport(schedule)
	loopCtx, stop := context.WithCancel(ctx)
	defer stop()
	loop.StopAfterWaits(4, stop)
	loopErr := reconcile.RunLoopWithPort(loopCtx, loop, "delete-reply", kind, 100*time.Millisecond, 1, scan)
	permanent := mode == "delete_denied" || mode == "store_failure"
	if permanent {
		var typed *jetstream.APIError
		if !errors.As(loopErr, &typed) || typed.ErrorCode != 10057 || api.calls != 1 {
			return trace, fmt.Errorf("permanent deletion was swallowed or retried: %v calls=%d", loopErr, api.calls)
		}
	} else if loopErr != nil {
		return trace, fmt.Errorf("retryable/idempotent native deletion killed production loop: %w", loopErr)
	}
	subjects, err := timers.NativeTimerSubjects(ctx, "test", "one")
	if err != nil || len(subjects) != map[bool]int{true: 1, false: 0}[permanent] {
		return trace, fmt.Errorf("retained native hints=%v permanent=%t err=%v", subjects, permanent, err)
	}
	if mode == "unavailable" && api.calls != 2 {
		return trace, fmt.Errorf("unavailable delete did not retry: calls=%d", api.calls)
	}
	if permanent {
		// A corrected later caller can delete the same observed hint. The failed
		// request did not erase it or advance the scanner past the error.
		if _, err := scan(ctx, 1, 1, false); err != nil {
			return trace, err
		}
	}
	schedule.RecordTransport(TransportEvent{Operation: "check_native_delete_reply", Outcome: mode + "/" + kind})
	if err := schedule.Finish(); err != nil {
		return trace, err
	}
	return trace, nil
}

func TestSeededNativeTimerDeleteReplies(t *testing.T) {
	if path := os.Getenv("SIM_NATIVE_DELETE_OUT"); path != "" {
		seed := int64(42)
		if raw := os.Getenv("FAULT_SEED"); raw != "" {
			var err error
			seed, err = strconv.ParseInt(raw, 10, 64)
			if err != nil {
				t.Fatal(err)
			}
		}
		trace, err := runNativeTimerDeleteReply(seed, nil)
		if err != nil {
			_ = trace.Save(path)
			t.Fatalf("seed=%d trace=%s: %v", seed, path, err)
		}
		if err := trace.Save(path); err != nil {
			t.Fatal(err)
		}
		return
	}
	coverage := map[string]bool{}
	for seed := range seededSchedules(t) {
		trace, err := runNativeTimerDeleteReply(seed, nil)
		if err != nil {
			path := os.Getenv("FAULT_TRACE_OUT")
			if path == "" {
				path = filepath.Join(t.TempDir(), "native-delete-failure.json")
			}
			_ = trace.Save(path)
			t.Fatalf("seed=%d trace=%s: %v", seed, path, err)
		}
		coverage[trace.Decisions[0].Chosen+"/"+trace.Decisions[1].Chosen] = true
		if seed <= 10 {
			again, err := runNativeTimerDeleteReply(seed, &trace)
			if err != nil || !reflect.DeepEqual(trace, again) {
				t.Fatalf("replay seed=%d: %v", seed, err)
			}
		}
	}
	if len(coverage) != 16 {
		t.Fatal("missing native delete reply coverage", coverage)
	}
}
