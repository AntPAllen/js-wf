package sim

import (
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

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"js-wf/client"
	"js-wf/worker"
)

// This models missing replies and the production notification decision, not
// the unknown server interleaving in the real seed-28 journal-leader failure.
type notificationBudgetPort struct {
	*SignalTransport
	schedule    *Scheduler
	mode        string
	stalled     bool
	budgetError error
}

func (p *notificationBudgetPort) missing(ctx context.Context) error {
	p.stalled = true
	deadline, ok := ctx.Deadline()
	if !ok || time.Until(deadline) > 15*time.Second {
		p.budgetError = fmt.Errorf("parent notification inherits delivery lifetime instead of 15s budget")
		return p.budgetError
	}
	if err := p.schedule.AdvanceMillis(15000); err != nil {
		return err
	}
	p.schedule.RecordTransport(TransportEvent{Operation: "parent_notification_missing_reply", Outcome: p.mode, AtMillis: p.schedule.NowMillis()})
	return context.DeadlineExceeded
}
func (p *notificationBudgetPort) LastInvocation(ctx context.Context, subject string) (*jetstream.RawStreamMsg, error) {
	if !p.stalled && p.mode == "invocation_drop" {
		return nil, p.missing(ctx)
	}
	return p.SignalTransport.LastInvocation(ctx, subject)
}
func (p *notificationBudgetPort) StateValue(ctx context.Context, key string) ([]byte, error) {
	if !p.stalled && p.mode == "state_drop" {
		return nil, p.missing(ctx)
	}
	return p.SignalTransport.StateValue(ctx, key)
}
func (p *notificationBudgetPort) PublishSignal(ctx context.Context, msg *nats.Msg, id string) (client.SignalPublishAck, error) {
	if !p.stalled && (p.mode == "publish_drop" || p.mode == "publish_ack_lost") {
		if p.mode == "publish_ack_lost" {
			if _, err := p.SignalTransport.PublishSignal(ctx, msg, id); err != nil {
				return client.SignalPublishAck{}, err
			}
		}
		return client.SignalPublishAck{}, p.missing(ctx)
	}
	return p.SignalTransport.PublishSignal(ctx, msg, id)
}
func runSeededParentNotificationBudget(seed int64, replay *Trace) (trace Trace, runErr error) {
	schedule := NewScheduler(seed)
	if replay != nil {
		var err error
		schedule, err = ReplayScheduler(*replay)
		if err != nil {
			return trace, err
		}
	}
	if err := schedule.SetWorkload("parent_notification_response_budget"); err != nil {
		return trace, err
	}
	defer func() { trace = schedule.Trace() }()
	mode, err := schedule.Choose([]string{"invocation_drop", "state_drop", "publish_drop", "publish_ack_lost"})
	if err != nil {
		return trace, err
	}
	ctx, stop := context.WithTimeout(context.Background(), 5*time.Minute)
	defer stop()
	transport := NewSignalTransport(schedule)
	port := &notificationBudgetPort{SignalTransport: transport, schedule: schedule, mode: mode}
	c := client.NewWithSignalPorts(transport, port)
	parent, err := c.Start(ctx, "parent", "p", []byte(`null`))
	if err != nil {
		return trace, err
	}
	child, err := c.StartChild(ctx, "child", "c", []byte(`null`), "parent", "p", parent.InvSeq, "result")
	if err != nil {
		return trace, err
	}
	headers := nats.Header{client.ParentTypeHeader: []string{"parent"}, client.ParentIDHeader: []string{"p"}, client.ParentInvSeqHeader: []string{strconv.FormatUint(parent.InvSeq, 10)}, client.ParentSignalHeader: []string{"result"}}
	payload := []byte(`{"result":"NDI="}`)
	if err = worker.NotifyParentWithClient(ctx, c, "child", "c", child.InvSeq, payload, headers); !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, client.ErrSignalUnknown) {
		return trace, fmt.Errorf("unbounded or incorrect initial notification: %v", err)
	}
	if port.budgetError != nil {
		return trace, port.budgetError
	}
	if schedule.NowMillis() != 15000 {
		return trace, fmt.Errorf("request budget virtual elapsed=%d", schedule.NowMillis())
	}
	for i := 0; i < 2; i++ {
		if err = worker.NotifyParentWithClient(ctx, c, "child", "c", child.InvSeq, payload, headers); err != nil {
			return trace, err
		}
	}
	transport.mu.Lock()
	count := len(transport.signals)
	transport.mu.Unlock()
	if count != 1 {
		return trace, fmt.Errorf("repaired/repeated parent notifications=%d", count)
	}
	schedule.RecordTransport(TransportEvent{Operation: "check_parent_notification_budget", Outcome: mode, Sequence: uint64(count), AtMillis: schedule.NowMillis()})
	if err = schedule.Finish(); err != nil {
		return trace, err
	}
	return schedule.Trace(), nil
}
func TestSeededParentNotificationBudgetReplay(t *testing.T) {
	if os.Getenv("SIM_NOTIFY_BUDGET_HELPER") == "1" {
		seed, err := strconv.ParseInt(os.Getenv("FAULT_SEED"), 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		trace, err := runSeededParentNotificationBudget(seed, nil)
		if err != nil {
			t.Fatal(err)
		}
		if err = trace.Save(os.Getenv("SIM_NOTIFY_BUDGET_OUT")); err != nil {
			t.Fatal(err)
		}
		return
	}
	modes := map[string]bool{}
	for seed, limit := int64(1), seededScheduleLimit(t); seed <= limit; seed++ {
		generated, err := runSeededParentNotificationBudget(seed, nil)
		if err != nil {
			path := os.Getenv("FAULT_TRACE_OUT")
			if path == "" {
				path = filepath.Join(t.TempDir(), "notification-failure.json")
			}
			_ = generated.Save(path)
			t.Fatalf("FAULT_SEED=%d FAULT_TRACE=%s: %v", seed, path, err)
		}
		modes[generated.Decisions[0].Chosen] = true
		if seed <= 10 {
			replayed, err := runSeededParentNotificationBudget(seed, &generated)
			if err != nil || !reflect.DeepEqual(generated, replayed) {
				t.Fatalf("seed=%d exact replay: %v", seed, err)
			}
		}
	}
	if len(modes) != 4 {
		t.Fatalf("missing notification modes: %v", modes)
	}
	var files [2]string
	for i := range files {
		files[i] = filepath.Join(t.TempDir(), fmt.Sprintf("notify-%d.json", i))
		cmd := exec.Command(os.Args[0], "-test.run=^TestSeededParentNotificationBudgetReplay$")
		cmd.Env = append(os.Environ(), "SIM_NOTIFY_BUDGET_HELPER=1", "FAULT_SEED=42", "SIM_NOTIFY_BUDGET_OUT="+files[i])
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("cross-process replay: %v: %s", err, out)
		}
	}
	a, err := os.ReadFile(files[0])
	if err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(files[1])
	if err != nil {
		t.Fatal(err)
	}
	if string(a) != string(b) {
		t.Fatal("notification trace differs across processes")
	}
}
