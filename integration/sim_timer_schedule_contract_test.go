package integration_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"js-wf/identity"
	"js-wf/provision"
	"js-wf/reconcile"
	"js-wf/sim"
	"js-wf/testcluster"
	"js-wf/worker"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

type hiddenTimerAckPort struct {
	worker.TimerSchedulePort
	used bool
}

type hiddenFallbackAckPort struct {
	reconcile.FallbackTimerScanPort
	publishHidden bool
	deleteHidden  bool
}

func (p *hiddenFallbackAckPort) PublishWakeup(ctx context.Context, message *nats.Msg, messageID string) error {
	err := p.FallbackTimerScanPort.PublishWakeup(ctx, message, messageID)
	if err == nil && !p.publishHidden {
		p.publishHidden = true
		return nats.ErrTimeout
	}
	return err
}

func (p *hiddenFallbackAckPort) DeleteTimer(ctx context.Context, sequence uint64) error {
	err := p.FallbackTimerScanPort.DeleteTimer(ctx, sequence)
	if err == nil && !p.deleteHidden {
		p.deleteHidden = true
		return nats.ErrTimeout
	}
	return err
}

func (p *hiddenTimerAckPort) PublishFallback(ctx context.Context, subject string, payload []byte, messageID string) (bool, error) {
	duplicate, err := p.TimerSchedulePort.PublishFallback(ctx, subject, payload, messageID)
	if err == nil && !duplicate && !p.used {
		p.used = true
		return false, nats.ErrTimeout
	}
	return duplicate, err
}

func (p *hiddenTimerAckPort) PublishNative(ctx context.Context, message *nats.Msg, messageID string) (bool, error) {
	duplicate, err := p.TimerSchedulePort.PublishNative(ctx, message, messageID)
	if err == nil && !duplicate && !p.used {
		p.used = true
		return false, nats.ErrTimeout
	}
	return duplicate, err
}

func TestSimTimerScheduleContractAgainstRealCluster(t *testing.T) {
	all, _ := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	run, err := all[0].Stream(ctx, "WF_RUN")
	if err != nil {
		t.Fatal(err)
	}
	fallback, err := all[0].CreateStream(ctx, jetstream.StreamConfig{
		Name: "WF_TIMER", Subjects: []string{"wf.timer.*.*.*.*"},
		Retention: jetstream.LimitsPolicy, Storage: jetstream.FileStorage,
		Replicas: 3, Discard: jetstream.DiscardNew, Duplicates: 2 * time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	base := time.Now().UTC()
	fireAt := base.Add(2 * time.Second)
	clock := sim.NewScheduler(72)
	model := sim.NewTimerScheduleTransport(clock, base)
	realPort := worker.NewTimerSchedulePort(all[0])
	var nativeIDs, fallbackIDs []string
	for _, scenario := range []struct {
		id     string
		native bool
		lost   bool
	}{
		{id: "native", native: true},
		{id: "native-lost", native: true, lost: true},
		{id: "fallback"},
		{id: "fallback-lost", lost: true},
	} {
		port := realPort
		if scenario.lost {
			port = &hiddenTimerAckPort{TimerSchedulePort: realPort}
			if err := model.QueueFault(sim.LoseAckAfterCommit); err != nil {
				t.Fatal(err)
			}
		}
		call := func(p worker.TimerSchedulePort) (bool, error) {
			return worker.ScheduleTimerWithPort(ctx, p, scenario.native, "test", scenario.id, 7, 3, fireAt)
		}
		modelNew, modelErr := call(model)
		realNew, realErr := call(port)
		if scenario.lost {
			if modelNew || realNew || !errors.Is(modelErr, sim.ErrTransportLost) || !errors.Is(realErr, nats.ErrTimeout) {
				t.Fatalf("%s lost ack model=%t/%v real=%t/%v", scenario.id, modelNew, modelErr, realNew, realErr)
			}
		} else if modelErr != nil || realErr != nil || !modelNew || !realNew {
			t.Fatalf("%s first publish model=%t/%v real=%t/%v", scenario.id, modelNew, modelErr, realNew, realErr)
		}
		modelNew, modelErr = call(model)
		realNew, realErr = call(realPort)
		if modelErr != nil || realErr != nil || modelNew || realNew {
			t.Fatalf("%s duplicate model=%t/%v real=%t/%v", scenario.id, modelNew, modelErr, realNew, realErr)
		}
		if scenario.native {
			nativeIDs = append(nativeIDs, scenario.id)
		} else {
			fallbackIDs = append(fallbackIDs, scenario.id)
		}
	}
	if got := len(model.NativeSources()); got != 2 {
		t.Fatalf("modeled native sources=%d", got)
	}
	for i, id := range nativeIDs {
		source := fmt.Sprintf("wf.schedule.test.%s.3", id)
		stored, err := run.GetLastMsgForSubject(ctx, source)
		if err != nil {
			t.Fatalf("native source %s: %v", id, err)
		}
		modeled := model.NativeSources()[i]
		if stored.Header.Get(jetstream.ScheduleHeader) != modeled.Header.Get(jetstream.ScheduleHeader) ||
			stored.Header.Get(jetstream.ScheduleTargetHeader) != modeled.Header.Get(jetstream.ScheduleTargetHeader) ||
			stored.Header.Get(identity.TimerInvSeqHeader) != modeled.Header.Get(identity.TimerInvSeqHeader) ||
			stored.Header.Get(identity.TimerStepHeader) != modeled.Header.Get(identity.TimerStepHeader) ||
			!bytes.Equal(stored.Data, modeled.Data) {
			t.Fatalf("native source %s real=%+v model=%+v", id, stored, modeled)
		}
	}
	for i, id := range fallbackIDs {
		stored, err := fallback.GetLastMsgForSubject(ctx, identity.TimerSubject("test", id, 7, 3))
		if err != nil || !bytes.Equal(stored.Data, model.FallbackRecords()[i].Data) {
			t.Fatalf("fallback %s real=%+v model=%+v err=%v", id, stored, model.FallbackRecords()[i], err)
		}
	}
	if err := model.Advance(2500 * time.Millisecond); err != nil {
		t.Fatal(err)
	}
	if len(model.Runs()) != 2 {
		t.Fatalf("modeled target deliveries=%d", len(model.Runs()))
	}
	for _, id := range nativeIDs {
		target := identity.RunSubject("test", id, provision.Partitions)
		var stored *jetstream.RawStreamMsg
		for ctx.Err() == nil {
			stored, err = run.GetLastMsgForSubject(ctx, target)
			if err == nil {
				break
			}
			if !errors.Is(err, jetstream.ErrMsgNotFound) {
				t.Fatal(err)
			}
			time.Sleep(25 * time.Millisecond)
		}
		if err != nil || stored == nil || string(stored.Data) != identity.Key("test", id) || stored.Header.Get(identity.TimerInvSeqHeader) != "7" {
			t.Fatalf("native target %s: stored=%+v err=%v", id, stored, err)
		}
		if _, err := run.GetMsg(ctx, stored.Sequence+1, jetstream.WithGetMsgSubject(target)); !errors.Is(err, jetstream.ErrMsgNotFound) {
			t.Fatalf("native target %s duplicated: %v", id, err)
		}
	}
	realScanPort := reconcile.NewFallbackTimerScanPort(all[0])
	hidden := &hiddenFallbackAckPort{FallbackTimerScanPort: realScanPort}
	serverNow := func(ctx context.Context) (time.Time, error) {
		info, err := run.Info(ctx)
		if err != nil {
			return time.Time{}, err
		}
		return info.TimeStamp, nil
	}
	realScan := reconcile.NewFallbackTimerScanWithPort(hidden, serverNow)
	modelScan := reconcile.NewFallbackTimerScanWithPort(model, func(context.Context) (time.Time, error) {
		return base.Add(time.Duration(clock.NowMillis()) * time.Millisecond), nil
	})
	if err := model.QueueWakeupFault(sim.LoseAckAfterCommit); err != nil {
		t.Fatal(err)
	}
	if err := model.QueueDeleteFault(sim.LoseAckAfterCommit); err != nil {
		t.Fatal(err)
	}
	for attempt := 0; attempt < 3; attempt++ {
		modeled, modelErr := modelScan.Scan(ctx, 1, 10, false)
		real, realErr := realScan.Scan(ctx, 1, 10, false)
		if attempt < 2 {
			if !errors.Is(modelErr, sim.ErrTransportLost) || !errors.Is(realErr, nats.ErrTimeout) || modeled.Reenqueued != 1 || real.Reenqueued != 1 {
				t.Fatalf("fallback attempt %d model=%+v/%v real=%+v/%v", attempt, modeled, modelErr, real, realErr)
			}
		} else if modelErr != nil || realErr != nil || modeled.Reenqueued != 1 || real.Reenqueued != 1 || len(model.RetainedFallbackRecords()) != 0 {
			t.Fatalf("fallback final model=%+v/%v real=%+v/%v retained=%d", modeled, modelErr, real, realErr, len(model.RetainedFallbackRecords()))
		}
	}
	if len(model.Runs()) != 4 {
		t.Fatalf("modeled routed timer wakeups=%d", len(model.Runs()))
	}
	for _, id := range fallbackIDs {
		subject := identity.TimerSubject("test", id, 7, 3)
		if _, err := fallback.GetLastMsgForSubject(ctx, subject); !errors.Is(err, jetstream.ErrMsgNotFound) {
			t.Fatalf("fallback timer %s remained: %v", id, err)
		}
		target := identity.RunSubject("test", id, provision.Partitions)
		stored, err := run.GetLastMsgForSubject(ctx, target)
		if err != nil || string(stored.Data) != identity.Key("test", id) || stored.Header.Get(identity.TimerInvSeqHeader) != "7" {
			t.Fatalf("fallback target %s=%+v err=%v", id, stored, err)
		}
		if _, err := run.GetMsg(ctx, stored.Sequence+1, jetstream.WithGetMsgSubject(target)); !errors.Is(err, jetstream.ErrMsgNotFound) {
			t.Fatalf("fallback target %s duplicated: %v", id, err)
		}
	}
}

func TestSimNativeTimerQuorumHealContractAgainstRealCluster(t *testing.T) {
	cluster, err := testcluster.StartPartitionable(t.TempDir(), 3)
	if err != nil {
		t.Fatal(err)
	}
	all, cluster := setupCluster(t, cluster)
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	const typ, id = "timer-contract", "route-quorum"
	base := time.Now().UTC()
	fireAt := base.Add(8 * time.Second)
	clock := sim.NewScheduler(73)
	model := sim.NewTimerScheduleTransport(clock, base)
	if _, err := worker.ScheduleTimerWithPort(ctx, model, true, typ, id, 7, 3, fireAt); err != nil {
		t.Fatal(err)
	}
	if _, err := worker.ScheduleTimerWithPort(ctx, worker.NewTimerSchedulePort(all[0]), true, typ, id, 7, 3, fireAt); err != nil {
		t.Fatal(err)
	}
	mesh := cluster.RouteMesh()
	defer mesh.Heal()
	if err := mesh.PartitionNode(0); err != nil {
		t.Fatal(err)
	}
	if err := waitTimerRouteCounts(ctx, cluster, 0); err != nil {
		t.Fatal(err)
	}
	cluster.KillNode(1) // Node 0 is isolated; node 2 alone has no quorum.
	if time.Until(fireAt) < time.Second {
		t.Fatal("failed to remove schedule quorum before timer due time")
	}
	model.SetScheduleQuorum(false)
	if err := model.Advance(16 * time.Second); err != nil || len(model.Runs()) != 0 {
		t.Fatalf("modeled due target before heal: runs=%d err=%v", len(model.Runs()), err)
	}
	if wait := time.Until(fireAt.Add(time.Second)); wait > 0 {
		select {
		case <-time.After(wait):
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	healedAt := time.Now()
	mesh.Heal()
	if err := cluster.RestartNode(1); err != nil {
		t.Fatal(err)
	}
	if err := waitTimerRouteCounts(ctx, cluster, -1); err != nil {
		t.Fatal(err)
	}
	quorumReadyAt := time.Now()
	model.SetScheduleQuorum(true)
	if len(model.Runs()) != 1 {
		t.Fatalf("modeled target after quorum recovery=%d", len(model.Runs()))
	}
	target := identity.RunSubject(typ, id, provision.Partitions)
	var delivered *jetstream.RawStreamMsg
	var lastErr error
	for ctx.Err() == nil {
		attempt, stop := context.WithTimeout(ctx, 2*time.Second)
		run, err := all[2].Stream(attempt, "WF_RUN")
		if err == nil {
			delivered, err = run.GetLastMsgForSubject(attempt, target)
		}
		stop()
		if err == nil {
			break
		}
		lastErr = err
		time.Sleep(50 * time.Millisecond)
	}
	if delivered == nil || string(delivered.Data) != identity.Key(typ, id) || delivered.Header.Get(identity.TimerInvSeqHeader) != "7" {
		t.Fatalf("real target after heal=%+v last_error=%v routes=%d/%d/%d context=%v", delivered, lastErr, cluster.Servers[0].NumRoutes(), cluster.Servers[1].NumRoutes(), cluster.Servers[2].NumRoutes(), ctx.Err())
	}
	run, err := all[2].Stream(ctx, "WF_RUN")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := run.GetMsg(ctx, delivered.Sequence+1, jetstream.WithGetMsgSubject(target)); !errors.Is(err, jetstream.ErrMsgNotFound) {
		t.Fatalf("duplicate native target after route heal: %v", err)
	}
	t.Logf("native timer target visible %s after route heal and %s after all routes returned", time.Since(healedAt), time.Since(quorumReadyAt))
	if err := model.Advance(time.Second); err != nil || len(model.Runs()) != 1 {
		t.Fatalf("modeled duplicate after heal: runs=%d err=%v", len(model.Runs()), err)
	}
}
