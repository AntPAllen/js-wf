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
	"js-wf/sim"
	"js-wf/worker"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

type hiddenTimerAckPort struct {
	worker.TimerSchedulePort
	used bool
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
	model := sim.NewTimerScheduleTransport(sim.NewScheduler(72), base)
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
}
