package sim

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"testing"
	"time"

	"js-wf/identity"
	"js-wf/journal"
	"js-wf/provision"
	"js-wf/reconcile"

	"github.com/nats-io/nats.go"
)

func runSuspendedNakRecovery(seed int64, replay *Trace) (trace Trace, runErr error) {
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
	if err := schedule.SetWorkload("suspended_lost_nak_recovery"); err != nil {
		return Trace{}, err
	}
	defer func() { trace = schedule.Trace() }()
	ctx := context.Background()
	base := time.Unix(1_700_000_000, 0).UTC()
	const typ, id = "test", "nak-recovery"
	model := NewSignalTransport(schedule)
	scan := reconcile.NewSuspendedScanWithPort(model)
	scan.Now = func() time.Time { return base.Add(time.Duration(schedule.NowMillis()) * time.Millisecond) }
	invSeq, err := model.PublishInvocation(ctx, &nats.Msg{Subject: identity.InvocationSubject(typ, id), Data: []byte(`null`)})
	if err != nil {
		return trace, err
	}
	signal := &nats.Msg{Subject: "wf.sig.test.nak-recovery.go", Data: []byte(`ready`), Header: nats.Header{}}
	signal.Header.Set("Wf-Inv-Seq", strconv.FormatUint(invSeq, 10))
	signalSeq := model.CommitSignal(signal)
	records, err := suspendedFixture(invSeq, "matching_signal", base.Add(-time.Second), signalSeq)
	if err != nil {
		return trace, err
	}
	model.SetJournal(typ, id, records)
	lag, err := schedule.Choose([]string{"0", "1000", "2000", "3000"})
	if err != nil {
		return trace, err
	}
	lagMillis, _ := strconv.ParseInt(lag, 10, 64)
	if err := schedule.AdvanceMillis(lagMillis); err != nil {
		return trace, err
	}
	firstAt := schedule.NowMillis()
	if result, err := scan.Scan(ctx, invSeq, 1, false); err != nil || result.Reenqueued != 1 || len(model.Runs()) != 1 {
		return trace, fmt.Errorf("seed %d initial scan=%+v runs=%d err=%v", seed, result, len(model.Runs()), err)
	}
	dispatch := NewDispatchTransport(schedule, 20*time.Second)
	runs := model.Runs()
	dispatch.PublishRun(runs[0].Subject, runs[0].Data)
	partition := identity.Partition(typ, id, provision.Partitions)
	consumer, err := dispatch.Consumer(ctx, partition)
	if err != nil {
		return trace, err
	}
	batch, err := consumer.FetchOne(ctx)
	if err != nil {
		return trace, err
	}
	first := <-batch.Messages()
	if first == nil {
		return trace, fmt.Errorf("seed %d missing first wakeup", seed)
	}
	if err := dispatch.QueueFault(DispatchFault{Operation: "nak", Kind: "drop_before_commit"}); err != nil {
		return trace, err
	}
	if err := first.NakWithDelay(time.Second); !errors.Is(err, ErrTransportLost) {
		return trace, fmt.Errorf("seed %d lost nak: %v", seed, err)
	}
	if _, err := scan.Scan(ctx, invSeq, 1, false); err != nil || len(model.Runs()) != 1 {
		return trace, fmt.Errorf("seed %d same-window rescan runs=%d err=%v", seed, len(model.Runs()), err)
	}
	if err := schedule.AdvanceMillis(10_000 - schedule.NowMillis()); err != nil {
		return trace, err
	}
	if result, err := scan.Scan(ctx, invSeq, 1, false); err != nil || result.Reenqueued != 1 || len(model.Runs()) != 2 {
		return trace, fmt.Errorf("seed %d retry scan=%+v runs=%d err=%v", seed, result, len(model.Runs()), err)
	}
	if schedule.NowMillis()-firstAt >= 20_000 {
		return trace, fmt.Errorf("seed %d retry waited for AckWait", seed)
	}
	runs = model.Runs()
	dispatch.PublishRun(runs[1].Subject, runs[1].Data)
	batch, err = consumer.FetchOne(ctx)
	if err != nil {
		return trace, err
	}
	second := <-batch.Messages()
	meta, metaErr := second.Metadata()
	if metaErr != nil || meta.Sequence.Stream != 2 {
		return trace, fmt.Errorf("seed %d retry delivery=%+v err=%v", seed, meta, metaErr)
	}
	if err := second.Ack(); err != nil {
		return trace, err
	}
	model.SetJournal(typ, id, append(records, journal.Record{Entry: journal.Entry{Kind: journal.Completed, Index: uint64(len(records)), Epoch: 1}, Sequence: records[len(records)-1].Sequence + 1}))
	if result, err := scan.Scan(ctx, invSeq, 1, false); err != nil || result.Reenqueued != 0 || len(model.Runs()) != 2 {
		return trace, fmt.Errorf("seed %d terminal rescan=%+v runs=%d err=%v", seed, result, len(model.Runs()), err)
	}
	if err := schedule.AdvanceMillis(firstAt + 20_000 - schedule.NowMillis()); err != nil {
		return trace, err
	}
	batch, err = consumer.FetchOne(ctx)
	if err != nil {
		return trace, err
	}
	old := <-batch.Messages()
	meta, metaErr = old.Metadata()
	if metaErr != nil || meta.Sequence.Stream != 1 || meta.NumDelivered != 2 {
		return trace, fmt.Errorf("seed %d old redelivery=%+v err=%v", seed, meta, metaErr)
	}
	if err := old.Ack(); err != nil || dispatch.Pending() != 0 {
		return trace, fmt.Errorf("seed %d old wakeup ack=%v pending=%d", seed, err, dispatch.Pending())
	}
	schedule.RecordTransport(TransportEvent{Operation: "check_suspended_lost_nak_recovery", Subject: identity.JournalSubject(typ, id), Outcome: "retried_before_ackwait", AtMillis: schedule.NowMillis()})
	if err := schedule.Finish(); err != nil {
		return trace, err
	}
	return schedule.Trace(), nil
}

func TestSeededSuspendedLostNakRecovery(t *testing.T) {
	for seed := range seededSchedules(t) {
		generated, err := runSuspendedNakRecovery(seed, nil)
		if err != nil {
			t.Fatalf("FAULT_SEED=%d: %v", seed, err)
		}
		if seed <= 10 {
			replayed, err := runSuspendedNakRecovery(seed, &generated)
			if err != nil || !reflect.DeepEqual(generated, replayed) {
				t.Fatalf("FAULT_SEED=%d replay: %v", seed, err)
			}
		}
	}
}
