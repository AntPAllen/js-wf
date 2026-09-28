package integration_test

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"js-wf/client"
	"js-wf/identity"
	"js-wf/journal"
	"js-wf/reconcile"
	"js-wf/sim"

	"github.com/nats-io/nats.go"
)

// Compare the modeled timer repair decisions with retained state on three
// real JetStream nodes, including a deleted invocation sequence hole.
func TestSimTimerScanContractAgainstRealCluster(t *testing.T) {
	all, _ := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	model := sim.NewSignalTransport(sim.NewScheduler(91))
	modelScan := reconcile.NewTimerScanWithPort(model)
	realScan := reconcile.NewTimerScan(all[2])
	c := client.New(all[0])
	j := journal.New(all[0])
	run, err := all[2].Stream(ctx, "WF_RUN")
	if err != nil {
		t.Fatal(err)
	}
	inv, err := all[0].Stream(ctx, "WF_INV")
	if err != nil {
		t.Fatal(err)
	}
	fixtures := []struct {
		id       string
		kind     string
		fireAt   time.Time
		complete bool
		terminal bool
		delete   bool
		wantRun  int
	}{
		{id: "due", kind: "timer", fireAt: time.Now().Add(-time.Hour), wantRun: 1},
		{id: "future", kind: "timer_start", fireAt: time.Now().Add(time.Hour)},
		{id: "completed", kind: "timer_await", fireAt: time.Now().Add(-time.Hour), complete: true},
		{id: "terminal", kind: "timer", fireAt: time.Now().Add(-time.Hour), terminal: true},
		{id: "hole", delete: true},
	}
	sequences := make([]uint64, len(fixtures))
	for i, fixture := range fixtures {
		handle, err := c.Start(ctx, "test", fixture.id, []byte(`null`))
		if err != nil {
			t.Fatal(err)
		}
		sequences[i] = handle.InvSeq
		modeled, err := model.PublishInvocation(ctx, &nats.Msg{Subject: identity.InvocationSubject("test", fixture.id), Data: []byte(`null`)})
		if err != nil || modeled != handle.InvSeq {
			t.Fatalf("invocation %s real=%d model=%d err=%v", fixture.id, handle.InvSeq, modeled, err)
		}
		if fixture.delete {
			if err := inv.DeleteMsg(ctx, handle.InvSeq); err != nil {
				t.Fatal(err)
			}
			model.PurgeInvocation(identity.InvocationSubject("test", fixture.id))
			continue
		}
		seq, err := j.Append(ctx, "test", fixture.id, journal.Entry{Index: 0, Kind: journal.Started}, 0)
		if err != nil {
			t.Fatal(err)
		}
		payload, err := json.Marshal(struct {
			Kind   string    `json:"kind"`
			FireAt time.Time `json:"fire_at"`
		}{fixture.kind, fixture.fireAt})
		if err != nil {
			t.Fatal(err)
		}
		seq, err = j.Append(ctx, "test", fixture.id, journal.Entry{Index: 1, Kind: journal.StepRequested, Payload: payload}, seq)
		if err != nil {
			t.Fatal(err)
		}
		if fixture.complete || fixture.terminal {
			kind := journal.StepCompleted
			if fixture.terminal {
				kind = journal.Completed
			}
			if _, err := j.Append(ctx, "test", fixture.id, journal.Entry{Index: 2, Kind: kind}, seq); err != nil {
				t.Fatal(err)
			}
		}
		records, _, err := j.Read(ctx, "test", fixture.id)
		if err != nil {
			t.Fatal(err)
		}
		model.SetJournal("test", fixture.id, records)
	}
	if err := run.Purge(ctx); err != nil {
		t.Fatal(err)
	}
	modelBaseline := len(model.Runs())
	var wantRuns uint64
	for i, fixture := range fixtures {
		modeled, modelErr := modelScan.Scan(ctx, sequences[i], 1, false)
		real, realErr := realScan.Scan(ctx, sequences[i], 1, false)
		if modelErr != nil || realErr != nil || !reflect.DeepEqual(modeled, real) || real.Reenqueued != fixture.wantRun {
			t.Fatalf("case %s model=%+v err=%v real=%+v err=%v", fixture.id, modeled, modelErr, real, realErr)
		}
		wantRuns += uint64(fixture.wantRun)
		info, err := run.Info(ctx)
		if err != nil || info.State.Msgs != wantRuns || len(model.Runs()) != modelBaseline+int(wantRuns) {
			t.Fatalf("case %s run count real=%+v model=%d baseline=%d want=%d err=%v", fixture.id, info, len(model.Runs()), modelBaseline, wantRuns, err)
		}
	}
	modeled, modelErr := modelScan.Scan(ctx, sequences[0], 1, false)
	real, realErr := realScan.Scan(ctx, sequences[0], 1, false)
	info, err := run.Info(ctx)
	if modelErr != nil || realErr != nil || err != nil || !reflect.DeepEqual(modeled, real) || info.State.Msgs != wantRuns || len(model.Runs()) != modelBaseline+int(wantRuns) {
		t.Fatalf("repeat due timer model=%+v err=%v real=%+v err=%v run=%+v infoErr=%v", modeled, modelErr, real, realErr, info, err)
	}
}
