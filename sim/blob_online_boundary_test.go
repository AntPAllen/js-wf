package sim

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"js-wf/client"
	"js-wf/identity"
	"js-wf/retention"
)

// This is an adversarial contract for violating the quiescent precondition,
// not an online collector. The actual Start and collector decisions run here.
type onlineBoundaryStart struct {
	*StartTransport
	blobs       *BlobSweepTransport
	scheduler   *Scheduler
	afterUpload func() error
}

func (p *onlineBoundaryStart) PutInput(ctx context.Context, name string, data []byte) error {
	if err := p.StartTransport.PutInput(ctx, name, data); err != nil {
		return err
	}
	p.blobs.PutObject(name, data, time.UnixMilli(p.scheduler.NowMillis()).UTC())
	if p.afterUpload != nil {
		hook := p.afterUpload
		p.afterUpload = nil
		return hook()
	}
	return nil
}
func (p *onlineBoundaryStart) PublishInvocation(ctx context.Context, msg *nats.Msg) (uint64, error) {
	seq, err := p.StartTransport.PublishInvocation(ctx, msg)
	if err == nil {
		_, err = p.blobs.PublishSubject("WF_INV", msg.Subject, msg.Header, msg.Data)
	}
	return seq, err
}

type onlineBoundarySweep struct {
	*BlobSweepTransport
	beforeDelete func() error
}

func (p *onlineBoundarySweep) DeleteObject(ctx context.Context, name string) error {
	if p.beforeDelete != nil {
		hook := p.beforeDelete
		p.beforeDelete = nil
		if err := hook(); err != nil {
			return err
		}
	}
	return p.BlobSweepTransport.DeleteObject(ctx, name)
}

func runOnlineBlobBoundary(seed int64, replay *Trace) (trace Trace, runErr error) {
	schedule := NewScheduler(seed)
	if replay != nil {
		var err error
		schedule, err = ReplayScheduler(*replay)
		if err != nil {
			return Trace{}, err
		}
	}
	if err := schedule.SetWorkload("online_blob_boundary"); err != nil {
		return Trace{}, err
	}
	defer func() { trace = schedule.Trace() }()
	mode, err := schedule.Choose([]string{"paused_upload", "refresh_after_census", "quiescent_control"})
	if err != nil {
		return trace, err
	}
	ctx := context.Background()
	blobs := NewBlobSweepTransport(schedule)
	start := &onlineBoundaryStart{StartTransport: NewStartTransport(schedule), blobs: blobs, scheduler: schedule}
	sweep := &onlineBoundarySweep{BlobSweepTransport: blobs}
	payload := bytes.Repeat([]byte("x"), client.MaxInlineInput+1)
	var object string
	// Derive the production key from the actual uploaded bytes rather than assume
	// input object naming. The refreshed orphan uses that exact shared key.
	var result retention.BlobSweepResult
	switch mode {
	case "paused_upload":
		start.afterUpload = func() error {
			if err := schedule.AdvanceMillis((2 * time.Hour).Milliseconds()); err != nil {
				return err
			}
			var err error
			result, err = retention.SweepBlobsQuiescentWithPort(ctx, sweep, time.Hour, time.UnixMilli(schedule.NowMillis()).UTC())
			return err
		}
		if _, err = client.NewWithStartPort(start).Start(ctx, "gc-boundary", "paused", payload); err != nil {
			return trace, err
		}
	case "refresh_after_census":
		// Use one initial completed Start only to obtain the exact production key,
		// then remove its retained reference, making a genuinely retired candidate.
		if _, err = client.NewWithStartPort(start).Start(ctx, "gc-boundary", "old", payload); err != nil {
			return trace, err
		}
		old, _ := start.Invocation(identity.InvocationSubject("gc-boundary", "old"))
		object = old.Header.Get("Wf-Input-Ref")
		start.PurgeInvocation(identity.InvocationSubject("gc-boundary", "old"))
		if err = blobs.Purge("WF_INV", 1); err != nil {
			return trace, err
		}
		if err = schedule.AdvanceMillis((2 * time.Hour).Milliseconds()); err != nil {
			return trace, err
		}
		sweep.beforeDelete = func() error {
			_, err := client.NewWithStartPort(start).Start(ctx, "gc-boundary", "refreshed", payload)
			return err
		}
		result, err = retention.SweepBlobsQuiescentWithPort(ctx, sweep, time.Hour, time.UnixMilli(schedule.NowMillis()).UTC())
		if err != nil {
			return trace, err
		}
	case "quiescent_control":
		if _, err = client.NewWithStartPort(start).Start(ctx, "gc-boundary", "kept", payload); err != nil {
			return trace, err
		}
		if err = schedule.AdvanceMillis((2 * time.Hour).Milliseconds()); err != nil {
			return trace, err
		}
		result, err = retention.SweepBlobsQuiescentWithPort(ctx, sweep, time.Hour, time.UnixMilli(schedule.NowMillis()).UTC())
		if err != nil {
			return trace, err
		}
	}
	id := map[string]string{"paused_upload": "paused", "refresh_after_census": "refreshed", "quiescent_control": "kept"}[mode]
	invocation, ok := start.Invocation(identity.InvocationSubject("gc-boundary", id))
	if !ok {
		return trace, fmt.Errorf("acknowledged invocation missing")
	}
	object = invocation.Header.Get("Wf-Input-Ref")
	if object == "" {
		return trace, fmt.Errorf("production input did not spill")
	}
	dangling := !blobs.HasObject(object)
	if mode == "quiescent_control" {
		if dangling || result.Deleted != 0 || result.Referenced != 1 {
			return trace, fmt.Errorf("quiescent control damaged referenced input: %+v", result)
		}
	} else if !dangling || result.Deleted != 1 {
		return trace, fmt.Errorf("expected active-writer counterexample absent: mode=%s result=%+v dangling=%t", mode, result, dangling)
	}
	schedule.RecordTransport(TransportEvent{Operation: "check_online_blob_boundary", Subject: object, Sequence: uint64(result.Deleted), Outcome: mode})
	if err = schedule.Finish(); err != nil {
		return trace, err
	}
	return schedule.Trace(), nil
}

func TestSeededOnlineBlobBoundaryReplay(t *testing.T) {
	observed := map[string]int{}
	for seed := range seededSchedules(t) {
		generated, err := runOnlineBlobBoundary(seed, nil)
		if err != nil {
			t.Fatalf("FAULT_SEED=%d: %v", seed, err)
		}
		mode := generated.Decisions[0].Chosen
		observed[mode]++
		replayed, err := runOnlineBlobBoundary(seed, &generated)
		if err != nil || !reflect.DeepEqual(generated, replayed) {
			t.Fatalf("FAULT_SEED=%d replay differs: %v", seed, err)
		}
		if root := os.Getenv("SIM_ONLINE_BLOB_BOUNDARY_ROOT"); root != "" && observed[mode] == 1 {
			if err = generated.Save(filepath.Join(root, fmt.Sprintf("seed-%03d.json", seed))); err != nil {
				t.Fatal(err)
			}
		}
	}
	if len(observed) != 3 {
		t.Fatalf("boundary coverage=%v", observed)
	}
	t.Logf("online blob boundary: modes=%v active-writer failures expected; every generated trace exactly replayed; quiescent control preserved", observed)
}
