package integrity

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// This is a delivery-cost diagnostic, not a retained-state integrity gate.
// All paths validate every coordinate and payload on the same quiet R3 stream.
func TestAuditDeliveryNativeCostComparison(t *testing.T) {
	if os.Getenv("WF_AUDIT_DELIVERY_COST") != "1" {
		t.Skip("opt-in native iterator/adapter/callback delivery comparison")
	}
	js, ctx := batchedAuditCluster(t)
	stream := candidateNativeStream(t, js, ctx)
	const count = 100000
	payload := bytes.Repeat([]byte("d"), 128)
	for first := 1; first <= count; first += 128 {
		var pending []jetstream.PubAckFuture
		for seq := first; seq < first+128 && seq <= count; seq++ {
			data := bytes.Clone(payload)
			binary.BigEndian.PutUint64(data, uint64(seq))
			f, err := js.PublishAsync("audit.fault.delivery", data)
			if err != nil {
				t.Fatal(err)
			}
			pending = append(pending, f)
		}
		for n, f := range pending {
			select {
			case ack := <-f.Ok():
				if ack.Stream != "AUDIT_FAULT" || ack.Sequence != uint64(first+n) {
					t.Fatalf("publish coordinate %+v", ack)
				}
			case err := <-f.Err():
				t.Fatal(err)
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
		}
	}
	type measurement struct {
		Mode           string
		Records        int
		ElapsedNS      int64
		AllocatedBytes uint64
		GCCycles       uint32
		CleanupNS      int64
	}
	var results []measurement
	type cleanupObservation struct {
		Mode                string
		DeletedConsumer     string
		ElapsedNS           int64
		StreamConsumerCount int
		Names               []string
		StreamLeader        string
		Error               string
	}
	var cleanups []cleanupObservation
	persist := func() error {
		if root := os.Getenv("WF_AUDIT_BATCH_ROOT"); root != "" {
			data, err := json.MarshalIndent(struct {
				Results  []measurement
				Cleanups []cleanupObservation
				Scope    string
			}{results, cleanups, "Quiet R3 100k x128B delivery; linked server allocations included; cleanup name/count convergence shares original30s reader context and is timed separately; not400k integrity/fault/default adoption"}, "", "  ")
			if err != nil {
				return err
			}
			return os.WriteFile(filepath.Join(root, t.Name(), "delivery-cost.json"), append(data, '\n'), 0600)
		}
		return nil
	}
	for _, mode := range []string{"next", "adapter", "consume", "callback_adapter", "buffered_callback_adapter", "next_recheck"} {
		c, err := stream.CreateConsumer(ctx, jetstream.ConsumerConfig{Name: "cost-" + mode, MemoryStorage: true, Replicas: 3, AckPolicy: jetstream.AckNonePolicy})
		if err != nil {
			t.Fatal(err)
		}
		call, cancel := context.WithTimeout(ctx, 30*time.Second)
		t.Cleanup(cancel)
		seen := 0
		validate := func(msg jetstream.Msg) error {
			s, seq, _, e := compactMessageCoordinates(msg)
			if e != nil {
				return e
			}
			if s != "AUDIT_FAULT" || seq != uint64(seen+1) || len(msg.Data()) != len(payload) || binary.BigEndian.Uint64(msg.Data()) != seq || !bytes.Equal(msg.Data()[8:], payload[8:]) {
				return fmt.Errorf("delivery mismatch at %d: %s/%d", seen, s, seq)
			}
			seen++
			return nil
		}
		var before, after runtime.MemStats
		runtime.ReadMemStats(&before)
		began := time.Now()
		if mode == "consume" {
			done := make(chan error, 1)
			finish := func(e error) {
				select {
				case done <- e:
				default:
				}
			}
			cc, e := c.Consume(func(msg jetstream.Msg) {
				if e := validate(msg); e != nil {
					finish(e)
					return
				}
				if seen == count {
					finish(nil)
				}
			}, jetstream.PullMaxBytes(8<<20), jetstream.PullExpiry(2*time.Second), jetstream.ConsumeErrHandler(func(_ jetstream.ConsumeContext, e error) { finish(e) }))
			err = e
			if e == nil {
				select {
				case err = <-done:
				case <-call.Done():
					err = call.Err()
				}
				cc.Stop()
				select {
				case <-cc.Closed():
				case <-call.Done():
					err = call.Err()
				}
			}
		} else if mode == "callback_adapter" || mode == "buffered_callback_adapter" {
			var d callbackDiagnosticReader
			var e error
			if mode == "buffered_callback_adapter" {
				d, e = newCallbackDeliveryWithBuffer(c, 256, 1<<20)
			} else {
				d, e = newCallbackDelivery(c)
			}
			err = e
			if e == nil {
				for seen < count && err == nil {
					batch := byteDeliveryBatch(call, min(4096, count-seen), func() (jetstream.Msg, error) { return d.next(call) }, d.stop, false)
					for msg := range batch.Messages() {
						if e := validate(msg); e != nil && err == nil {
							err = e
						}
					}
					if err == nil {
						err = batch.Error()
					}
				}
				err = errors.Join(err, d.stopAndJoin(call))
			}
		} else {
			it, e := c.Messages(jetstream.PullMaxBytes(8<<20), jetstream.PullExpiry(2*time.Second))
			err = e
			if e == nil {
				if mode == "adapter" {
					for seen < count && err == nil {
						n := min(4096, count-seen)
						batch := byteIteratorBatch(call, it, n, false)
						for msg := range batch.Messages() {
							if e := validate(msg); e != nil && err == nil {
								err = e
							}
						}
						if err == nil {
							err = batch.Error()
						}
					}
				} else {
					next := jetstream.NextContext(call)
					for seen < count && err == nil {
						msg, e := it.Next(next)
						if e != nil {
							err = e
							break
						}
						err = validate(msg)
					}
				}
				it.Stop()
			}
		}
		elapsed := time.Since(began)
		runtime.ReadMemStats(&after)
		if err != nil || seen != count {
			cancel()
			t.Fatalf("%s count=%d err=%v", mode, seen, err)
		}
		results = append(results, measurement{Mode: mode, Records: seen, ElapsedNS: int64(elapsed), AllocatedBytes: after.TotalAlloc - before.TotalAlloc, GCCycles: after.NumGC - before.NumGC})
		t.Logf("delivery cost %+v", results[len(results)-1])
		beganCleanup := time.Now()
		if e := stream.DeleteConsumer(call, c.CachedInfo().Name); e != nil {
			results[len(results)-1].CleanupNS = int64(time.Since(beganCleanup))
			cleanups = append(cleanups, cleanupObservation{Mode: mode, DeletedConsumer: c.CachedInfo().Name, ElapsedNS: results[len(results)-1].CleanupNS, Error: e.Error()})
			if saveErr := persist(); saveErr != nil {
				t.Fatal(saveErr)
			}
			t.Fatal(e)
		}
		for {
			observation := cleanupObservation{Mode: mode, DeletedConsumer: c.CachedInfo().Name, ElapsedNS: int64(time.Since(beganCleanup))}
			info, e := stream.Info(call)
			if e == nil {
				observation.StreamConsumerCount = info.State.Consumers
				if info.Cluster != nil {
					observation.StreamLeader = info.Cluster.Leader
				}
				if info.State.Msgs != count {
					e = fmt.Errorf("population changed to %d", info.State.Msgs)
				}
			}
			if e == nil {
				listed := stream.ConsumerNames(call)
				for name := range listed.Name() {
					observation.Names = append(observation.Names, name)
				}
				e = listed.Err()
			}
			if e != nil {
				observation.Error = e.Error()
			}
			cleanups = append(cleanups, observation)
			results[len(results)-1].CleanupNS = int64(time.Since(beganCleanup))
			if e := persist(); e != nil {
				t.Fatal(e)
			}
			t.Logf("delivery cleanup %+v", observation)
			if e != nil {
				t.Fatal(e)
			}
			if observation.StreamConsumerCount == 0 && len(observation.Names) == 0 {
				break
			}
			timer := time.NewTimer(10 * time.Millisecond)
			select {
			case <-timer.C:
			case <-call.Done():
				timer.Stop()
				t.Fatal(call.Err())
			}
		}
		cancel()
	}
	info, err := stream.Info(ctx)
	if err != nil || info.State.Consumers != 0 || info.State.Msgs != count {
		t.Fatalf("cleanup/population %+v/%v", info, err)
	}
	if err := persist(); err != nil {
		t.Fatal(err)
	}
}
