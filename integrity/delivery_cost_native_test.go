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
	}
	var results []measurement
	for _, mode := range []string{"next", "adapter", "consume", "callback_adapter", "buffered_callback_adapter", "next_recheck"} {
		c, err := stream.CreateConsumer(ctx, jetstream.ConsumerConfig{Name: "cost-" + mode, MemoryStorage: true, Replicas: 3, AckPolicy: jetstream.AckNonePolicy})
		if err != nil {
			t.Fatal(err)
		}
		call, cancel := context.WithTimeout(ctx, 30*time.Second)
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
			create := newCallbackDelivery
			if mode == "buffered_callback_adapter" {
				create = func(c jetstream.Consumer) (*callbackDelivery, error) {
					return newCallbackDeliveryWithBuffer(c, 256, 1<<20)
				}
			}
			d, e := create(c)
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
		cancel()
		if e := stream.DeleteConsumer(ctx, c.CachedInfo().Name); e != nil {
			t.Fatal(e)
		}
		if err != nil || seen != count {
			t.Fatalf("%s count=%d err=%v", mode, seen, err)
		}
		results = append(results, measurement{mode, seen, int64(elapsed), after.TotalAlloc - before.TotalAlloc, after.NumGC - before.NumGC})
		t.Logf("delivery cost %+v", results[len(results)-1])
	}
	info, err := stream.Info(ctx)
	if err != nil || info.State.Consumers != 0 || info.State.Msgs != count {
		t.Fatalf("cleanup/population %+v/%v", info, err)
	}
	if root := os.Getenv("WF_AUDIT_BATCH_ROOT"); root != "" {
		data, err := json.MarshalIndent(struct {
			Results []measurement
			Scope   string
		}{results, "Quiet R3 100k x128B delivery only; allocations include linked in-process server activity; not 400k integrity, fault recovery, or default adoption"}, "", "  ")
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(root, t.Name(), "delivery-cost.json"), append(data, '\n'), 0600); err != nil {
			t.Fatal(err)
		}
	}
}
