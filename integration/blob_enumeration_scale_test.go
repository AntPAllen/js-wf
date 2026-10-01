package integration_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"js-wf/retention"
)

// The default crosses the server's 100,000-subject stream-info page for
// both Object Store and KV metadata. Smaller diagnostics are not scale proof.
func TestQuiescentBlobSweepPaginationBoundary(t *testing.T) {
	if os.Getenv("WF_BLOB_ENUMERATION_SCALE") == "" {
		t.Skip("set WF_BLOB_ENUMERATION_SCALE=1 for the real metadata pagination boundary")
	}
	count := 100005
	if raw := os.Getenv("WF_BLOB_ENUMERATION_COUNT"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n < 100 || n > count {
			t.Fatalf("invalid WF_BLOB_ENUMERATION_COUNT %q", raw)
		}
		count = n
	}
	all, _ := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	defer cancel()
	objects, err := all[0].ObjectStore(ctx, "WF_BLOB")
	if err != nil {
		t.Fatal(err)
	}
	state, err := all[0].KeyValue(ctx, "WF_STATE")
	if err != nil {
		t.Fatal(err)
	}
	const protected = "terminal-result-protected"
	for _, name := range []string{protected, "input-orphan", "input-deleted"} {
		if _, err := objects.PutBytes(ctx, name, []byte(name)); err != nil {
			t.Fatal(err)
		}
	}
	if err := objects.Delete(ctx, "input-deleted"); err != nil {
		t.Fatal(err)
	}
	jobs := make(chan int, 64)
	var wg sync.WaitGroup
	var done atomic.Int64
	var once sync.Once
	var firstErr error
	start := time.Now()
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for index := range jobs {
				name := fmt.Sprintf("user-scale-%06d", index)
				if _, err := objects.PutBytes(ctx, name, []byte(name)); err != nil {
					once.Do(func() { firstErr = fmt.Errorf("object %s: %w", name, err); cancel() })
					return
				}
				value := []byte("0")
				if index == count-1 {
					value = []byte(`{"result_ref":"terminal-result-protected"}`)
				}
				if _, err := state.Put(ctx, fmt.Sprintf("scale.%06d", index), value); err != nil {
					once.Do(func() { firstErr = fmt.Errorf("state %d: %w", index, err); cancel() })
					return
				}
				if n := done.Add(1); n%10000 == 0 {
					t.Logf("stored %d/%d real objects and KV keys in %s", n, count, time.Since(start))
				}
			}
		}()
	}
produce:
	for i := 0; i < count; i++ {
		select {
		case jobs <- i:
		case <-ctx.Done():
			break produce
		}
	}
	close(jobs)
	wg.Wait()
	if firstErr != nil || done.Load() != int64(count) {
		t.Fatalf("stored=%d want=%d err=%v context=%v", done.Load(), count, firstErr, ctx.Err())
	}
	// Independent raw stream censuses establish that pagination is required.
	for _, name := range []string{"OBJ_WF_BLOB", "KV_WF_STATE"} {
		stream, err := all[0].Stream(ctx, name)
		if err != nil {
			t.Fatal(err)
		}
		info, err := stream.Info(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if name == "KV_WF_STATE" && info.State.NumSubjects != uint64(count) {
			t.Fatalf("KV subjects=%d want=%d", info.State.NumSubjects, count)
		}
		if name == "OBJ_WF_BLOB" && info.State.NumSubjects <= uint64(count*2) {
			t.Fatalf("object subjects=%d want more than %d", info.State.NumSubjects, count*2)
		}
		t.Logf("raw census %s subjects=%d messages=%d", name, info.State.NumSubjects, info.State.Msgs)
	}
	now := time.Now().Add(time.Minute)
	for _, age := range []time.Duration{24 * time.Hour, 0} {
		at := time.Now()
		result, err := retention.SweepBlobsQuiescent(ctx, all[0], age, now)
		wantDeleted := 0
		if age == 0 {
			wantDeleted = 1
		}
		if err != nil || result.Objects != count+2 || result.Referenced != 1 || result.Eligible != wantDeleted || result.Deleted != wantDeleted {
			t.Fatalf("age=%s result=%+v want objects=%d referenced=1 deleted=%d err=%v", age, result, count+2, wantDeleted, err)
		}
		t.Logf("sweep age=%s result=%+v elapsed=%s", age, result, time.Since(at))
		data, err := objects.GetBytes(ctx, protected)
		if err != nil || string(data) != protected {
			t.Fatalf("protected beyond-page state reference lost: data=%q err=%v", data, err)
		}
		if age != 0 {
			if _, err := objects.GetInfo(ctx, "input-orphan"); err != nil {
				t.Fatalf("age guard lost orphan: %v", err)
			}
		}
	}
	if _, err := objects.GetInfo(ctx, "input-orphan"); !errors.Is(err, jetstream.ErrObjectNotFound) {
		t.Fatalf("orphan retained: %v", err)
	}
	if _, err := objects.GetInfo(ctx, "input-deleted"); !errors.Is(err, jetstream.ErrObjectNotFound) {
		t.Fatalf("deleted object resurrected: %v", err)
	}
	// Payload reads bracket the unmanaged population independently of sweep counts.
	for _, index := range []int{0, count / 2, count - 1} {
		name := fmt.Sprintf("user-scale-%06d", index)
		data, err := objects.GetBytes(ctx, name)
		if err != nil || string(data) != name {
			t.Fatalf("unmanaged %s lost: data=%q err=%v", name, data, err)
		}
	}
	t.Logf("completed count=%d pagination_boundary=%v elapsed=%s", count, count > 100000, time.Since(start))
}
