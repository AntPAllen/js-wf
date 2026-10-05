package integrity

import (
	"context"
	"errors"
	"os"
	"strings"
	"sync"
	"testing"

	"github.com/nats-io/nats.go/jetstream"
)

type staleTerminalJS struct {
	jetstream.JetStream
	gets *int
}

func (j staleTerminalJS) KeyValue(ctx context.Context, bucket string) (jetstream.KeyValue, error) {
	kv, err := j.JetStream.KeyValue(ctx, bucket)
	if err != nil || bucket != "WF_STATE" {
		return kv, err
	}
	return staleTerminalKV{KeyValue: kv, gets: j.gets}, nil
}

type staleTerminalKV struct {
	jetstream.KeyValue
	gets *int
}

func (k staleTerminalKV) Get(ctx context.Context, key string) (jetstream.KeyValueEntry, error) {
	if key == "audit.first" || key == "audit.second" {
		*k.gets++
		return nil, jetstream.ErrKeyNotFound
	}
	return k.KeyValue.Get(ctx, key)
}

func TestTerminalStateLeaderNativeStaleAbsenceAndRealDeletion(t *testing.T) {
	if os.Getenv("WF_AUDIT_BATCH_CANDIDATE") != "1" {
		t.Skip("opt-in authoritative terminal-state read regression")
	}
	base, ctx, cluster := batchedAuditClusterWithServers(t)
	batchAuditPublish(t, ctx, base, "first", batchAuditEntries())
	batchAuditPublish(t, ctx, base, "second", batchAuditEntries())
	state, err := base.Stream(ctx, "KV_WF_STATE")
	if err != nil || !state.CachedInfo().Config.AllowDirect {
		t.Fatalf("expected SDK direct-enabled state bucket: %v", err)
	}
	var mu sync.Mutex
	var requests []string
	js, err := jetstream.NewWithAPIPrefix(cluster.Clients[0], "$JS.API.", jetstream.WithClientTrace(&jetstream.ClientTrace{RequestSent: func(subject string, _ []byte) {
		mu.Lock()
		requests = append(requests, subject)
		mu.Unlock()
	}}))
	if err != nil {
		t.Fatal(err)
	}
	gets := 0
	report, err := Check(ctx, staleTerminalJS{JetStream: js, gets: &gets})
	if err != nil || report != (Report{Invocations: 2, Journals: 2, Entries: 8, Terminal: 2}) || gets != 0 {
		t.Fatalf("stale follower absence trusted: report=%+v stale_gets=%d err=%v", report, gets, err)
	}
	mu.Lock()
	leaderGets := 0
	for _, subject := range requests {
		if strings.HasPrefix(subject, "$JS.API.DIRECT.GET.KV_WF_STATE") {
			t.Errorf("terminal read used direct API: %s", subject)
		}
		if subject == "$JS.API.STREAM.MSG.GET.KV_WF_STATE" {
			leaderGets++
		}
	}
	mu.Unlock()
	if leaderGets != 2 {
		t.Fatalf("leader state reads=%d, expected2", leaderGets)
	}
	kv, err := js.KeyValue(ctx, "WF_STATE")
	if err != nil {
		t.Fatal(err)
	}
	for _, operation := range []string{"delete", "purge"} {
		if operation == "delete" {
			err = kv.Delete(ctx, "audit.second")
		} else {
			err = kv.Purge(ctx, "audit.second")
		}
		if err != nil {
			t.Fatal(err)
		}
		_, err = Check(ctx, js)
		if !errors.Is(err, jetstream.ErrKeyNotFound) {
			t.Fatalf("%s concealed real absence: %v", operation, err)
		}
		if _, err := kv.Put(ctx, "audit.second", []byte(`"ok"`)); err != nil {
			t.Fatal(err)
		}
	}
	t.Logf("R3 state AllowDirect=true, synthetic stale Get bypassed, %d actual administrative reads; real DEL/PURGE rejected", leaderGets)
}
