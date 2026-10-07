package integrity

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"js-wf/testcluster"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

type nativeCreationState struct {
	jetstream.KeyValue
	t      *testing.T
	first  jetstream.KeyValue
	nc     *nats.Conn
	proxy  *testcluster.ClientProxy
	parent time.Time
	calls  int
	proof  map[string]any
}

func (s *nativeCreationState) WatchAll(ctx context.Context, opts ...jetstream.WatchOpt) (jetstream.KeyWatcher, error) {
	s.calls++
	deadline, _ := ctx.Deadline()
	if !deadline.Equal(s.parent) {
		s.t.Fatal("native creation changed the full initial-set deadline")
	}
	if s.calls != 1 {
		if s.proxy.Stats().Active != 0 || s.proxy.PendingAPI().Disposition != "cancelled" {
			s.t.Fatal("fresh native watch began before blocked transport joined")
		}
		return s.KeyValue.WatchAll(ctx, opts...)
	}
	started := time.Now()
	watch, err := s.first.WatchAll(ctx, opts...)
	elapsed := time.Since(started)
	pending := s.proxy.PendingAPI()
	if watch != nil || !errors.Is(err, context.Canceled) || pending == nil || pending.ForwardedBytes != 0 || pending.Disposition != "held" ||
		!strings.HasPrefix(pending.Subject, "$JS.API.CONSUMER.CREATE.KV_WF_STATE.") || elapsed < auditStateProgressInterval || elapsed >= 3*time.Second {
		s.t.Fatalf("real blocked creation did not cancel: watch=%v err=%v elapsed=%s pending=%+v", watch, err, elapsed, pending)
	}
	s.proof["first_creation_started"] = started.UTC()
	s.proof["first_creation_returned"] = time.Now().UTC()
	s.proof["first_creation_elapsed_ns"] = elapsed.Nanoseconds()
	s.proof["first_native_error"] = err.Error()
	s.proof["pending_before_close"] = pending
	s.nc.Close()
	s.proxy.Close()
	s.proof["pending_after_close"] = s.proxy.PendingAPI()
	s.proof["proxy_stats"] = s.proxy.Stats()
	s.proof["proxy_trace"] = s.proxy.TrafficTrace()
	return watch, err
}

// Exercise the real nats.go WatchAll request and real two-second watchdog.
// The consumer-create request is held before publication, not after commit.
// This focused R3 request-cancellation component does not reproduce the R5
// 24-hour failure or replace its full-cohort and fault requirements.
func TestStateCreationNativeRecovery(t *testing.T) {
	root := os.Getenv("WF_STATE_CREATION_ROOT")
	if root == "" {
		t.Skip("set WF_STATE_CREATION_ROOT to a fresh absolute artifact directory")
	}
	if !filepath.IsAbs(root) {
		t.Fatal("require absolute artifact root")
	}
	if err := os.Mkdir(root, 0700); err != nil {
		t.Fatal(err)
	}
	cluster, err := testcluster.Start(filepath.Join(root, "cluster"), 3)
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Close()
	setup, stopSetup := context.WithTimeout(t.Context(), 30*time.Second)
	defer stopSetup()
	js, err := jetstream.New(cluster.Clients[0])
	if err != nil {
		t.Fatal(err)
	}
	// TCP readiness does not establish the metadata quorum needed to create
	// an R3 bucket. Admit that quorum within the original setup budget before
	// making the first creation request; the recovery budget is unchanged.
	ready := time.NewTicker(20 * time.Millisecond)
	defer ready.Stop()
	for {
		leader := false
		for _, peer := range cluster.Servers {
			leader = leader || peer.JetStreamIsLeader()
		}
		if leader {
			break
		}
		select {
		case <-setup.Done():
			t.Fatal("metadata leader readiness:", setup.Err())
		case <-ready.C:
		}
	}
	state, err := js.CreateKeyValue(setup, jetstream.KeyValueConfig{Bucket: "WF_STATE", Replicas: 3, Storage: jetstream.FileStorage, History: 1})
	if err != nil {
		t.Fatal(err)
	}
	expected := make(map[string][]byte)
	for i := range 1024 {
		key, payload := fmt.Sprintf("included-%04d", i), []byte(fmt.Sprintf("retained-state-%04d", i))
		if _, err := state.Put(setup, key, payload); err != nil {
			t.Fatal(err)
		}
		expected[key] = payload
	}
	for _, key := range []string{"deleted", "purged", "excluded"} {
		if _, err := state.Put(setup, key, []byte(key)); err != nil {
			t.Fatal(err)
		}
	}
	if err := state.Delete(setup, "deleted"); err != nil {
		t.Fatal(err)
	}
	if err := state.Purge(setup, "purged"); err != nil {
		t.Fatal(err)
	}
	proxy, err := testcluster.NewClientProxy(cluster.Servers[0].ClientURL())
	if err != nil {
		t.Fatal(err)
	}
	defer proxy.Close()
	if err := proxy.EnableTrafficFileTrace(filepath.Join(root, "wire.jsonl"), 16<<20); err != nil {
		t.Fatal(err)
	}
	if err := proxy.HoldFirstConsumerCreate("$JS.API.", "KV_WF_STATE"); err != nil {
		t.Fatal(err)
	}
	nc, err := nats.Connect(proxy.URL(), nats.IgnoreDiscoveredServers(), nats.NoReconnect())
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	firstJS, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	firstState, err := firstJS.KeyValue(setup, "WF_STATE")
	if err != nil {
		t.Fatal(err)
	}
	ctx, stop := context.WithTimeout(t.Context(), 20*time.Second)
	defer stop()
	deadline, _ := ctx.Deadline()
	proof := map[string]any{"scope": "Actual R3 WatchAll pre-publication creation stall and fresh snapshot recovery; no R5/24h/full-cohort qualification", "deadline": deadline.UTC(), "parent_budget_ns": int64(20 * time.Second), "expected_keys": len(expected)}
	wrapped := &nativeCreationState{KeyValue: state, t: t, first: firstState, nc: nc, proxy: proxy, parent: deadline, proof: proof}
	var frames []StateSnapshotObservation
	observed := WithStateSnapshotObserver(ctx, func(frame StateSnapshotObservation) { frames = append(frames, frame) })
	include := func(key string) bool { return key != "excluded" }
	started := time.Now()
	snapshot, err := auditStateRead(observed, func(call context.Context) (jetstream.KeyValue, error) {
		return initialAuditStateWithProgressTimeout(call, wrapped, include)
	})
	if err != nil || wrapped.calls != 2 {
		t.Fatalf("native fresh snapshot attempts=%d err=%v", wrapped.calls, err)
	}
	validate := func(value jetstream.KeyValue) {
		keys, err := value.Keys(ctx)
		if err != nil || len(keys) != len(expected) {
			t.Fatalf("complete snapshot keys=%d want=%d err=%v", len(keys), len(expected), err)
		}
		for key, want := range expected {
			entry, err := value.Get(ctx, key)
			if err != nil || !bytes.Equal(entry.Value(), want) {
				t.Fatalf("native state bytes differ: %s err=%v", key, err)
			}
		}
		for _, key := range []string{"deleted", "purged", "excluded"} {
			if _, err := value.Get(ctx, key); !errors.Is(err, jetstream.ErrKeyNotFound) {
				t.Fatalf("tombstone/excluded key retained: %s err=%v", key, err)
			}
		}
	}
	validate(snapshot)
	control, err := initialAuditState(ctx, state, include)
	if err != nil {
		t.Fatal(err)
	}
	validate(control)
	stream, err := js.Stream(ctx, "KV_WF_STATE")
	if err != nil {
		t.Fatal(err)
	}
	info, err := stream.Info(ctx)
	if err != nil || info.Config.Replicas != 3 || info.State.Consumers != 0 {
		t.Fatalf("native watch consumer cleanup/config: %+v err=%v", info, err)
	}
	stats, trace := proxy.Stats(), proxy.TrafficTrace()
	if stats.Active != 0 || stats.AcceptedConnections != 1 || stats.UpstreamDialFailures != 0 || stats.BufferOverflows != 0 || trace.Truncated || trace.FrameFileError != "" || trace.FrameRecords == 0 {
		t.Fatalf("incomplete wire/closed relay proof: stats=%+v trace=%+v", stats, trace)
	}
	creationFailures, completed := 0, 0
	for _, frame := range frames {
		if !frame.Deadline.Equal(deadline) {
			t.Fatal("native observer deadline differs")
		}
		if frame.Event == "watch_creation_error" {
			creationFailures++
			if frame.Received != 0 || frame.InitialComplete || !strings.Contains(frame.Error, "creation made no progress") {
				t.Fatal("creation timeout certified partial state")
			}
		}
		if frame.Event == "initial_complete" {
			completed++
			if frame.Included != 1026 || frame.Received != 1027 || !frame.InitialComplete {
				t.Fatalf("native complete barrier counts: %+v", frame)
			}
		}
	}
	if creationFailures != 1 || completed != 1 || ctx.Err() != nil {
		t.Fatalf("native creation recovery/barrier/deadline failure: %+v", frames)
	}
	var ids []string
	for _, server := range cluster.Servers {
		ids = append(ids, server.ID())
	}
	data, err := json.Marshal(expected)
	if err != nil {
		t.Fatal(err)
	}
	hash := sha256.Sum256(data)
	proof["expected_values_sha256"] = hex.EncodeToString(hash[:])
	proof["attempts"], proof["elapsed_ns"], proof["server_ids"] = wrapped.calls, time.Since(started).Nanoseconds(), ids
	proof["snapshot_observations"], proof["state_stream_info"], proof["passed"] = frames, info, true
	if err := os.WriteFile(filepath.Join(root, "expected-values.json"), data, 0600); err != nil {
		t.Fatal(err)
	}
	data, err = json.MarshalIndent(proof, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "native-proof.json"), append(data, '\n'), 0600); err != nil {
		t.Fatal(err)
	}
	t.Logf("native state creation recovered: attempts=2 included_keys=1024 peers=3 consumers=0 elapsed=%s", time.Since(started))
}
