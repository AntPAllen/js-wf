package integration_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"js-wf/client"
	"js-wf/identity"
	"js-wf/testcluster"
	"js-wf/wf"
	"js-wf/worker"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// Inject only one weak metadata absence; all subsequent payload reads and the
// administrative leader lookup use the real file-backed R3 cluster. This is a
// client-boundary control, not a claim to have forced native follower lag.
type absentResultJS struct {
	jetstream.JetStream
	remaining atomic.Int64
	calls     atomic.Int64
}

func (j *absentResultJS) ObjectStore(ctx context.Context, bucket string) (jetstream.ObjectStore, error) {
	store, err := j.JetStream.ObjectStore(ctx, bucket)
	if err != nil {
		return nil, err
	}
	return &absentResultStore{ObjectStore: store, owner: j}, nil
}

type absentResultStore struct {
	jetstream.ObjectStore
	owner *absentResultJS
}

func (s *absentResultStore) GetBytes(ctx context.Context, name string, opts ...jetstream.GetObjectOpt) ([]byte, error) {
	s.owner.calls.Add(1)
	if s.owner.remaining.Add(-1) >= 0 {
		return nil, jetstream.ErrObjectNotFound
	}
	return s.ObjectStore.GetBytes(ctx, name, opts...)
}

// Change only the administrative oracle route, retaining the real modern
// Object Store path. A responding subscriber that deliberately drops replies
// distinguishes deadline handling from immediate no-responders errors.
type resultOracleRoute struct {
	jetstream.JetStream
	options jetstream.JetStreamOptions
}

func (j resultOracleRoute) Options() jetstream.JetStreamOptions { return j.options }

func TestResultReadVerifiesWeakAbsence(t *testing.T) {
	runResultReadWeakAbsence(t, "")
}

func TestResultReadVerifiesWeakAbsenceInJetStreamDomain(t *testing.T) {
	runResultReadWeakAbsence(t, "WFRESULT")
}

func runResultReadWeakAbsence(t *testing.T, domain string) {
	t.Helper()
	if os.Getenv("WF_RESULT_ABSENCE_ROOT") == "" {
		t.Skip("set WF_RESULT_ABSENCE_ROOT to retain native regression stores")
	}
	root := filepath.Join(os.Getenv("WF_RESULT_ABSENCE_ROOT"), t.Name())
	if err := os.Mkdir(root, 0700); err != nil {
		t.Fatal(err)
	}
	var cluster *testcluster.Cluster
	var err error
	if domain == "" {
		cluster, err = testcluster.Start(root, 3)
	} else {
		cluster, err = testcluster.StartWithDomain(root, 3, domain)
	}
	if err != nil {
		t.Fatal(err)
	}
	all, _ := setupClusterDomain(t, cluster, domain)
	var mu sync.Mutex
	var requests []string
	prefix := "$JS.API"
	if domain != "" {
		prefix = "$JS." + domain + ".API"
	}
	js, err := newTestJetStreamDomain(cluster.Clients[0], domain, jetstream.WithClientTrace(&jetstream.ClientTrace{
		RequestSent: func(subject string, _ []byte) { mu.Lock(); requests = append(requests, subject); mu.Unlock() },
	}))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	stream, err := all[0].Stream(ctx, "OBJ_WF_BLOB")
	if err != nil || !stream.CachedInfo().Config.AllowDirect {
		t.Fatalf("direct-enabled bucket: %v", err)
	}
	objects, err := js.ObjectStore(ctx, "WF_BLOB")
	if err != nil {
		t.Fatal(err)
	}
	const name = "absence-result"
	data := []byte(`"` + strings.Repeat("r", 700*1024) + `"`)
	if _, err := objects.PutBytes(ctx, name, data); err != nil {
		t.Fatal(err)
	}
	wrapped := &absentResultJS{JetStream: js}
	port := worker.NewResultBlobPort(wrapped)
	c := client.New(wrapped)
	handle, err := c.Start(ctx, "test", "result-absence", []byte(`null`))
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(data)
	terminal, _ := json.Marshal(wf.Outcome{InvSeq: handle.InvSeq, ResultRef: name, ResultHash: hex.EncodeToString(digest[:])})
	state, err := all[0].KeyValue(ctx, "WF_STATE")
	if err != nil {
		t.Fatal(err)
	}
	// Synthetic terminal publisher isolates the public Await result-read contract.
	if _, err := state.Put(ctx, identity.Key(handle.Type, handle.ID), terminal); err != nil {
		t.Fatal(err)
	}
	for _, reader := range []struct {
		name string
		read func(context.Context) ([]byte, error)
	}{
		{"worker", func(ctx context.Context) ([]byte, error) { return port.GetBytes(ctx, name) }},
		{"client", func(ctx context.Context) ([]byte, error) { return c.Await(ctx, handle.Type, handle.ID) }},
	} {
		t.Run(reader.name, func(t *testing.T) {
			wrapped.remaining.Store(1)
			before := wrapped.calls.Load()
			mu.Lock()
			requests = nil
			mu.Unlock()
			got, err := reader.read(ctx)
			if err != nil || !bytes.Equal(got, data) {
				t.Fatalf("committed result after injected weak absence: bytes=%d err=%v", len(got), err)
			}
			if wrapped.calls.Load()-before != 2 {
				t.Fatalf("expected absence then real SDK read, got %d", wrapped.calls.Load()-before)
			}
			mu.Lock()
			defer mu.Unlock()
			leaders := 0
			for _, subject := range requests {
				if subject == prefix+".STREAM.MSG.GET.OBJ_WF_BLOB" {
					leaders++
				}
			}
			if leaders != 1 {
				t.Fatalf("expected one administrative metadata confirmation: %v", requests)
			}
			t.Logf("real payload recovered; leader confirmations=%d route=%s", leaders, prefix+".STREAM.MSG.GET.OBJ_WF_BLOB")
		})
	}
	t.Run("deletion", func(t *testing.T) {
		if err := objects.Delete(ctx, name); err != nil {
			t.Fatal(err)
		}
		wrapped.remaining.Store(0)
		if _, err := port.GetBytes(ctx, name); !errors.Is(err, jetstream.ErrObjectNotFound) {
			t.Fatalf("real deletion: %v", err)
		}
		if _, err := c.Await(ctx, handle.Type, handle.ID); !errors.Is(err, jetstream.ErrObjectNotFound) {
			t.Fatalf("deleted terminal: %v", err)
		}
	})
	t.Run("corruption", func(t *testing.T) {
		if _, err := objects.PutBytes(ctx, name, []byte(`"corrupt"`)); err != nil {
			t.Fatal(err)
		}
		if _, err := c.Await(ctx, handle.Type, handle.ID); !errors.Is(err, wf.ErrCorruptJournal) {
			t.Fatalf("corrupt terminal: %v", err)
		}
	})
	t.Run("permanent_stale_absence_deadline", func(t *testing.T) {
		wrapped.remaining.Store(1000000)
		attempt, stop := context.WithTimeout(ctx, 250*time.Millisecond)
		defer stop()
		started := time.Now()
		if _, err := port.GetBytes(attempt, name); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("deadline: %v", err)
		}
		if time.Since(started) > time.Second {
			t.Fatal("result deadline exceeded margin")
		}
	})
	t.Run("blocked_leader_deadline_and_cancel", func(t *testing.T) {
		for _, domain := range []bool{false, true} {
			options := js.Options()
			prefix := "$JS.RESULTCHECK.API"
			options.APIPrefix = prefix
			options.Domain = ""
			if domain {
				options.APIPrefix = ""
				options.Domain = "RESULTDOMAIN"
				prefix = "$JS.RESULTDOMAIN.API"
			}
			requested := make(chan struct{}, 4)
			sub, err := cluster.Clients[0].Subscribe(prefix+".STREAM.MSG.GET.OBJ_WF_BLOB", func(*nats.Msg) { requested <- struct{}{} })
			if err != nil {
				t.Fatal(err)
			}
			if err := cluster.Clients[0].FlushTimeout(time.Second); err != nil {
				t.Fatal(err)
			}
			injected := &absentResultJS{JetStream: resultOracleRoute{JetStream: js, options: options}}
			injected.remaining.Store(100)
			blocked := worker.NewResultBlobPort(injected)
			attempt, stop := context.WithTimeout(ctx, 250*time.Millisecond)
			started := time.Now()
			_, err = blocked.GetBytes(attempt, name)
			stop()
			if !errors.Is(err, context.DeadlineExceeded) || time.Since(started) > time.Second {
				t.Fatalf("blocked leader deadline domain=%v: %v", domain, err)
			}
			select {
			case <-requested:
			default:
				t.Fatal("leader request not routed to configured API")
			}
			attempt, stop = context.WithCancel(ctx)
			done := make(chan error, 1)
			go func() { _, err := blocked.GetBytes(attempt, name); done <- err }()
			select {
			case <-requested:
			case <-time.After(time.Second):
				stop()
				t.Fatal("cancel control did not reach leader request")
			}
			stop()
			select {
			case err := <-done:
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("blocked leader cancellation: %v", err)
				}
			case <-time.After(time.Second):
				t.Fatal("leader request ignored cancellation")
			}
			sub.Unsubscribe()
		}
	})
	t.Run("leader_rejects_malformed_metadata", func(t *testing.T) {
		subject := "$O.WF_BLOB.M." + base64.URLEncoding.EncodeToString([]byte(name))
		message := nats.NewMsg(subject)
		message.Header.Set(nats.MsgRollup, nats.MsgRollupSubject)
		message.Data = []byte(`{bad`)
		if _, err := js.PublishMsg(ctx, message); err != nil {
			t.Fatal(err)
		}
		wrapped.remaining.Store(1)
		if _, err := port.GetBytes(ctx, name); !errors.Is(err, jetstream.ErrBadObjectMeta) {
			t.Fatalf("malformed leader metadata: %v", err)
		}
	})

}
