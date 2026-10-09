package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"js-wf/identity"
	"js-wf/internal/graphpublication"
	"js-wf/journal"
)

func TestReaderExpiryCLIAdmission(t *testing.T) {
	graph := &workerGraphSelection{}
	for _, tc := range []struct {
		enabled, repair bool
		graph           *workerGraphSelection
		interval        time.Duration
		budget          int
	}{
		{true, true, nil, time.Second, 1},
		{true, false, graph, time.Second, 1},
		{true, true, graph, 0, 1},
		{true, true, graph, 11 * time.Second, 1},
		{true, true, graph, time.Second, 0},
		{true, true, graph, time.Second, 257},
		{false, true, graph, time.Second, 1},
	} {
		if _, err := selectReaderExpiry(tc.enabled, tc.repair, tc.graph, tc.interval, tc.budget); err == nil {
			t.Fatal("invalid reader expiry selection accepted", tc)
		}
	}
	selected, err := selectReaderExpiry(true, true, graph, time.Second, 256)
	if err != nil || selected.interval != time.Second || selected.budget != 256 {
		t.Fatal(selected, err)
	}
	if selected, err := selectReaderExpiry(false, true, nil, 0, 0); err != nil || selected != nil {
		t.Fatal(selected, err)
	}
	base := []string{"-id", "readers", "-handler-plugin", "unused"}
	for _, args := range [][]string{
		{"-graph-reader-expiry"},
		{"-graph-reader-expiry-interval", "1s"},
		{"-graph-authority-stream", "AUTH", "-graph-authority-prefix", "wf.graph", "-graph-object-bucket", "OBJECTS", "-timer-backend", "native", "-graph-reader-expiry", "-graph-reader-expiry-interval", "1s", "-graph-reader-expiry-budget", "257"},
	} {
		if err := run(context.Background(), append(append([]string(nil), base...), args...)); err == nil || !strings.Contains(err.Error(), "graph-reader-expiry") {
			t.Fatal(err)
		}
	}
}

func verifyWorkerReaderExpiry(t *testing.T, ctx context.Context, js jetstream.JetStream, cfg journal.NativeGraphConfig, store *journal.GraphStore, typ, id string) {
	t.Helper()
	authority, err := graphpublication.OpenNativeAuthority(ctx, js, cfg.AuthorityStream, cfg.AuthorityPrefix)
	if err != nil {
		t.Fatal(err)
	}
	port, err := graphpublication.OpenNativePort(ctx, authority, cfg.ObjectBucket)
	if err != nil {
		t.Fatal(err)
	}
	p := graphpublication.Protocol{Port: port}
	destination := fmt.Sprintf("journal/%x", sha256.Sum256([]byte(identity.JournalSubject(typ, id))))
	root, err := port.ReadRoot(ctx, destination)
	if err != nil {
		t.Fatal(err)
	}
	objects, err := port.Objects(ctx)
	if err != nil {
		t.Fatal(err)
	}
	canonicalApplication := append([]byte(nil), root.Application...)
	canonicalGraph := root.Graph
	liveExpiry := time.Now().UTC().Add(time.Hour)
	root = acquireFixtureReader(t, ctx, p, destination, liveExpiry)
	liveID := ""
	for _, pin := range root.Readers {
		if pin.Expires.Equal(liveExpiry) {
			liveID = pin.ID
		}
	}
	if liveID == "" {
		t.Fatal("live pin not acquired")
	}
	expiredAt := time.Now().UTC().Add(-time.Second)
	root = acquireFixtureReader(t, ctx, p, destination, expiredAt)
	expiredID := ""
	for _, pin := range root.Readers {
		if pin.Expires.Equal(expiredAt) {
			expiredID = pin.ID
		}
	}
	if expiredID == "" {
		t.Fatal("expired pin not acquired")
	}
	for {
		root, err = port.ReadRoot(ctx, destination)
		if err != nil {
			t.Fatal(err)
		}
		live, expired := false, false
		for _, pin := range root.Readers {
			live = live || pin.ID == liveID
			expired = expired || pin.ID == expiredID
		}
		if !live {
			t.Fatal("reader maintenance removed live pin")
		}
		if !expired {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("CLI did not expire reader pin", ctx.Err())
		case <-time.After(20 * time.Millisecond):
		}
	}
	if !bytes.Equal(root.Application, canonicalApplication) || !reflect.DeepEqual(root.Graph, canonicalGraph) {
		t.Fatal("reader expiry changed canonical application or graph")
	}
	after, err := port.Objects(ctx)
	if err != nil || !reflect.DeepEqual(after, objects) {
		t.Fatal("reader expiry changed objects", err)
	}
	maintenance, err := store.ReaderMaintenance()
	if err != nil {
		t.Fatal(err)
	}
	state, err := js.KeyValue(ctx, "WF_STATE")
	if err != nil {
		t.Fatal(err)
	}
	entry, err := state.Get(ctx, "scan.graph-reader-expiry."+maintenance.ReaderMaintenanceScope())
	if err != nil {
		t.Fatal("reader expiry checkpoint missing", err)
	}
	var checkpoint struct {
		Version int
		Scope   string
		Cursor  *graphpublication.ReaderSweepCursor
	}
	if err := json.Unmarshal(entry.Value(), &checkpoint); err != nil || checkpoint.Version != 2 || checkpoint.Scope != maintenance.ReaderMaintenanceScope() || checkpoint.Cursor == nil {
		t.Fatal("invalid CLI reader checkpoint", err, string(entry.Value()))
	}
	t.Log("CLI reader expiry removed expired pin, preserved live pin/application/graph/objects and persisted scoped version-2 checkpoint")
}

// Result visibility can precede release of the worker's reader pin. Fixture
// admission retries only a definite root CAS conflict, always using a fresh
// witness and retaining the original deadline and expiry.
func acquireFixtureReader(t *testing.T, ctx context.Context, p graphpublication.Protocol, destination string, expires time.Time) graphpublication.Root {
	t.Helper()
	for {
		root, err := p.Port.ReadRoot(ctx, destination)
		if err != nil {
			t.Fatal(err)
		}
		_, root, err = p.AcquireReader(ctx, destination, root.Head, expires)
		if err == nil {
			return root
		}
		if !errors.Is(err, graphpublication.ErrConflict) {
			t.Fatal(err)
		}
		select {
		case <-ctx.Done():
			t.Fatal("fixture reader admission did not converge", ctx.Err())
		case <-time.After(20 * time.Millisecond):
		}
	}
}
