package graphpublication

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"
)

type catalogOverridePort struct {
	Port
	keys []string
	err  error
}

func (p catalogOverridePort) RootKeys(context.Context) ([]string, error) { return p.keys, p.err }

func TestGraphCatalogExpiresEmptySnapshots(t *testing.T) {
	m, p := newModel("catalog")
	for _, destination := range []string{"empty-a", "empty-b"} {
		head := uint64(0)
		for i := 0; i < MaxReaders; i++ {
			_, root, err := p.AcquireReader(ctx, destination, head, epoch.Add(time.Hour))
			if err != nil {
				t.Fatal(err)
			}
			head = root.Head
		}
	}
	if len(m.blobs) != 0 || len(m.objects) != 0 {
		t.Fatal("empty snapshot created object grants")
	}
	for cycle := 0; cycle < 3; cycle++ {
		if n, err := p.SweepWithReaders(ctx, epoch.Add(time.Hour)); err != nil || n != 0 {
			t.Fatal(n, err)
		}
		for _, destination := range []string{"empty-a", "empty-b"} {
			root, _ := m.ReadRoot(ctx, destination)
			if root.Schema != RetentionSchema || len(root.Readers) != 0 || root.Head != uint64(MaxReaders+1+cycle*2) {
				t.Fatal(root)
			}
			if cycle < 2 {
				if _, _, err := p.AcquireReader(ctx, destination, root.Head, epoch.Add(time.Hour)); err != nil {
					t.Fatal(err)
				}
			}
		}
	}
	keys, err := m.RootKeys(ctx)
	if err != nil || len(keys) != 2 {
		t.Fatal(keys, err)
	}
}

func TestGraphCatalogFailureStopsObjectCollection(t *testing.T) {
	for _, mode := range []string{"missing-support", "unknown-list", "duplicate", "invalid", "unknown-root", "unknown-expiry", "cancelled"} {
		t.Run(mode, func(t *testing.T) {
			m, p, root, _ := readerFixture(t)
			if err := p.RetireLive(ctx, "history", root.Head); err != nil {
				t.Fatal(err)
			}
			before, _ := json.Marshal(m.roots)
			switch mode {
			case "missing-support":
				p.Port = struct{ Port }{m}
			case "unknown-list":
				p.Port = catalogOverridePort{Port: m, err: lostReply}
			case "duplicate":
				p.Port = catalogOverridePort{Port: m, keys: []string{"history", "history"}}
			case "invalid":
				p.Port = catalogOverridePort{Port: m, keys: []string{"history", ""}}
			case "unknown-root":
				m.readRootHook = func(string) error { return lostReply }
			case "unknown-expiry":
				m.rootAfter = func() error { return lostReply }
			}
			call := ctx
			if mode == "cancelled" {
				var cancel context.CancelFunc
				call, cancel = context.WithCancel(ctx)
				cancel()
			}
			if n, err := p.SweepWithReaders(call, epoch.Add(2*time.Hour)); err == nil || n != 0 || m.deletes != 0 || len(m.objects) == 0 {
				t.Fatal(n, err, m.deletes)
			}
			after, _ := json.Marshal(m.roots)
			if mode != "unknown-expiry" && string(before) != string(after) {
				t.Fatal("failed census mutated roots")
			}
		})
	}
}

func TestGraphCatalogRenewalConflictAndClosedGrants(t *testing.T) {
	m, p, root, reader := readerFixture(t)
	if err := p.RetireLive(ctx, "history", root.Head); err != nil {
		t.Fatal(err)
	}
	head := root.Head + 1
	m.rootBefore = func(_ string, expected uint64, _ Root) error {
		if expected != head {
			return errors.New("wrong catalog head")
		}
		_, err := p.RenewReader(ctx, reader, head, epoch.Add(4*time.Hour))
		return err
	}
	if _, err := p.SweepWithReaders(ctx, epoch.Add(3*time.Hour)); err != nil {
		t.Fatal(err)
	}
	if _, err := p.ReadRetained(ctx, reader, 0, epoch.Add(3*time.Hour)); err != nil {
		t.Fatal(err)
	}
	if _, err := p.SweepWithReaders(ctx, epoch.Add(4*time.Hour)); err != nil || len(m.objects) != 0 {
		t.Fatal(err, len(m.objects))
	}
	// All origin grants are closed. Future empty snapshots must still expire.
	root, _ = m.ReadRoot(ctx, "history")
	_, root, err := p.AcquireReader(ctx, "history", root.Head, epoch.Add(5*time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = p.SweepWithReaders(ctx, epoch.Add(5*time.Hour)); err != nil {
		t.Fatal(err)
	}
	current, _ := m.ReadRoot(ctx, "history")
	if current.Head != root.Head+1 || !reflect.DeepEqual(current.Readers, []ReaderPin(nil)) {
		t.Fatal(current)
	}
}
