package graphpublication

import (
	"bytes"
	"encoding/json"
	"errors"
	"testing"
	"time"
)

func TestGraphApplicationLifecycle(t *testing.T) {
	m, p := newModel("application")
	app := []byte(`{"generation":1,"index":0,"epoch":2}`)
	prepared, err := p.PrepareAppendWithApplication(ctx, "journal", 0, []byte("started"), [][]byte{[]byte("input")}, nil, epoch.Add(time.Hour), app)
	if err != nil {
		t.Fatal(err)
	}
	app[0] = '!'
	root, err := p.Commit(ctx, prepared)
	if err != nil || root.Schema != ApplicationSchema || root.Graph.Count != 1 || root.Application[0] != '{' {
		t.Fatal(root, err)
	}
	want := append([]byte(nil), root.Application...)
	root.Application[0] = '!'
	root, err = m.ReadRoot(ctx, "journal")
	if err != nil || !bytes.Equal(root.Application, want) {
		t.Fatal(root, err)
	}
	reader, root, err := p.AcquireReader(ctx, "journal", root.Head, epoch.Add(2*time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	prepared, err = p.PrepareAppend(ctx, "journal", root.Head, []byte("later"), nil, epoch.Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	root, err = p.Commit(ctx, prepared)
	if err != nil || !bytes.Equal(root.Application, want) || root.Schema != ApplicationSchema {
		t.Fatal(root, err)
	}
	root, err = p.RenewReader(ctx, reader, root.Head, epoch.Add(3*time.Hour))
	if err != nil || !bytes.Equal(root.Application, want) {
		t.Fatal(root, err)
	}
	if err = p.RetireLive(ctx, "journal", root.Head); err != nil {
		t.Fatal(err)
	}
	root, err = m.ReadRoot(ctx, "journal")
	if err != nil || root.Graph.Count != 0 || root.Schema != ApplicationSchema || !bytes.Equal(root.Application, want) {
		t.Fatal(root, err)
	}
	if _, err = p.ReadRetained(ctx, reader, 0, epoch.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	root, err = p.ReleaseReader(ctx, reader, root.Head)
	if err != nil || !bytes.Equal(root.Application, want) {
		t.Fatal(root, err)
	}
	if _, err = p.SweepWithReaders(ctx, epoch.Add(4*time.Hour)); err != nil || len(m.objects) != 0 {
		t.Fatal(err, len(m.objects))
	}
	root, err = p.UpdateApplication(ctx, "journal", root.Head, nil)
	if err != nil || root.Schema != ApplicationSchema || len(root.Application) != 0 {
		t.Fatal(root, err)
	}
	for _, schema := range []string{Schema, RetentionSchema} {
		next := cloneRoot(root)
		next.Schema = schema
		if _, err = m.CASRoot(ctx, "journal", root.Head, next); err == nil {
			t.Fatal("downgrade accepted", schema)
		}
	}
}

func TestGraphApplicationOriginalHeadAndUnknownReplies(t *testing.T) {
	for _, mode := range []string{"lost", "different", "later", "read-fails", "conflict"} {
		t.Run(mode, func(t *testing.T) {
			m, p := newModel("application-" + mode)
			prepared, err := p.PrepareAppend(ctx, "journal", 0, []byte("stale"), nil, epoch.Add(time.Hour))
			if err != nil {
				t.Fatal(err)
			}
			if mode == "conflict" {
				m.rootBefore = func(_ string, h uint64, _ Root) error {
					_, e := p.UpdateApplication(ctx, "journal", h, []byte("winner"))
					return e
				}
			} else {
				m.rootAfter = func() error {
					r := m.roots["journal"]
					switch mode {
					case "different":
						r.Application = []byte("other")
					case "later":
						r.Head++
					case "read-fails":
						m.readRootHook = func(string) error { return lostReply }
					}
					m.roots["journal"] = r
					return lostReply
				}
			}
			_, err = p.UpdateApplication(ctx, "journal", 0, []byte("cursor"))
			if mode == "lost" {
				if err != nil {
					t.Fatal(err)
				}
			} else if err == nil {
				t.Fatal("uncertain/non-own application image accepted")
			}
			m.readRootHook = nil
			if _, err = p.Commit(ctx, prepared); !errors.Is(err, ErrConflict) {
				t.Fatal("stale append not fenced", err)
			}
		})
	}
}

func TestGraphApplicationLimitsAndOpaqueOwnership(t *testing.T) {
	m, p := newModel("application-bounds")
	large := make([]byte, MaxApplicationBytes+1)
	if _, err := p.UpdateApplication(ctx, "journal", 0, large); err == nil {
		t.Fatal("unbounded descriptor accepted")
	}
	if _, err := p.PrepareAppendWithApplication(ctx, "journal", 0, nil, nil, nil, epoch.Add(time.Hour), large); err == nil {
		t.Fatal("unbounded append descriptor accepted")
	}
	if len(m.roots)+len(m.objects)+len(m.blobs)+len(m.observedRoots) != 0 {
		t.Fatal("invalid descriptor touched transport")
	}
	root, err := p.UpdateApplication(ctx, "journal", 0, make([]byte, MaxApplicationBytes))
	if err != nil {
		t.Fatal(err)
	}
	prepared, err := p.PrepareAppend(ctx, "journal", root.Head, []byte("entry"), [][]byte{[]byte("payload")}, epoch.Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	root, err = p.Commit(ctx, prepared)
	if err != nil {
		t.Fatal(err)
	}
	// Even a serialized graph receipt in the descriptor cannot retain bytes.
	data, err := json.Marshal(root.Graph)
	if err != nil {
		t.Fatal(err)
	}
	root, err = p.UpdateApplication(ctx, "journal", root.Head, data)
	if err != nil {
		t.Fatal(err)
	}
	if err = p.RetireLive(ctx, "journal", root.Head); err != nil {
		t.Fatal(err)
	}
	if _, err = p.SweepWithReaders(ctx, epoch.Add(2*time.Hour)); err != nil || len(m.objects) != 0 {
		t.Fatal(err, len(m.objects))
	}
	final, err := m.ReadRoot(ctx, "journal")
	if err != nil || !bytes.Equal(final.Application, data) {
		t.Fatal(final, err)
	}
	// Empty-reader expiry still advances the physical head without erasing app state.
	_, final, err = p.AcquireReader(ctx, "journal", final.Head, epoch.Add(3*time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = p.SweepWithReaders(ctx, epoch.Add(4*time.Hour)); err != nil {
		t.Fatal(err)
	}
	got, err := m.ReadRoot(ctx, "journal")
	if err != nil || got.Head != final.Head+1 || len(got.Readers) != 0 || !bytes.Equal(got.Application, data) {
		t.Fatal(got, err)
	}
}
