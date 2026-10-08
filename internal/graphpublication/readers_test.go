package graphpublication

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"js-wf/internal/retainedgraph"
)

func readerFixture(t *testing.T) (*memoryPort, Protocol, Root, Reader) {
	t.Helper()
	m, p := newModel("reader")
	prepared, err := p.PrepareAppend(ctx, "history", 0, []byte("old"), [][]byte{[]byte("payload")}, epoch.Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	root, err := p.Commit(ctx, prepared)
	if err != nil {
		t.Fatal(err)
	}
	reader, root, err := p.AcquireReader(ctx, "history", root.Head, epoch.Add(2*time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	return m, p, root, reader
}

func TestGraphReaderAppendRetireRelease(t *testing.T) {
	m, p, root, reader := readerFixture(t)
	old, err := retainedgraph.Read(ctx, stageStore{protocol: p}, reader.Snapshot(), 0)
	if err != nil {
		t.Fatal(err)
	}
	prepared, err := p.PrepareAppendWithOwned(ctx, "history", root.Head, []byte("new"), nil, []OwnedPayload{{Index: 0, Link: old.Blobs[0]}}, epoch.Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	root, err = p.Commit(ctx, prepared)
	if err != nil || root.Schema != RetentionSchema || len(root.Readers) != 1 {
		t.Fatal(root, err)
	}
	if err = p.RetireLive(ctx, "history", root.Head); err != nil {
		t.Fatal(err)
	}
	if _, err = p.Sweep(ctx, epoch.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	retired := m.roots["history"]
	if retired.Graph.Count != 0 || retired.Token != "" || len(retired.Readers) != 1 || retired.Head != root.Head+1 {
		t.Fatal(retired)
	}
	census(t, m, Root{Graph: reader.Snapshot()}, [][]byte{[]byte("old")})
	record, err := p.ReadRetained(ctx, reader, 0, epoch.Add(time.Hour))
	if err != nil || !reflect.DeepEqual(record, old) {
		t.Fatal(record, err)
	}
	data, err := m.Get(ctx, record.Blobs[0], 100)
	if err != nil || string(data) != "payload" {
		t.Fatal(string(data), err)
	}
	// Returned snapshots and authority images cannot mutate the stored pin.
	snapshot := reader.Snapshot()
	snapshot.Frontier[0].Link.Hash = "mutated"
	root.Readers[0].Graph.Frontier[0].Link.Hash = "also-mutated"
	readback, _ := m.ReadRoot(ctx, "history")
	readback.Readers[0].Graph.Frontier[0].Link.Hash = "mutated-copy"
	if _, err = p.ReadRetained(ctx, reader, 0, epoch.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	final, err := p.ReleaseReader(ctx, reader, root.Head+1)
	if err != nil || final.Schema != RetentionSchema || len(final.Readers) != 0 {
		t.Fatal(final, err)
	}
	if _, err = p.RenewReader(ctx, reader, final.Head, epoch.Add(3*time.Hour)); !errors.Is(err, ErrRevoked) {
		t.Fatal(err)
	}
	if _, err = p.Sweep(ctx, epoch.Add(time.Hour)); err != nil || len(m.objects) != 0 {
		t.Fatal(len(m.objects), err)
	}
}

func TestGraphReaderRenewalExpiryFences(t *testing.T) {
	for _, winner := range []string{"renewal", "collector"} {
		t.Run(winner, func(t *testing.T) {
			m, p, root, reader := readerFixture(t)
			if err := p.RetireLive(ctx, "history", root.Head); err != nil {
				t.Fatal(err)
			}
			head := root.Head + 1
			if winner == "renewal" {
				m.rootBefore = func(_ string, expected uint64, _ Root) error {
					if expected != head {
						return fmt.Errorf("wrong paused head")
					}
					_, err := p.RenewReader(ctx, reader, head, epoch.Add(4*time.Hour))
					return err
				}
				if _, err := p.Sweep(ctx, epoch.Add(3*time.Hour)); err != nil {
					t.Fatal(err)
				}
				if m.roots["history"].Head != head+1 || len(m.roots["history"].Readers) != 1 {
					t.Fatal(m.roots["history"])
				}
				if _, err := p.ReadRetained(ctx, reader, 0, epoch.Add(3*time.Hour)); err != nil {
					t.Fatal(err)
				}
				if _, err := p.Sweep(ctx, epoch.Add(4*time.Hour)); err != nil {
					t.Fatal(err)
				}
			} else {
				m.rootBefore = func(_ string, _ uint64, _ Root) error { _, err := p.Sweep(ctx, epoch.Add(2*time.Hour)); return err }
				if _, err := p.RenewReader(ctx, reader, head, epoch.Add(4*time.Hour)); !errors.Is(err, ErrConflict) {
					t.Fatal(err)
				}
				if _, err := p.RenewReader(ctx, reader, m.roots["history"].Head, epoch.Add(4*time.Hour)); !errors.Is(err, ErrRevoked) {
					t.Fatal(err)
				}
			}
			if len(m.objects) != 0 || len(m.roots["history"].Readers) != 0 || m.roots["history"].Schema != RetentionSchema {
				t.Fatal(m.roots["history"], len(m.objects))
			}
		})
	}
}

func TestGraphReaderUnknownAcknowledgements(t *testing.T) {
	for _, operation := range []string{"acquire", "renew", "release", "expire"} {
		t.Run(operation, func(t *testing.T) {
			m, p, root, reader := readerFixture(t)
			m.rootAfter = func() error { return lostReply }
			var err error
			switch operation {
			case "acquire":
				_, _, err = p.AcquireReader(ctx, "history", root.Head, epoch.Add(3*time.Hour))
			case "renew":
				_, err = p.RenewReader(ctx, reader, root.Head, epoch.Add(3*time.Hour))
			case "release":
				_, err = p.ReleaseReader(ctx, reader, root.Head)
			case "expire":
				m.rootAfter = nil
				if err = p.RetireLive(ctx, "history", root.Head); err != nil {
					t.Fatal(err)
				}
				m.rootAfter = func() error { return lostReply }
				_, err = p.Sweep(ctx, epoch.Add(2*time.Hour))
				if !errors.Is(err, lostReply) || m.deletes != 0 || len(m.objects) == 0 {
					t.Fatal(err, m.deletes)
				}
				if _, err = p.Sweep(ctx, epoch.Add(2*time.Hour)); err != nil || len(m.objects) != 0 {
					t.Fatal(err, len(m.objects))
				}
				return
			}
			if err != nil || m.roots["history"].Head != root.Head+1 {
				t.Fatal(err, m.roots["history"])
			}
		})
	}
	// A failed witnessed read cannot resolve the acquisition's unknown reply.
	m, p, root, _ := readerFixture(t)
	m.rootAfter = func() error { m.readRootHook = func(string) error { return lostReply }; return lostReply }
	reader, _, err := p.AcquireReader(ctx, "history", root.Head, epoch.Add(3*time.Hour))
	if !errors.Is(err, lostReply) || reader.id != "" || len(m.roots["history"].Readers) != 2 {
		t.Fatal(reader, err)
	}
}

func TestGraphReaderBoundsAndFailClosed(t *testing.T) {
	m, p, root, reader := readerFixture(t)
	for len(root.Readers) < MaxReaders {
		var err error
		_, root, err = p.AcquireReader(ctx, "history", root.Head, epoch.Add(2*time.Hour))
		if err != nil {
			t.Fatal(err)
		}
	}
	before, _ := json.Marshal(m.roots["history"])
	if _, _, err := p.AcquireReader(ctx, "history", root.Head, epoch.Add(2*time.Hour)); err == nil {
		t.Fatal("unbounded readers")
	}
	if _, err := p.RenewReader(ctx, reader, root.Head, epoch.Add(time.Hour)); err == nil {
		t.Fatal("shortened reader")
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := p.ReleaseReader(cancelled, reader, root.Head); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	after, _ := json.Marshal(m.roots["history"])
	if string(before) != string(after) {
		t.Fatal("invalid call mutated root")
	}
	for _, mutate := range []func(*Root){
		func(r *Root) { r.Schema = Schema },
		func(r *Root) { r.Readers[0].Expires = time.Time{} },
		func(r *Root) { r.Readers[1].ID = r.Readers[0].ID },
		func(r *Root) { r.Readers[0].Graph.Frontier[0].Link.Hash = "bad" },
	} {
		bad := cloneRoot(root)
		mutate(&bad)
		if _, err := normalizeRoot(bad); err == nil {
			t.Fatal("accepted malformed retention")
		}
	}
	if err := p.RetireLive(ctx, "history", root.Head); err != nil {
		t.Fatal(err)
	}
	if _, err := p.ReadRetained(ctx, reader, 0, epoch.Add(2*time.Hour)); !errors.Is(err, ErrRevoked) {
		t.Fatal(err)
	}
	delete(m.objects, reader.graph.Frontier[0].Link.Reference.Object)
	if _, err := p.Sweep(ctx, epoch.Add(time.Hour)); err == nil || m.deletes != 0 {
		t.Fatal("uncertain ancestry allowed collection", err, m.deletes)
	}
}

func TestGraphReaderOriginalHeadAndEmptySnapshots(t *testing.T) {
	m, p, root, reader := readerFixture(t)
	prepared, err := p.PrepareAppend(ctx, "history", root.Head, []byte("pending"), nil, epoch.Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	_, next, err := p.AcquireReader(ctx, "history", root.Head, epoch.Add(2*time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = p.Commit(ctx, prepared); !errors.Is(err, ErrConflict) {
		t.Fatal("reader did not fence prepared append", err)
	}
	if err = p.Retire(ctx, "history", next.Head); err == nil {
		t.Fatal("v1 retirement erased retention")
	}
	if err = p.RetireLive(ctx, "history", next.Head); err != nil {
		t.Fatal(err)
	}
	if _, err = p.Sweep(ctx, epoch.Add(2*time.Hour)); err != nil || len(m.objects) != 0 {
		t.Fatal(err, len(m.objects))
	}
	if _, err = p.RenewReader(ctx, reader, m.roots["history"].Head, epoch.Add(3*time.Hour)); !errors.Is(err, ErrRevoked) {
		t.Fatal(err)
	}
	for round := 0; round < 3; round++ {
		current, _ := m.ReadRoot(ctx, "history")
		for i := 0; i < MaxReaders; i++ {
			_, current, err = p.AcquireReader(ctx, "history", current.Head, epoch.Add(time.Hour))
			if err != nil {
				t.Fatal(err)
			}
		}
		current, err = p.ExpireReaders(ctx, "history", current.Head, epoch.Add(time.Hour))
		if err != nil || len(current.Readers) != 0 || current.Schema != RetentionSchema {
			t.Fatal(current, err)
		}
	}
}

func TestGraphReaderAcquireRetirementRace(t *testing.T) {
	for _, winner := range []string{"acquire", "retire"} {
		t.Run(winner, func(t *testing.T) {
			m, p, root, _ := readerFixture(t)
			if winner == "acquire" {
				m.rootBefore = func(_ string, expected uint64, _ Root) error {
					_, _, err := p.AcquireReader(ctx, "history", expected, epoch.Add(3*time.Hour))
					return err
				}
				if err := p.RetireLive(ctx, "history", root.Head); !errors.Is(err, ErrConflict) {
					t.Fatal(err)
				}
				if len(m.roots["history"].Readers) != 2 || m.roots["history"].Graph.Count != 1 {
					t.Fatal(m.roots["history"])
				}
			} else {
				m.rootBefore = func(_ string, expected uint64, _ Root) error { return p.RetireLive(ctx, "history", expected) }
				if _, _, err := p.AcquireReader(ctx, "history", root.Head, epoch.Add(3*time.Hour)); !errors.Is(err, ErrConflict) {
					t.Fatal(err)
				}
				if len(m.roots["history"].Readers) != 1 || m.roots["history"].Graph.Count != 0 {
					t.Fatal(m.roots["history"])
				}
			}
		})
	}
}
