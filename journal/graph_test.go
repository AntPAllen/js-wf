package journal_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"js-wf/internal/graphpublication"
	"js-wf/journal"
	"js-wf/sim"
)

func graphModel(t *testing.T, encoding journal.Encoding) (*journal.GraphStore, *sim.GraphPublicationTransport, *time.Time) {
	t.Helper()
	now := time.Unix(1000, 0).UTC()
	m := sim.NewGraphPublicationTransport(sim.NewScheduler(1))
	s, err := journal.NewGraphStore(journal.GraphConfig{Protocol: m.Protocol(), Now: func() time.Time { return now }, PinTTL: time.Minute, IntentTTL: time.Second, Encoding: encoding})
	if err != nil {
		t.Fatal(err)
	}
	return s, m, &now
}

func TestGraphJournalGenerationRetainedReaderAndDrain(t *testing.T) {
	for _, encoding := range []journal.Encoding{journal.JSON, journal.ProtobufV1} {
		t.Run(string(encoding), func(t *testing.T) {
			s, m, now := graphModel(t, encoding)
			ctx := context.Background()
			tail, err := s.Begin(ctx, "flow", "id", 7)
			if err != nil || tail != 0 {
				t.Fatal(tail, err)
			}
			tail, err = s.Append(ctx, "flow", "id", 7, journal.Entry{Kind: journal.Started, Epoch: 3}, tail, [][]byte{[]byte("input")}, nil)
			if err != nil || tail != 1 {
				t.Fatal(tail, err)
			}
			// Reader metadata advances the authority head, never the logical tail.
			view, err := s.Open(ctx, "flow", "id", 7)
			if err != nil {
				t.Fatal(err)
			}
			record, err := view.Read(ctx, 0)
			if err != nil || record.Sequence != 1 || len(record.Blobs) != 1 {
				t.Fatal(record, err)
			}
			large := json.RawMessage(`"` + string(bytes.Repeat([]byte("x"), 100000)) + `"`)
			tail, err = s.Append(ctx, "flow", "id", 7, journal.Entry{Kind: journal.Completed, Index: 1, Epoch: 4, Payload: large}, tail, nil, []graphpublication.OwnedPayload{{Index: 0, Link: record.Blobs[0]}})
			if err != nil || tail != 2 {
				t.Fatal(tail, err)
			}
			records, readTail, err := s.Read(ctx, "flow", "id", 7)
			if err != nil || readTail != 2 || len(records) != 2 || !bytes.Equal(records[1].Payload, large) {
				t.Fatal(len(records), readTail, err)
			}
			if _, err = s.Begin(ctx, "flow", "id", 8); !errors.Is(err, journal.ErrStale) {
				t.Fatal("new generation before retirement", err)
			}
			if err = s.Retire(ctx, "flow", "id", 7, tail); err != nil {
				t.Fatal(err)
			}
			tail, err = s.Begin(ctx, "flow", "id", 8)
			if err != nil || tail != 2 {
				t.Fatal(tail, err)
			}
			if _, err = s.Append(ctx, "flow", "id", 7, journal.Entry{Kind: journal.Started}, 0, nil, nil); !errors.Is(err, journal.ErrStale) {
				t.Fatal("old generation revived", err)
			}
			if _, err = s.Open(ctx, "flow", "id", 7); !errors.Is(err, journal.ErrStale) {
				t.Fatal("old generation opened", err)
			}
			// Old exact pin still owns input while the new generation is current.
			if _, err = m.Protocol().SweepWithReaders(ctx, now.Add(2*time.Second)); err != nil {
				t.Fatal(err)
			}
			data, err := view.Payload(ctx, 0, record.Blobs[0], 100)
			if err != nil || string(data) != "input" {
				t.Fatal(string(data), err)
			}
			foreign := record.Blobs[0]
			foreign.Reference.Generation++
			if _, err = view.Payload(ctx, 0, foreign, 100); !errors.Is(err, journal.ErrGap) {
				t.Fatal("foreign receipt read", err)
			}
			*now = now.Add(10 * time.Second)
			if err = view.Renew(ctx); err != nil {
				t.Fatal(err)
			}
			if err = view.Close(ctx); err != nil {
				t.Fatal(err)
			}
			if _, err = m.Protocol().SweepWithReaders(ctx, *now); err != nil {
				t.Fatal(err)
			}
			objects, err := m.Objects(ctx)
			if err != nil || len(objects) != 0 {
				t.Fatal(len(objects), err)
			}
			if _, err = view.Read(ctx, 0); !errors.Is(err, graphpublication.ErrRevoked) {
				t.Fatal(err)
			}
			tail, err = s.Append(ctx, "flow", "id", 8, journal.Entry{Kind: journal.Started}, tail, nil, nil)
			if err != nil || tail != 3 {
				t.Fatal(tail, err)
			}
			if err = s.Retire(ctx, "flow", "id", 8, tail); !errors.Is(err, journal.ErrStale) {
				t.Fatal("active generation retired", err)
			}
			tail, err = s.Append(ctx, "flow", "id", 8, journal.Entry{Kind: journal.Failed, Index: 1, Epoch: 1}, tail, nil, nil)
			if err != nil || tail != 4 {
				t.Fatal(tail, err)
			}
			if err = s.Retire(ctx, "flow", "id", 8, tail); err != nil {
				t.Fatal(err)
			}
			if _, err = m.Protocol().SweepWithReaders(ctx, *now); err != nil {
				t.Fatal(err)
			}
			objects, err = m.Objects(ctx)
			if err != nil || len(objects) != 0 {
				t.Fatal(len(objects), err)
			}
			if err = m.CheckReferences(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestGraphJournalStaleAndUnknownAppends(t *testing.T) {
	for _, mode := range []string{"lost", "drop", "readback", "competitor", "retirement-lost", "begin-lost"} {
		t.Run(mode, func(t *testing.T) {
			s, m, now := graphModel(t, journal.JSON)
			ctx := context.Background()
			if mode == "begin-lost" {
				if err := m.QueueFault("cas_root", sim.LoseAckAfterCommit); err != nil {
					t.Fatal(err)
				}
			}
			tail, err := s.Begin(ctx, "flow", "id", 5)
			if err != nil {
				t.Fatal(err)
			}
			if mode == "lost" || mode == "readback" {
				if err = m.QueueFault("cas_root", sim.LoseAckAfterCommit); err != nil {
					t.Fatal(err)
				}
			}
			if mode == "drop" {
				if err = m.QueueFault("cas_root", sim.DropBeforeCommit); err != nil {
					t.Fatal(err)
				}
			}
			if mode == "readback" {
				m.PauseBefore("cas_root", func() error { return m.QueueFault("read_root", sim.DropBeforeCommit) })
			}
			if mode == "competitor" {
				m.PauseBefore("cas_root", func() error {
					_, e := s.Append(ctx, "flow", "id", 5, journal.Entry{Kind: journal.Started, Epoch: 2}, tail, nil, nil)
					return e
				})
			}
			seq, err := s.Append(ctx, "flow", "id", 5, journal.Entry{Kind: journal.Started, Epoch: 1}, tail, nil, nil)
			switch mode {
			case "drop", "readback":
				if !errors.Is(err, journal.ErrUnknown) {
					t.Fatal(seq, err)
				}
			case "competitor":
				if !errors.Is(err, journal.ErrStale) {
					t.Fatal(seq, err)
				}
			default:
				if err != nil || seq != 1 {
					t.Fatal(seq, err)
				}
			}
			records, tail, err := s.Read(ctx, "flow", "id", 5)
			if err != nil {
				t.Fatal(err)
			}
			if mode == "drop" {
				if len(records) != 0 || tail != 0 {
					t.Fatal(records, tail)
				}
				tail, err = s.Append(ctx, "flow", "id", 5, journal.Entry{Kind: journal.Started, Epoch: 1}, tail, nil, nil)
				if err != nil {
					t.Fatal(err)
				}
			} else if len(records) != 1 || tail != 1 {
				t.Fatal(records, tail)
			}
			// Epoch regression, duplicate start, skipped index and wrong tail all stop.
			for _, bad := range []journal.Entry{{Kind: journal.Started, Index: 1, Epoch: 9}, {Kind: journal.StepCompleted, Index: 2, Epoch: 9}, {Kind: journal.StepCompleted, Index: 1, Epoch: 0}} {
				if _, err = s.Append(ctx, "flow", "id", 5, bad, tail, nil, nil); !errors.Is(err, journal.ErrStale) {
					t.Fatal(bad, err)
				}
			}
			// Every rejected write leaves the observed journal unchanged.
			records, tail, err = s.Read(ctx, "flow", "id", 5)
			if err != nil {
				t.Fatal(err)
			}
			tail, err = s.Append(ctx, "flow", "id", 5, journal.Entry{Kind: journal.Completed, Index: uint64(len(records)), Epoch: 9}, tail, nil, nil)
			if err != nil {
				t.Fatal(err)
			}
			if mode == "retirement-lost" {
				if err = m.QueueFault("cas_root", sim.LoseAckAfterCommit); err != nil {
					t.Fatal(err)
				}
			}
			if err = s.Retire(ctx, "flow", "id", 5, tail); err != nil {
				t.Fatal(err)
			}
			if err = s.Retire(ctx, "flow", "id", 5, tail); err != nil {
				t.Fatal("retire not idempotent", err)
			}
			if _, err = m.Protocol().SweepWithReaders(ctx, now.Add(2*time.Second)); err != nil {
				t.Fatal(err)
			}
			objects, err := m.Objects(ctx)
			if err != nil || len(objects) != 0 {
				t.Fatal(len(objects), err)
			}
		})
	}
}

func TestGraphJournalCorruptCursorAndLeaseBoundary(t *testing.T) {
	for _, mode := range []string{"unknown", "duplicate", "alias", "overflow", "count", "retired", "schema", "expired", "expires-during-read"} {
		t.Run(mode, func(t *testing.T) {
			s, m, now := graphModel(t, journal.JSON)
			ctx := context.Background()
			tail, err := s.Begin(ctx, "flow", "id", 9)
			if err != nil {
				t.Fatal(err)
			}
			tail, err = s.Append(ctx, "flow", "id", 9, journal.Entry{Kind: journal.Started}, tail, nil, nil)
			if err != nil {
				t.Fatal(err)
			}
			if mode == "expired" || mode == "expires-during-read" {
				view, err := s.Open(ctx, "flow", "id", 9)
				if err != nil {
					t.Fatal(err)
				}
				advance := func() { *now = now.Add(time.Minute) }
				if mode == "expired" {
					advance()
				} else {
					m.PauseBefore("get", func() error { advance(); return nil })
				}
				if _, err = view.Read(ctx, 0); !errors.Is(err, graphpublication.ErrRevoked) {
					t.Fatal("expired snapshot returned success", err)
				}
				if err = view.Renew(ctx); !errors.Is(err, graphpublication.ErrRevoked) {
					t.Fatal("expired view revived", err)
				}
				return
			}
			keys, err := m.RootKeys(ctx)
			if err != nil || len(keys) != 1 {
				t.Fatal(keys, err)
			}
			root, err := m.ReadRoot(ctx, keys[0])
			if err != nil {
				t.Fatal(err)
			}
			switch mode {
			case "unknown":
				root.Application = append(root.Application[:len(root.Application)-1], []byte(`,"other":1}`)...)
			case "duplicate":
				root.Application = append(root.Application[:len(root.Application)-1], []byte(`,"count":1}`)...)
			case "alias":
				root.Application = bytes.Replace(root.Application, []byte(`"count"`), []byte(`"Count"`), 1)
			case "overflow":
				root.Application = bytes.Replace(root.Application, []byte(`"base":0`), []byte(`"base":18446744073709551615`), 1)
			case "count":
				root.Application = bytes.Replace(root.Application, []byte(`"count":1`), []byte(`"count":2`), 1)
			case "retired":
				root.Application = bytes.Replace(root.Application, []byte(`"retired":false`), []byte(`"retired":true`), 1)
			case "schema":
				root.Application = bytes.Replace(root.Application, []byte(`js-wf-graph-journal-cursor-v1`), []byte(`unknown`), 1)
			}
			if _, err = m.CASRoot(ctx, keys[0], root.Head, root); err != nil {
				t.Fatal(err)
			}
			if _, err = s.Open(ctx, "flow", "id", 9); !errors.Is(err, journal.ErrGap) {
				t.Fatal("corrupt cursor opened", err)
			}
			if _, err = s.Begin(ctx, "flow", "id", 10); !errors.Is(err, journal.ErrGap) {
				t.Fatal("corrupt cursor replaced", err)
			}
		})
	}
}

func TestGraphJournalLimitsAndForeignRoot(t *testing.T) {
	s, m, _ := graphModel(t, journal.JSON)
	ctx := context.Background()
	if _, err := s.Begin(ctx, "bad.type", "id", 1); err == nil {
		t.Fatal("invalid identity admitted")
	}
	if _, err := s.Begin(ctx, "flow", "id", 0); !errors.Is(err, journal.ErrStale) {
		t.Fatal(err)
	}
	for _, entry := range []journal.Entry{{Kind: journal.Started, Index: journal.MaxEntries}, {Kind: journal.Started, Payload: json.RawMessage(bytes.Repeat([]byte(" "), journal.MaxGraphEntryBytes+1))}, {Kind: "unknown"}} {
		if _, err := s.Append(ctx, "flow", "id", 1, entry, 0, nil, nil); err == nil {
			t.Fatal("invalid entry admitted")
		}
	}
	if _, err := s.Append(ctx, "flow", "id", 1, journal.Entry{Kind: journal.Started}, 0, make([][]byte, 128), nil); !errors.Is(err, journal.ErrTooLong) {
		t.Fatal(err)
	}
	objects, err := m.Objects(ctx)
	if err != nil || len(objects) != 0 {
		t.Fatal(objects, err)
	}
	tail, err := s.Begin(ctx, "flow", "id", 1)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.Append(ctx, "flow", "id", 1, journal.Entry{Kind: journal.Started}, tail+1, nil, nil); !errors.Is(err, journal.ErrStale) {
		t.Fatal("wrong tail accepted", err)
	}
	keys, err := m.RootKeys(ctx)
	if err != nil || len(keys) != 1 {
		t.Fatal(keys, err)
	}
	// A graph v2 root in a journal namespace cannot be implicitly imported.
	other := sim.NewGraphPublicationTransport(sim.NewScheduler(2))
	p := other.Protocol()
	if _, _, err = p.AcquireReader(ctx, keys[0], 0, time.Unix(2000, 0)); err != nil {
		t.Fatal(err)
	}
	foreign, err := journal.NewGraphStore(journal.GraphConfig{Protocol: p})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = foreign.Begin(ctx, "flow", "id", 1); !errors.Is(err, journal.ErrGap) {
		t.Fatal("foreign root adopted", err)
	}
}

func TestGraphJournalEntryPayloadAlias(t *testing.T) {
	s, m, _ := graphModel(t, journal.JSON)
	ctx := context.Background()
	tail, err := s.Begin(ctx, "flow", "alias", 1)
	if err != nil {
		t.Fatal(err)
	}
	entry := journal.Entry{Kind: journal.Started, Epoch: 1}
	encoded, err := journal.MarshalEntry(entry, journal.JSON)
	if err != nil {
		t.Fatal(err)
	}
	tail, err = s.Append(ctx, "flow", "alias", 1, entry, tail, [][]byte{encoded}, nil)
	if err != nil {
		t.Fatal(err)
	}
	view, err := s.Open(ctx, "flow", "alias", 1)
	if err != nil {
		t.Fatal(err)
	}
	defer view.Close(ctx)
	record, err := view.Read(ctx, 0)
	if err != nil || record.EntryBlob.Hash == "" {
		t.Fatal(record, err)
	}
	data, err := view.Payload(ctx, 0, record.EntryBlob, len(encoded))
	if err != nil || !bytes.Equal(data, encoded) {
		t.Fatal(string(data), err)
	}
	if err = m.CheckReferences(); err != nil {
		t.Fatal(err)
	}
}

func TestGraphJournalCloseContentionAndAmbiguousRelease(t *testing.T) {
	for _, mode := range []string{"competitor", "lost", "readback"} {
		t.Run(mode, func(t *testing.T) {
			s, m, now := graphModel(t, journal.JSON)
			ctx := context.Background()
			if _, err := s.Begin(ctx, "flow", "id", 1); err != nil {
				t.Fatal(err)
			}
			first, err := s.Open(ctx, "flow", "id", 1)
			if err != nil {
				t.Fatal(err)
			}
			second, err := s.Open(ctx, "flow", "id", 1)
			if err != nil {
				t.Fatal(err)
			}
			if mode == "competitor" {
				m.PauseBefore("cas_root", func() error { return second.Close(ctx) })
			} else {
				if err = m.QueueFault("cas_root", sim.LoseAckAfterCommit); err != nil {
					t.Fatal(err)
				}
				if mode == "readback" {
					m.PauseBefore("cas_root", func() error { return m.QueueFault("read_root", sim.DropBeforeCommit) })
				}
			}
			err = first.Close(ctx)
			if mode == "readback" {
				if err == nil {
					t.Fatal("ambiguous release silently retried")
				}
			} else {
				if err != nil {
					t.Fatal(err)
				}
				if err = first.Close(ctx); err != nil {
					t.Fatal("close not idempotent", err)
				}
			}
			if mode != "competitor" {
				*now = now.Add(time.Second)
				if err = second.Renew(ctx); err != nil {
					t.Fatal("foreign pin changed", err)
				}
				if err = second.Close(ctx); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}

func TestGraphJournalAcquireRenewContention(t *testing.T) {
	for _, mode := range []string{"open", "renew", "open-replaced", "renew-expired", "open-unknown", "renew-unknown"} {
		t.Run(mode, func(t *testing.T) {
			s, m, now := graphModel(t, journal.JSON)
			ctx := context.Background()
			tail, err := s.Begin(ctx, "flow", "id", 1)
			if err != nil {
				t.Fatal(err)
			}
			tail, err = s.Append(ctx, "flow", "id", 1, journal.Entry{Kind: journal.Started}, tail, nil, nil)
			if err != nil {
				t.Fatal(err)
			}
			tail, err = s.Append(ctx, "flow", "id", 1, journal.Entry{Kind: journal.Completed, Index: 1}, tail, nil, nil)
			if err != nil {
				t.Fatal(err)
			}
			competing, err := s.Open(ctx, "flow", "id", 1)
			if err != nil {
				t.Fatal(err)
			}
			var view *journal.GraphView
			if mode == "renew" || mode == "renew-expired" || mode == "renew-unknown" {
				view, err = s.Open(ctx, "flow", "id", 1)
				if err != nil {
					t.Fatal(err)
				}
				*now = now.Add(time.Second)
			}
			if mode == "open-unknown" || mode == "renew-unknown" {
				if err = m.QueueFault("cas_root", sim.LoseAckAfterCommit); err != nil {
					t.Fatal(err)
				}
				m.PauseBefore("cas_root", func() error { return m.QueueFault("read_root", sim.DropBeforeCommit) })
			} else {
				m.PauseBefore("cas_root", func() error {
					if mode == "open-replaced" {
						if e := s.Retire(ctx, "flow", "id", 1, tail); e != nil {
							return e
						}
						_, e := s.Begin(ctx, "flow", "id", 2)
						return e
					}
					if mode == "renew-expired" {
						*now = now.Add(time.Minute)
					}
					return competing.Close(ctx)
				})
			}
			if view == nil {
				view, err = s.Open(ctx, "flow", "id", 1)
			} else {
				err = view.Renew(ctx)
			}
			switch mode {
			case "open-replaced":
				if !errors.Is(err, journal.ErrStale) || view != nil {
					t.Fatal("replaced generation opened", err)
				}
			case "renew-expired":
				if !errors.Is(err, graphpublication.ErrRevoked) {
					t.Fatal("expired lease renewed", err)
				}
			case "open-unknown", "renew-unknown":
				if err == nil {
					t.Fatal("ambiguous mutation retried")
				}
				*now = now.Add(time.Second)
				if e := competing.Renew(ctx); e != nil {
					t.Fatal("foreign pin changed", e)
				}
			default:
				if err != nil {
					t.Fatal(err)
				}
				if view.Count() != 2 {
					t.Fatal("snapshot changed", view.Count())
				}
				if err = view.Close(ctx); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}

func TestGraphJournalTerminalSnapshotAdmission(t *testing.T) {
	s, m, _ := graphModel(t, journal.JSON)
	ctx := context.Background()
	if view, err := s.OpenTerminal(ctx, "flow", "id", 1); view != nil || err != nil {
		t.Fatal("uninitialized not pending", err)
	}
	tail, err := s.Begin(ctx, "flow", "id", 1)
	if err != nil {
		t.Fatal(err)
	}
	tail, err = s.Append(ctx, "flow", "id", 1, journal.Entry{Kind: journal.Started}, tail, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	keys, err := m.RootKeys(ctx)
	if err != nil || len(keys) != 1 {
		t.Fatal(keys, err)
	}
	before, err := m.ReadRoot(ctx, keys[0])
	if err != nil {
		t.Fatal(err)
	}
	if view, e := s.OpenTerminal(ctx, "flow", "id", 1); view != nil || e != nil {
		t.Fatal("active not pending", e)
	}
	after, err := m.ReadRoot(ctx, keys[0])
	if err != nil || after.Head != before.Head || len(after.Readers) != 0 {
		t.Fatal("pending poll acquired reader", after, err)
	}
	tail, err = s.Append(ctx, "flow", "id", 1, journal.Entry{Kind: journal.Completed, Index: 1}, tail, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	view, err := s.OpenTerminal(ctx, "flow", "id", 1)
	if err != nil || view == nil || view.Count() != 2 {
		t.Fatal(view, err)
	}
	if err = view.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if err = s.Retire(ctx, "flow", "id", 1, tail); err != nil {
		t.Fatal(err)
	}
	if view, err = s.OpenTerminal(ctx, "flow", "id", 1); view != nil || !errors.Is(err, journal.ErrStale) {
		t.Fatal("retired terminal admitted", err)
	}
	if _, err = s.Begin(ctx, "flow", "id", 2); err != nil {
		t.Fatal(err)
	}
	if view, err = s.OpenTerminal(ctx, "flow", "id", 1); view != nil || !errors.Is(err, journal.ErrStale) {
		t.Fatal("replaced terminal admitted", err)
	}
}
