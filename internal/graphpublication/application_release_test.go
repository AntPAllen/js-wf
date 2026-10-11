package graphpublication

import (
	"bytes"
	"context"
	"errors"
	"reflect"
	"testing"
	"time"
)

func TestGraphApplicationAndReaderReleaseAtomic(t *testing.T) {
	for _, mode := range []string{"success", "stale_head", "missing_pin", "forged_snapshot", "oversized", "cancelled_before", "conflict", "rejected", "committed_lost", "committed_unknown", "committed_replaced", "committed_cancelled"} {
		t.Run(mode, func(t *testing.T) {
			m, p := newModel(mode)
			root := EmptyRoot()
			for _, stream := range []string{"input", "history"} {
				prepared, err := p.PrepareStreamAppendWithApplication(ctx, "owner", root.Head, stream, []byte(stream), [][]byte{[]byte("owned-" + stream)}, nil, epoch.Add(time.Hour), []byte("old"))
				if err != nil {
					t.Fatal(err)
				}
				root, err = p.Commit(ctx, prepared)
				if err != nil {
					t.Fatal(err)
				}
			}
			reader, root, err := p.AcquireReader(ctx, "owner", root.Head, epoch.Add(time.Hour))
			if err != nil {
				t.Fatal(err)
			}
			foreign, root, err := p.AcquireReader(ctx, "owner", root.Head, epoch.Add(time.Hour))
			if err != nil {
				t.Fatal(err)
			}
			if mode == "missing_pin" {
				root, err = p.ReleaseReader(ctx, reader, root.Head)
				if err != nil {
					t.Fatal(err)
				}
			}
			before := cloneRoot(root)
			oldObjects := make(map[string][]byte, len(m.objects))
			for name, data := range m.objects {
				oldObjects[name] = bytes.Clone(data)
			}
			expected := root.Head
			data := []byte("new")
			attempt, cancel := context.WithCancel(ctx)
			defer cancel()
			cause := error(nil)
			switch mode {
			case "stale_head":
				expected--
				cause = ErrConflict
			case "missing_pin":
				cause = ErrRevoked
			case "forged_snapshot":
				reader.streams = copyStreams(reader.streams)
				reader.streams[0].Graph.Count++
				cause = ErrRevoked
			case "oversized":
				data = make([]byte, MaxApplicationBytes+1)
			case "cancelled_before":
				cancel()
				cause = context.Canceled
			case "conflict":
				cause = ErrConflict
			case "rejected", "committed_unknown", "committed_replaced", "committed_cancelled":
				cause = context.DeadlineExceeded
			}
			reads, writes := 0, 0
			m.readRootHook = func(string) error { reads++; return nil }
			m.rootBefore = func(string, uint64, Root) error {
				writes++
				if mode == "conflict" {
					return ErrConflict
				}
				if mode == "rejected" {
					return context.DeadlineExceeded
				}
				return nil
			}
			if mode == "committed_lost" || mode == "committed_unknown" || mode == "committed_replaced" || mode == "committed_cancelled" {
				m.rootAfter = func() error {
					switch mode {
					case "committed_unknown":
						m.readRootHook = func(string) error { reads++; return context.DeadlineExceeded }
					case "committed_replaced":
						r := m.roots["owner"]
						r.Head++
						r.Application = []byte("successor")
						m.roots["owner"] = r
					case "committed_cancelled":
						cancel()
					}
					return context.DeadlineExceeded
				}
			}
			ack, err := p.UpdateApplicationAndReleaseReader(attempt, reader, expected, data)
			success := mode == "success" || mode == "committed_lost"
			if success {
				if err != nil || ack.Head != before.Head+1 || string(ack.Application) != "new" {
					t.Fatal("atomic acknowledgement", ack, err)
				}
				// Neither the caller's bytes nor returned slices can alter authority.
				data[0] = '!'
				ack.Readers[0].ID = "changed"
				ack.Application[0] = '!'
			} else {
				if err == nil || ack.Head != 0 || cause != nil && !errors.Is(err, cause) {
					t.Fatal("failure classification", ack, err)
				}
			}
			committed := success || mode == "committed_unknown" || mode == "committed_replaced" || mode == "committed_cancelled"
			wantWrites := 1
			if mode == "stale_head" || mode == "missing_pin" || mode == "forged_snapshot" || mode == "oversized" || mode == "cancelled_before" {
				wantWrites = 0
			}
			if writes != wantWrites {
				t.Fatal("publication retried or attempted after invalid admission", writes, wantWrites)
			}
			if mode == "conflict" && reads != 1 || mode == "committed_cancelled" && reads != 1 {
				t.Fatal("unexpected readback", reads)
			}
			if mode == "committed_lost" && reads != 2 || mode == "committed_unknown" && reads != 2 {
				t.Fatal("exact readback missing", reads)
			}
			m.rootBefore, m.rootAfter, m.readRootHook = nil, nil, nil
			actual, err := m.ReadRoot(ctx, "owner")
			if err != nil {
				t.Fatal(err)
			}
			if committed {
				if len(actual.Readers) != 1 || actual.Readers[0].ID != foreign.id || !reflect.DeepEqual(actual.Streams, before.Streams) || !reflect.DeepEqual(actual.Graph, before.Graph) || actual.Token != before.Token {
					t.Fatal("atomic mutation changed ownership or foreign reader", actual)
				}
				want := "new"
				if mode == "committed_replaced" {
					want = "successor"
				}
				if string(actual.Application) != want {
					t.Fatal("wrong committed descriptor", string(actual.Application))
				}
			} else if !reflect.DeepEqual(actual, before) {
				t.Fatal("rejected mutation changed authority")
			}
			if !reflect.DeepEqual(m.objects, oldObjects) {
				t.Fatal("metadata mutation changed immutable objects")
			}
			record, err := p.ReadRetainedStream(ctx, foreign, "input", 0, epoch.Add(time.Minute))
			if err != nil || string(record.Data) != "input" {
				t.Fatal("foreign reader lost payload access", record, err)
			}
		})
	}
}
