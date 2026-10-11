package graphpublication

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
)

func TestGraphReleaseReaderCurrent(t *testing.T) {
	for _, mode := range []string{"valid", "observe_deadline", "cas_drop", "cas_commit_lost", "cas_commit_unknown", "overtaken_commit", "conflict", "forged", "revoked"} {
		t.Run(mode, func(t *testing.T) {
			m, p, initial, reader := readerFixture(t)
			reads := 0
			committed := false
			m.readRootHook = func(string) error {
				reads++
				if mode == "observe_deadline" || mode == "cas_commit_unknown" && committed {
					return context.DeadlineExceeded
				}
				return nil
			}
			switch mode {
			case "cas_drop":
				m.rootBefore = func(string, uint64, Root) error { return context.DeadlineExceeded }
			case "cas_commit_lost", "cas_commit_unknown", "overtaken_commit":
				m.rootAfter = func() error {
					committed = true
					if mode == "overtaken_commit" {
						r := m.roots["history"]
						r.Head++
						m.roots["history"] = r
					}
					return context.DeadlineExceeded
				}
			case "conflict":
				m.rootBefore = func(string, uint64, Root) error {
					r := m.roots["history"]
					r.Head++ // A competing authority publication won the observed head.
					m.roots["history"] = r
					return ErrConflict
				}
			case "forged":
				reader.id = "foreign"
			case "revoked":
				r := m.roots["history"]
				r.Readers = nil
				r.Head++
				m.roots["history"] = r
			}
			ack, err := p.ReleaseReaderCurrent(ctx, reader)
			wantReads := 1
			if mode == "cas_drop" || strings.HasPrefix(mode, "cas_commit_") || mode == "overtaken_commit" {
				wantReads = 2
			}
			if reads != wantReads {
				t.Fatalf("authority reads=%d want=%d", reads, wantReads)
			}
			switch mode {
			case "valid", "cas_commit_lost":
				if err != nil || ack.Head != initial.Head+1 || len(ack.Readers) != 0 {
					t.Fatalf("release: %v %v", ack, err)
				}
			case "conflict":
				if !errors.Is(err, ErrConflict) {
					t.Fatal(err)
				}
			case "forged", "revoked":
				if !errors.Is(err, ErrRevoked) {
					t.Fatal(err)
				}
			default:
				if !errors.Is(err, context.DeadlineExceeded) {
					t.Fatal(err)
				}
				phase := "release reader pin"
				if mode == "observe_deadline" {
					phase = "observe reader release head"
				}
				var release *ReaderReleaseError
				if !errors.As(err, &release) || release.Phase != phase {
					t.Fatal(err)
				}
			}
			final := m.roots["history"]
			if !samePublication(initial, final) {
				t.Fatal("release changed publication")
			}
			removed := mode == "valid" || mode == "cas_commit_lost" || mode == "cas_commit_unknown" || mode == "overtaken_commit" || mode == "revoked"
			if (len(final.Readers) == 0) != removed {
				t.Fatal("reader outcome differs")
			}
			if !removed && !reflect.DeepEqual(initial.Readers, final.Readers) {
				t.Fatal("live pin changed")
			}
		})
	}
}
