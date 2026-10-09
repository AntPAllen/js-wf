package visibility

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
)

func TestPostgresNamespaceIsolation(t *testing.T) {
	dsn := os.Getenv("WF_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("set WF_TEST_POSTGRES_DSN")
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	stores := []*PostgresStore{{DB: db}, {DB: db, Namespace: "graph/account-a"}, {DB: db, Namespace: "graph/account-b'; DROP TABLE wf_visibility;--"}}
	for i, s := range stores {
		if err := s.Init(ctx); err != nil {
			t.Fatal(err)
		}
		if i > 0 {
			defer db.ExecContext(context.Background(), "DROP TABLE "+s.tableName())
		}
		if err := s.Put(ctx, Row{Type: "namespace-test", ID: "shared", Status: "completed", InvSeq: uint64(i + 1), Attributes: map[string]string{"account": s.Namespace}}, "old"); err != nil {
			t.Fatal(err)
		}
		defer s.Delete(context.Background(), "namespace-test", "shared")
	}
	for i, s := range stores {
		row, err := s.Get(ctx, "namespace-test", "shared")
		if err != nil || row.InvSeq != uint64(i+1) {
			t.Fatalf("store %d row=%+v err=%v", i, row, err)
		}
		rows, err := s.QueryPage(ctx, "completed", map[string]string{"account": s.Namespace}, "", "", 10)
		if err != nil || len(rows) != 1 || rows[0].InvSeq != uint64(i+1) {
			t.Fatalf("page %d rows=%v err=%v", i, rows, err)
		}
		rows, err = s.ListByAttribute(ctx, "account", s.Namespace, "completed")
		if err != nil || len(rows) != 1 {
			t.Fatalf("attribute query %d: %v %v", i, rows, err)
		}
	}
	// Upsert and ordered status queries must remain scoped to the selected table.
	a, b := stores[1], stores[2]
	if err := a.Put(ctx, Row{Type: "namespace-test", ID: "shared", Status: "running", InvSeq: 2}, "current"); err != nil {
		t.Fatal(err)
	}
	rows, err := a.List(ctx, "completed")
	if err != nil || len(rows) != 0 {
		t.Fatalf("upsert status: %v %v", rows, err)
	}
	rows, err = a.List(ctx, "running")
	if err != nil || len(rows) != 1 {
		t.Fatalf("status list: %v %v", rows, err)
	}
	rows, err = a.QueryPage(ctx, "", nil, "namespace-test", "shared", 10)
	if err != nil || len(rows) != 0 {
		t.Fatalf("seek cursor: %v %v", rows, err)
	}
	if err := a.withWriter(ctx, func(ctx context.Context) error {
		// A separate connection to the same namespace must contend, while B and
		// legacy can progress independently. Using another pool avoids reentrant
		// acquisition of a session-level PostgreSQL lock.
		otherDB, err := sql.Open("pgx", dsn)
		if err != nil {
			return err
		}
		defer otherDB.Close()
		other := &PostgresStore{DB: otherDB, Namespace: a.Namespace}
		if err := other.withWriter(ctx, func(context.Context) error { t.Fatal("second writer entered"); return nil }); !errors.Is(err, ErrPostgresWriterBusy) {
			t.Fatalf("same namespace lock: %v", err)
		}
		for _, s := range []*PostgresStore{b, stores[0]} {
			if err := s.withWriter(ctx, func(context.Context) error { return nil }); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := a.withWriter(ctx, func(context.Context) error { return nil }); err != nil {
		t.Fatalf("released writer lock: %v", err)
	}
	if err := a.DeleteGeneration(ctx, "namespace-test", "shared", 99); err != nil {
		t.Fatal(err)
	}
	if _, err := a.Get(ctx, "namespace-test", "shared"); err != nil {
		t.Fatal(err)
	}
	if err := a.DeleteOtherGenerations(ctx, "new"); err != nil {
		t.Fatal(err)
	}
	if _, err := a.Get(ctx, "namespace-test", "shared"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("cleanup: %v", err)
	}
	for _, s := range []*PostgresStore{stores[0], b} {
		if _, err := s.Get(ctx, "namespace-test", "shared"); err != nil {
			t.Fatalf("other namespace removed: %v", err)
		}
	}
	if err := b.DeleteGeneration(ctx, "namespace-test", "shared", 3); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Get(ctx, "namespace-test", "shared"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("generation delete: %v", err)
	}
}
