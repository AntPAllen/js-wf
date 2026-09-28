package integration_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"os"
	"testing"
	"time"

	"js-wf/client"
	"js-wf/identity"
	"js-wf/provision"
	"js-wf/retention"
	"js-wf/visibility"
	"js-wf/wf"
	"js-wf/worker"

	_ "github.com/jackc/pgx/v5/stdlib"
)

func TestPostgresVisibilityProjection(t *testing.T) {
	dsn := os.Getenv("WF_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("set WF_TEST_POSTGRES_DSN for PostgreSQL integration test")
	}
	all, _ := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	p, err := visibility.New(ctx, all[0], visibility.WithPostgres(&visibility.PostgresStore{DB: db}))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `TRUNCATE wf_visibility`); err != nil {
		t.Fatal(err)
	}
	const typ, id = "test", "postgres-visible"
	c := client.New(all[0])
	if _, err := c.Start(ctx, typ, id, []byte(`1`)); err != nil {
		t.Fatal(err)
	}
	if err := p.Rebuild(ctx); err != nil {
		t.Fatal(err)
	}
	row, err := p.Get(ctx, typ, id)
	if err != nil || row.Status != "queued" {
		t.Fatalf("queued row=%+v err=%v", row, err)
	}
	w, err := worker.New(ctx, all[1], "pg-visibility-worker", map[string]worker.Handler{typ: func(c *wf.Context, input json.RawMessage) (json.RawMessage, error) {
		if err := c.SetSearchAttributes(map[string]string{"team": "alpha"}); err != nil {
			return nil, err
		}
		return input, nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	workerCtx, stopWorker := context.WithCancel(ctx)
	defer stopWorker()
	workerDone := make(chan error, 1)
	go func() { workerDone <- w.RunPartition(workerCtx, identity.Partition(typ, id, provision.Partitions)) }()
	projectCtx, stopProject := context.WithCancel(ctx)
	projectDone := make(chan error, 1)
	go func() { projectDone <- p.Run(projectCtx) }()
	if value, err := c.Await(ctx, typ, id); err != nil || string(value) != "1" {
		t.Fatalf("result=%s err=%v", value, err)
	}
	for ctx.Err() == nil {
		rows, err := p.ListByAttribute(ctx, "team", "alpha", "completed")
		lag, lagErr := p.Lag(ctx)
		if err == nil && lagErr == nil && len(rows) == 1 && lag == 0 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if ctx.Err() != nil {
		t.Fatal("PostgreSQL projection did not catch up")
	}
	stopProject()
	if err := <-projectDone; err != nil && !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	stopWorker()
	if err := <-workerDone; err != nil {
		t.Fatal(err)
	}
	if err := p.Rebuild(ctx); err != nil {
		t.Fatal(err)
	}
	rows, err := p.List(ctx, "completed")
	if err != nil || len(rows) != 1 || rows[0].ID != id {
		t.Fatalf("completed rows=%+v err=%v", rows, err)
	}
	if err := retention.Purge(ctx, all[0], typ, id, time.Minute); err != nil {
		t.Fatal(err)
	}
	if err := p.Rebuild(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := p.Get(ctx, typ, id); !errors.Is(err, visibility.ErrNotFound) {
		t.Fatalf("purged row: %v", err)
	}
	if _, err := c.Start(ctx, typ, id, []byte(`2`)); err != nil {
		t.Fatal(err)
	}
	if err := p.Rebuild(ctx); err != nil {
		t.Fatal(err)
	}
	row, err = p.Get(ctx, typ, id)
	if err != nil || row.Status != "queued" || row.Attributes["team"] != "" {
		t.Fatalf("reused row=%+v err=%v", row, err)
	}
	rows, err = p.ListByAttribute(ctx, "team", "alpha", "")
	if err != nil || len(rows) != 0 {
		t.Fatalf("stale attribute rows=%+v err=%v", rows, err)
	}
}
