package integration_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
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
	store := &visibility.PostgresStore{DB: db}
	p, err := visibility.New(ctx, all[0], visibility.WithPostgres(store))
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
	for _, extra := range []visibility.Row{
		{Type: typ, ID: "postgres-a", Status: "queued", Attributes: map[string]string{"team": "alpha"}},
		{Type: typ, ID: "postgres-z", Status: "queued", Attributes: map[string]string{"team": "alpha"}},
	} {
		if err := store.Put(ctx, extra, "test-page"); err != nil {
			t.Fatal(err)
		}
	}
	var ids []string
	for cursor := ""; ; {
		page, err := p.ListPage(ctx, "queued", cursor, 1)
		if err != nil || len(page.Rows) != 1 {
			t.Fatalf("page=%+v err=%v", page, err)
		}
		ids = append(ids, page.Rows[0].ID)
		if page.Next == "" {
			break
		}
		cursor = page.Next
	}
	if len(ids) != 3 || ids[0] != "postgres-a" || ids[1] != id || ids[2] != "postgres-z" {
		t.Fatalf("paged ids=%v", ids)
	}
	first, err := p.ListByAttributePage(ctx, "team", "alpha", "queued", "", 1)
	if err != nil || len(first.Rows) != 1 || first.Rows[0].ID != "postgres-a" || first.Next == "" {
		t.Fatalf("attribute page=%+v err=%v", first, err)
	}
	second, err := p.ListByAttributePage(ctx, "team", "alpha", "queued", first.Next, 1)
	if err != nil || len(second.Rows) != 1 || second.Rows[0].ID != "postgres-z" || second.Next != "" {
		t.Fatalf("attribute next=%+v err=%v", second, err)
	}
	if err := p.Rebuild(ctx); err != nil {
		t.Fatal(err)
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
	otherDB, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer otherDB.Close()
	other, err := visibility.New(ctx, all[2], visibility.WithPostgres(&visibility.PostgresStore{DB: otherDB}))
	if err != nil {
		t.Fatal(err)
	}
	if err := other.Rebuild(ctx); !errors.Is(err, visibility.ErrPostgresWriterBusy) {
		t.Fatalf("second PostgreSQL writer was allowed during Run: %v", err)
	}
	stopProject()
	if err := <-projectDone; err != nil && !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	stopWorker()
	if err := <-workerDone; err != nil {
		t.Fatal(err)
	}
	if err := other.Rebuild(ctx); err != nil {
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

func TestPostgresIncrementalPurgeAndLateEvent(t *testing.T) {
	dsn := os.Getenv("WF_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("set WF_TEST_POSTGRES_DSN for PostgreSQL integration test")
	}
	all, _ := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
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
	const typ, id = "test", "postgres-incremental-purge"
	c := client.New(all[0])
	first, err := c.Start(ctx, typ, id, []byte(`null`))
	if err != nil {
		t.Fatal(err)
	}
	w, err := worker.New(ctx, all[1], "pg-purge-worker", map[string]worker.Handler{typ: func(_ *wf.Context, _ json.RawMessage) (json.RawMessage, error) {
		return json.RawMessage(`null`), nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	workerCtx, stopWorker := context.WithCancel(ctx)
	workerDone := make(chan error, 1)
	go func() { workerDone <- w.RunPartition(workerCtx, identity.Partition(typ, id, provision.Partitions)) }()
	projectCtx, stopProject := context.WithCancel(ctx)
	projectDone := make(chan error, 1)
	go func() { projectDone <- p.Run(projectCtx) }()
	defer func() {
		stopProject()
		<-projectDone
	}()
	if _, err := c.Await(ctx, typ, id); err != nil {
		t.Fatal(err)
	}
	stopWorker()
	if err := <-workerDone; err != nil {
		t.Fatal(err)
	}
	for ctx.Err() == nil {
		row, err := p.Get(ctx, typ, id)
		lag, lagErr := p.Lag(ctx)
		if err == nil && row.Status == "completed" && lagErr == nil && lag == 0 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if ctx.Err() != nil {
		t.Fatal("PostgreSQL did not project terminal result")
	}
	if err := retention.Purge(ctx, all[0], typ, id, time.Minute); err != nil {
		t.Fatal(err)
	}
	for ctx.Err() == nil {
		_, err := p.Get(ctx, typ, id)
		lag, lagErr := p.Lag(ctx)
		if errors.Is(err, visibility.ErrNotFound) && lagErr == nil && lag == 0 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if ctx.Err() != nil {
		t.Fatal("PostgreSQL purge event did not remove the row")
	}
	second, err := c.Start(ctx, typ, id, []byte(`null`))
	if err != nil || second.InvSeq == first.InvSeq {
		t.Fatalf("reused start=%+v err=%v", second, err)
	}
	secondWorker, err := worker.New(ctx, all[1], "pg-purge-reuse-worker", map[string]worker.Handler{typ: func(_ *wf.Context, _ json.RawMessage) (json.RawMessage, error) {
		return json.RawMessage(`null`), nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	secondCtx, stopSecond := context.WithCancel(ctx)
	secondDone := make(chan error, 1)
	go func() {
		secondDone <- secondWorker.RunPartition(secondCtx, identity.Partition(typ, id, provision.Partitions))
	}()
	if _, err := c.Await(ctx, typ, id); err != nil {
		t.Fatal(err)
	}
	stopSecond()
	if err := <-secondDone; err != nil {
		t.Fatal(err)
	}
	for ctx.Err() == nil {
		row, err := p.Get(ctx, typ, id)
		lag, lagErr := p.Lag(ctx)
		if err == nil && row.InvSeq == second.InvSeq && row.Status == "completed" && lagErr == nil && lag == 0 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if ctx.Err() != nil {
		t.Fatal("PostgreSQL did not project the reused generation")
	}
	if _, err := all[0].Publish(ctx, "wf.purge."+typ+"."+id, []byte(strconv.FormatUint(first.InvSeq, 10))); err != nil {
		t.Fatal(err)
	}
	for ctx.Err() == nil {
		lag, err := p.Lag(ctx)
		if err == nil && lag == 0 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if ctx.Err() != nil {
		t.Fatal("late purge event was not acknowledged")
	}
	row, err := p.Get(ctx, typ, id)
	if err != nil || row.InvSeq != second.InvSeq || row.Status != "completed" {
		t.Fatalf("late event changed reused row=%+v err=%v", row, err)
	}
}

// TestPostgresPurgeFeedScale exercises the sink and durable purge consumer at
// the plan's 10,000-invocation retention scale. It uses materialized rows so
// the test isolates event-drain throughput from workflow execution time.
func TestPostgresPurgeFeedScale(t *testing.T) {
	if os.Getenv("WF_PG_PURGE_SCALE") == "" {
		t.Skip("set WF_PG_PURGE_SCALE=1 for the 10,000-event PostgreSQL purge proof")
	}
	dsn := os.Getenv("WF_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("set WF_TEST_POSTGRES_DSN to a disposable PostgreSQL database")
	}
	count := 10000
	if value := os.Getenv("WF_PG_PURGE_COUNT"); value != "" {
		var err error
		count, err = strconv.Atoi(value)
		if err != nil || count < 1 || count > 10000 {
			t.Fatalf("invalid WF_PG_PURGE_COUNT %q", value)
		}
	}
	all, _ := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	store := &visibility.PostgresStore{DB: db}
	p, err := visibility.New(ctx, all[0], visibility.WithPostgres(store))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `TRUNCATE wf_visibility`); err != nil {
		t.Fatal(err)
	}
	runCtx, stop := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- p.Run(runCtx) }()
	defer func() {
		stop()
		<-done
	}()
	// Acknowledgment of this sentinel proves the initial rebuild is over.
	if _, err := all[0].Publish(ctx, "wf.purge.test.pg-scale-ready", []byte("1")); err != nil {
		t.Fatal(err)
	}
	purges, err := all[0].Stream(ctx, "WF_PURGE")
	if err != nil {
		t.Fatal(err)
	}
	for ctx.Err() == nil {
		info, err := purges.Info(ctx)
		if err == nil && info.State.Msgs == 0 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if ctx.Err() != nil {
		t.Fatal("PostgreSQL purge consumer did not start")
	}
	start := time.Now()
	for i := 0; i < count; i++ {
		id := fmt.Sprintf("pg-purge-%05d", i)
		// Every tenth row represents a newer generation. Its old-generation
		// event must leave it in place.
		invSeq := uint64(i + 1)
		if i%10 == 0 {
			invSeq += uint64(count)
		}
		if err := store.Put(ctx, visibility.Row{Type: "test", ID: id, InvSeq: invSeq, Status: "completed"}, "purge-scale"); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < count; i++ {
		id := fmt.Sprintf("pg-purge-%05d", i)
		if _, err := all[0].Publish(ctx, "wf.purge.test."+id, []byte(strconv.Itoa(i+1))); err != nil {
			t.Fatal(err)
		}
	}
	for ctx.Err() == nil {
		lag, err := p.Lag(ctx)
		if err == nil && lag == 0 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if ctx.Err() != nil {
		t.Fatal("PostgreSQL purge feed did not drain")
	}
	var remaining int
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM wf_visibility`).Scan(&remaining); err != nil {
		t.Fatal(err)
	}
	want := (count + 9) / 10
	if remaining != want {
		t.Fatalf("PostgreSQL rows after purge=%d, want %d", remaining, want)
	}
	rows, err := db.QueryContext(ctx, `SELECT id, row_data->>'inv_seq' FROM wf_visibility ORDER BY id`)
	if err != nil {
		t.Fatal(err)
	}
	seen := 0
	for rows.Next() {
		var id, sequence string
		if err := rows.Scan(&id, &sequence); err != nil {
			rows.Close()
			t.Fatal(err)
		}
		var index int
		if _, err := fmt.Sscanf(id, "pg-purge-%05d", &index); err != nil || index%10 != 0 || sequence != strconv.Itoa(index+1+count) {
			rows.Close()
			t.Fatalf("unexpected retained generation id=%s inv_seq=%s err=%v", id, sequence, err)
		}
		seen++
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		t.Fatal(err)
	}
	rows.Close()
	if seen != want {
		t.Fatalf("verified newer-generation rows=%d, want %d", seen, want)
	}
	info, err := purges.Info(ctx)
	if err != nil || info.State.Msgs != 0 {
		t.Fatalf("purge work queue retained messages=%d err=%v", info.State.Msgs, err)
	}
	t.Logf("purge events=%d remaining newer generations=%d elapsed=%s", count, remaining, time.Since(start))
}

func TestPostgresVisibilityWriterSessionLoss(t *testing.T) {
	dsn := os.Getenv("WF_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("set WF_TEST_POSTGRES_DSN for PostgreSQL integration test")
	}
	all, _ := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	firstDB, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer firstDB.Close()
	secondDB, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer secondDB.Close()
	first, err := visibility.New(ctx, all[0], visibility.WithPostgres(&visibility.PostgresStore{DB: firstDB}))
	if err != nil {
		t.Fatal(err)
	}
	second, err := visibility.New(ctx, all[1], visibility.WithPostgres(&visibility.PostgresStore{DB: secondDB}))
	if err != nil {
		t.Fatal(err)
	}
	runCtx, stop := context.WithCancel(ctx)
	defer stop()
	done := make(chan error, 1)
	go func() { done <- first.Run(runCtx) }()
	var pid int
	for ctx.Err() == nil {
		err = secondDB.QueryRowContext(ctx, `SELECT pid FROM pg_locks WHERE locktype='advisory' AND mode='ExclusiveLock' AND granted AND database=(SELECT oid FROM pg_database WHERE datname=current_database()) LIMIT 1`).Scan(&pid)
		if err == nil {
			break
		}
		if !errors.Is(err, sql.ErrNoRows) {
			t.Fatal(err)
		}
		time.Sleep(20 * time.Millisecond)
	}
	if ctx.Err() != nil {
		t.Fatal("PostgreSQL writer did not acquire advisory lock")
	}
	var terminated bool
	if err := secondDB.QueryRowContext(ctx, `SELECT pg_terminate_backend($1)`, pid).Scan(&terminated); err != nil {
		t.Skipf("terminating a PostgreSQL backend requires an administrative test role: %v", err)
	}
	if !terminated {
		t.Fatal("PostgreSQL did not terminate the writer lock session")
	}
	select {
	case err := <-done:
		if err == nil || !strings.Contains(err.Error(), "writer lock session lost") {
			t.Fatalf("writer after lock loss: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("writer did not exit after lock session loss")
	}
	if err := second.Rebuild(ctx); err != nil {
		t.Fatalf("replacement writer rebuild: %v", err)
	}
}
