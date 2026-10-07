package integration_test

import (
	"context"
	"database/sql"
	"errors"
	"github.com/nats-io/nats.go/jetstream"
	"js-wf/visibility"
	"syscall"
	"testing"
	"time"
)

// The blocking transaction remains held until the actual child is reaped.
// PostgreSQL's lock graph admits the startup boundary, without a sleep or a
// mocked SQL driver. The subsequent original full workload proves reuse.
func cancelStandaloneSQLStartup(t *testing.T, ctx context.Context, db *sql.DB, js jetstream.JetStream, binary, root, endpoint, domain string, signal syscall.Signal) map[string]any {
	t.Helper()
	if signal != syscall.SIGTERM && signal != syscall.SIGINT {
		t.Fatal("unsupported SQL startup signal", signal)
	}
	signalName, signalField, logSuffix := "SIGTERM", "sigterm_sent_at", ""
	if signal == syscall.SIGINT {
		signalName, signalField, logSuffix = "SIGINT", "sigint_sent_at", " signal=SIGINT"
	}
	if err := (&visibility.PostgresStore{DB: db}).Init(ctx); err != nil {
		t.Fatal(err)
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	var blocker int
	if err = tx.QueryRowContext(ctx, `SELECT pg_backend_pid()`).Scan(&blocker); err != nil {
		t.Fatal(err)
	}
	if _, err = tx.ExecContext(ctx, `LOCK TABLE wf_visibility IN ACCESS EXCLUSIVE MODE`); err != nil {
		t.Fatal(err)
	}
	lockedAt := time.Now().UTC()
	child := startStandaloneProjectionReady(t, ctx, db, binary, root, "sql-startup", endpoint, domain, true, blocker)
	backend := child.record["writer_backend_pid"].(int)
	var query, waitType, waitEvent string
	var blocked bool
	if err = db.QueryRowContext(ctx, `SELECT query,wait_event_type,wait_event,$2=ANY(pg_blocking_pids(pid)) FROM pg_stat_activity WHERE pid=$1`, backend, blocker).Scan(&query, &waitType, &waitEvent, &blocked); err != nil || !blocked || waitType != "Lock" {
		t.Fatalf("SQL startup lock admission query=%q wait=%s/%s blocked=%t err=%v", query, waitType, waitEvent, blocked, err)
	}
	admittedAt := time.Now().UTC()
	sentAt := time.Now().UTC()
	if err = child.command.Process.Signal(signal); err != nil {
		t.Fatal(err)
	}
	select {
	case <-child.joined:
	case <-time.After(10 * time.Second):
		t.Fatalf("SQL startup %s did not join while lock remained held", signalName)
	}
	record := child.recordExit(t)
	if child.err != nil || child.command.ProcessState.ExitCode() != 0 {
		t.Fatalf("SQL startup %s: %v", signalName, child.err)
	}
	// pgx cancellation and database Close must have removed every child session,
	// including the blocked statement, before the blocking lock is released.
	var sessions, locks, rows int
	if err = db.QueryRowContext(ctx, `SELECT count(*) FROM pg_stat_activity WHERE application_name=$1`, record["application_name"]).Scan(&sessions); err != nil || sessions != 0 {
		t.Fatalf("SQL startup residual sessions=%d err=%v", sessions, err)
	}
	if err = db.QueryRowContext(ctx, `SELECT count(*) FROM pg_locks WHERE pid=$1`, backend).Scan(&locks); err != nil || locks != 0 {
		t.Fatalf("SQL startup residual locks=%d err=%v", locks, err)
	}
	if err = tx.QueryRowContext(ctx, `SELECT count(*) FROM wf_visibility`).Scan(&rows); err != nil || rows != 0 {
		t.Fatalf("SQL startup mutated rows=%d err=%v", rows, err)
	}
	for _, source := range []struct{ stream, name string }{{"WF_JRN", "WF_VIEW_PG"}, {"WF_PURGE", "WF_VIEW_PG_PURGE"}} {
		stream, e := js.Stream(ctx, source.stream)
		if e != nil {
			t.Fatal(e)
		}
		_, e = stream.Consumer(ctx, source.name)
		if !errors.Is(e, jetstream.ErrConsumerNotFound) {
			t.Fatalf("SQL startup created durable %s: %v", source.name, e)
		}
	}
	checkedAt := time.Now().UTC()
	if err = tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	releasedAt := time.Now().UTC()
	proof := map[string]any{"child": record, "blocker_backend_pid": blocker, "blocked_backend_pid": backend, "query": query, "wait_event_type": waitType, "wait_event": waitEvent, "blocking_pid_confirmed": blocked, "lock_held_at": lockedAt, "blocked_statement_admitted_at": admittedAt, signalField: sentAt, "residual_checked_at": checkedAt, "blocker_released_at": releasedAt, "residual_sessions": sessions, "residual_locks": locks, "rows": rows, "projection_durables_absent": true}
	if signal == syscall.SIGINT {
		proof["signal"] = signalName
	}
	t.Logf("SQL startup cancelled: pid=%d backend=%d blocker=%d wait=%s/%s exit=0 residual_sessions=0 residual_locks=0 rows=0 durables_absent=true%s", child.command.Process.Pid, backend, blocker, waitType, waitEvent, logSuffix)
	return proof
}
