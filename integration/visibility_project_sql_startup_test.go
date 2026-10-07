package integration_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"github.com/nats-io/nats.go/jetstream"
	"js-wf/visibility"
	"os"
	"path/filepath"
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
	progress := map[string]any{"signal": signalName, "blocker_backend_pid": blocker, "blocked_backend_pid": backend, "query": query, "wait_event_type": waitType, "wait_event": waitEvent, "blocked_statement_admitted_at": admittedAt, "signal_sent_at": sentAt, "child": child.record}
	defer func() {
		data, e := json.MarshalIndent(progress, "", "  ")
		if e == nil {
			e = os.WriteFile(filepath.Join(root, "sql-startup-boundary-progress.json"), append(data, '\n'), 0600)
		}
		if e != nil {
			t.Error("retain SQL startup progress", e)
		}
	}()
	// Reaping a client does not synchronously join the remote PostgreSQL
	// backend. Observe its cleanup while retaining the blocker, within the
	// original ten-second signal budget (including process join).
	cleanupCtx, cancelCleanup := context.WithDeadline(ctx, sentAt.Add(10*time.Second))
	defer cancelCleanup()
	if err = child.command.Process.Signal(signal); err != nil {
		t.Fatal(err)
	}
	select {
	case <-child.joined:
	case <-cleanupCtx.Done():
		t.Fatalf("SQL startup %s did not join while lock remained held", signalName)
	}
	record := child.recordExit(t)
	progress["child"] = record
	if child.err != nil || child.command.ProcessState.ExitCode() != 0 {
		t.Fatalf("SQL startup %s: %v", signalName, child.err)
	}
	// pgx cancellation and database Close must have removed every child session,
	// including the blocked statement, before the blocking lock is released.
	var sessions, locks, rows int
	var observations []map[string]any
	progress["cleanup_observations"] = &observations
	for {
		if err = db.QueryRowContext(cleanupCtx, `SELECT (SELECT count(*) FROM pg_stat_activity WHERE application_name=$1),(SELECT count(*) FROM pg_locks WHERE pid=$2)`, record["application_name"], backend).Scan(&sessions, &locks); err != nil {
			t.Fatal("SQL startup cleanup observation", err)
		}
		observations = append(observations, map[string]any{"observed_at": time.Now().UTC(), "sessions": sessions, "locks": locks})
		if sessions == 0 && locks == 0 {
			break
		}
		select {
		case <-cleanupCtx.Done():
			t.Fatalf("SQL startup cleanup exceeded original10s: sessions=%d locks=%d", sessions, locks)
		case <-time.After(10 * time.Millisecond):
		}
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
	if !checkedAt.Before(sentAt.Add(10 * time.Second)) {
		t.Fatal("SQL startup cleanup exceeded original10s")
	}
	if err = tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	releasedAt := time.Now().UTC()
	progress["residual_checked_at"] = checkedAt
	progress["blocker_released_at"] = releasedAt
	progress["complete"] = true
	proof := map[string]any{"child": record, "blocker_backend_pid": blocker, "blocked_backend_pid": backend, "query": query, "wait_event_type": waitType, "wait_event": waitEvent, "blocking_pid_confirmed": blocked, "lock_held_at": lockedAt, "blocked_statement_admitted_at": admittedAt, signalField: sentAt, "residual_checked_at": checkedAt, "blocker_released_at": releasedAt, "residual_sessions": sessions, "residual_locks": locks, "rows": rows, "projection_durables_absent": true}
	if signal == syscall.SIGINT {
		proof["signal"] = signalName
	}
	t.Logf("SQL startup cancelled: pid=%d backend=%d blocker=%d wait=%s/%s exit=0 residual_sessions=0 residual_locks=0 rows=0 durables_absent=true%s", child.command.Process.Pid, backend, blocker, waitType, waitEvent, logSuffix)
	return proof
}
