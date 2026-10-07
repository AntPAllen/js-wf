package integration_test

import (
	"context"
	"database/sql"
	"testing"
	"time"
)

func faultStandaloneSQLLeaf(t *testing.T, ctx context.Context, db *sql.DB, binary, root, endpoint, domain string, leaf *projectionLeafFault) map[string]any {
	t.Helper()
	var baseline, rows int
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM wf_visibility`).Scan(&baseline); err != nil {
		t.Fatal(err)
	}
	child := startStandaloneProjection(t, ctx, db, binary, root, "leaf-fault", endpoint, domain, true)
	for {
		select {
		case <-child.joined:
			t.Fatalf("leaf-fault child exited before admission: %v", child.err)
		default:
		}
		if err := db.QueryRowContext(ctx, `SELECT count(*) FROM wf_visibility`).Scan(&rows); err != nil {
			t.Fatal(err)
		}
		if rows > baseline && rows < 50000 {
			break
		}
		if rows == 50000 || ctx.Err() != nil {
			t.Fatal("leaf-fault partial progress not admitted", rows, ctx.Err())
		}
		time.Sleep(10 * time.Millisecond)
	}
	started := time.Now().UTC()
	leaf.kill()
	select {
	case <-child.joined:
	case <-time.After(15 * time.Second):
		t.Fatal("projector did not report fatal transport loss while leaf remained killed")
	}
	record := child.recordExit(t)
	if record["exit_code"] != 1 {
		t.Fatalf("leaf-fault child exit=%v want1", record["exit_code"])
	}
	var sessions int
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM pg_stat_activity WHERE application_name=$1`, record["application_name"]).Scan(&sessions); err != nil || sessions != 0 {
		t.Fatalf("leaf-fault SQL sessions=%d err=%v", sessions, err)
	}
	var healthy int
	if err := db.QueryRowContext(ctx, `SELECT 1`).Scan(&healthy); err != nil || healthy != 1 {
		t.Fatal("SQL must remain healthy through transport fault", healthy, err)
	}
	checked := time.Now().UTC()
	leaf.restart()
	healed := time.Now().UTC()
	if healed.Sub(started) >= 30*time.Second {
		t.Fatal("whole SQL leaf transport cut exceeds original30s recovery target", healed.Sub(started))
	}
	result := map[string]any{"child": record, "baseline_rows": baseline, "admitted_rows": rows, "fault_started": started, "sql_sessions_after_reap": sessions, "sql_healthy_after_reap": true, "sql_checked_at": checked, "healed_at": healed, "sql_backend_terminated": false}
	t.Logf("SQL leaf transport fault: pid=%d backend=%d baseline_rows=%d admitted_rows=%d exit=1 SQL_healthy=true sessions=0", child.command.Process.Pid, record["writer_backend_pid"], baseline, rows)
	return result
}
