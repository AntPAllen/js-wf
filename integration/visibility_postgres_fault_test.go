package integration_test

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"js-wf/testcluster"
	"js-wf/visibility"
)

func captureProjectionProcess(t *testing.T, process *exec.Cmd, root string, proof map[string]any) {
	t.Helper()
	file, err := os.Open(fmt.Sprintf("/proc/%d/exe", process.Process.Pid))
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	digest := sha256.New()
	if _, err := io.Copy(digest, file); err != nil {
		t.Fatal(err)
	}
	parent, err := os.Open(os.Args[0])
	if err != nil {
		t.Fatal(err)
	}
	defer parent.Close()
	expected := sha256.New()
	if _, err := io.Copy(expected, parent); err != nil {
		t.Fatal(err)
	}
	if fmt.Sprintf("%x", digest.Sum(nil)) != fmt.Sprintf("%x", expected.Sum(nil)) {
		t.Fatal("projection SDK differs from parent")
	}
	info, err := exec.Command("go", "version", "-m", fmt.Sprintf("/proc/%d/exe", process.Process.Pid)).Output()
	if err != nil {
		t.Fatal(err)
	}
	proof["actual_projection_sdk_pid"] = process.Process.Pid
	proof["actual_projection_sdk_sha256"] = fmt.Sprintf("%x", digest.Sum(nil))
	proof["actual_projection_sdk_build_info"] = string(info)
	proof["projection_process_observed_before_kill"] = time.Now().UTC()
	if err := os.WriteFile(filepath.Join(root, "projection-sdk-build.txt"), info, 0600); err != nil {
		t.Fatal(err)
	}
}

func assertProjectionSIGKILL(t *testing.T, process *exec.Cmd, proof map[string]any) {
	t.Helper()
	state, ok := process.ProcessState.Sys().(syscall.WaitStatus)
	if !ok || !state.Signaled() || state.Signal() != syscall.SIGKILL {
		t.Fatalf("projection not reaped SIGKILL: %v", process.ProcessState)
	}
	proof["projection_process_reaped_sigkill"] = true
}

func applyPostgresProjectionCatchupFault(t *testing.T, ctx context.Context, db *sql.DB, cluster *testcluster.Cluster, all []jetstream.JetStream, stream jetstream.Stream, done <-chan error, proof map[string]any) {
	t.Helper()
	var pid, count int
	target := proof["count"].(int)
	for ctx.Err() == nil {
		select {
		case err := <-done:
			t.Fatalf("projection exited before fault admission: %v", err)
		default:
		}
		if err := db.QueryRowContext(ctx, `SELECT count(*) FROM wf_visibility`).Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count > 0 && count < target {
			err := db.QueryRowContext(ctx, `SELECT pid FROM pg_locks WHERE locktype='advisory' AND mode='ExclusiveLock' AND granted AND database=(SELECT oid FROM pg_database WHERE datname=current_database()) LIMIT 1`).Scan(&pid)
			if err == nil {
				break
			}
			if !errors.Is(err, sql.ErrNoRows) {
				t.Fatal(err)
			}
		}
		if count == target {
			t.Fatal("catch-up finished before fault admission")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if ctx.Err() != nil {
		t.Fatal("catch-up fault not admitted", ctx.Err())
	}
	info, err := stream.Info(ctx)
	if err != nil || info.Cluster == nil {
		t.Fatal("journal leader unavailable", err)
	}
	leader := -1
	for i, server := range cluster.Servers {
		if server.Name() == info.Cluster.Leader {
			leader = i
		}
	}
	if leader < 0 {
		t.Fatal("unknown journal leader", info.Cluster.Leader)
	}
	proof["catchup_admitted_rows"] = count
	proof["terminated_writer_backend_pid"] = pid
	proof["fault_started"] = time.Now().UTC()
	var terminated bool
	if err := db.QueryRowContext(ctx, `SELECT pg_terminate_backend($1)`, pid).Scan(&terminated); err != nil || !terminated {
		t.Fatalf("writer termination=%v err=%v", terminated, err)
	}
	proof["writer_backend_termination_confirmed"] = true
	oldID := cluster.Servers[leader].ID()
	cluster.KillNode(leader)
	if cluster.Servers[leader].Running() {
		t.Fatal("journal leader still running")
	}
	proof["journal_leader_stopped"] = time.Now().UTC()
	if err := cluster.RestartNode(leader); err != nil {
		t.Fatal(err)
	}
	proof["journal_leader_node"] = leader
	proof["journal_leader_old_server_id"] = oldID
	proof["journal_leader_new_server_id"] = cluster.Servers[leader].ID()
	proof["journal_leader_restarted"] = time.Now().UTC()
	// RestartNode closes/replaces the pinned connection on this node. Refresh
	// every JS handle before replacement construction or readiness queries.
	for i := range all {
		var err error
		all[i], err = jetstream.New(cluster.Clients[i])
		if err != nil {
			t.Fatal(err)
		}
		if all[i].Conn().IsClosed() {
			t.Fatal("refreshed client is closed", i)
		}
	}
	proof["pinned_clients_refreshed_after_restart"] = time.Now().UTC()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("faulted writer returned without an error")
		}
		proof["faulted_projection_error"] = err.Error()
		proof["faulted_projection_stopped"] = time.Now().UTC()
	case <-ctx.Done():
		t.Fatal("faulted writer did not exit", ctx.Err())
	}
	// Journal readiness alone does not certify the invocation/state/purge
	// sources used by a replacement full rebuild. Keep the original deadline.
	for _, name := range []string{"WF_INV", "WF_JRN", "KV_WF_STATE", "WF_PURGE"} {
		healed := false
		for ctx.Err() == nil {
			call, stop := context.WithTimeout(ctx, 2*time.Second)
			source, err := all[2].Stream(call, name)
			var info *jetstream.StreamInfo
			if err == nil {
				info, err = source.Info(call)
			}
			stop()
			if err == nil && info.Cluster != nil && info.Cluster.Leader != "" && len(info.Cluster.Replicas) == 2 {
				current := true
				for _, peer := range info.Cluster.Replicas {
					current = current && peer.Current && !peer.Offline
				}
				if current {
					proof[name+"_all_three_replicas_current"] = time.Now().UTC()
					healed = true
					break
				}
			}
			time.Sleep(20 * time.Millisecond)
		}
		if !healed {
			t.Fatal("projection source did not heal within original fixture context", name, ctx.Err())
		}
	}
	proof["journal_all_three_replicas_current"] = proof["WF_JRN_all_three_replicas_current"]
}

// Compare exposed row_data and actual indexed columns byte-for-byte. The random
// internal rebuild generation changes intentionally and is not a projected row.
func postgresProjectionState(t *testing.T, ctx context.Context, db *sql.DB, count int, path string) [32]byte {
	t.Helper()
	rows, err := db.QueryContext(ctx, `SELECT type,id,status,attributes::text,row_data::text FROM wf_visibility ORDER BY type,id`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	digest := sha256.New()
	writer := io.MultiWriter(file, digest)
	seen := map[uint64]bool{}
	total := 0
	for rows.Next() {
		var typ, id, status, attrs, data string
		if err := rows.Scan(&typ, &id, &status, &attrs, &data); err != nil {
			t.Fatal(err)
		}
		var row visibility.Row
		var attributes map[string]string
		if err := json.Unmarshal([]byte(data), &row); err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal([]byte(attrs), &attributes); err != nil {
			t.Fatal(err)
		}
		if typ != "view-scale" || id != fmt.Sprintf("job-%05d", total) || status != "completed" || row.Type != typ || row.ID != id || row.Status != status || row.SchemaVersion != 1 || row.InvSeq == 0 || seen[row.InvSeq] || row.LastIndex != 1 || row.JournalSeq == 0 || row.Started.IsZero() || row.Updated.Before(row.Started) || len(attributes) != 0 || len(row.Attributes) != 0 {
			t.Fatalf("incorrect projected row%d: %+v", total, row)
		}
		seen[row.InvSeq] = true
		line, err := json.Marshal([]string{typ, id, status, attrs, data})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := writer.Write(append(line, '\n')); err != nil {
			t.Fatal(err)
		}
		total++
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if total != count {
		t.Fatalf("SQL rows=%d want%d", total, count)
	}
	if err := file.Sync(); err != nil {
		t.Fatal(err)
	}
	var sum [32]byte
	copy(sum[:], digest.Sum(nil))
	return sum
}
