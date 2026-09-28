package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"js-wf/assignment"
	"js-wf/client"
	"js-wf/identity"
	"js-wf/integrity"
	"js-wf/journal"
	"js-wf/provision"
	"js-wf/wf"
	"js-wf/worker"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func TestPausedWorkerChild(t *testing.T) {
	if os.Getenv("WF_PAUSE_CHILD") != "1" {
		t.Skip("worker subprocess helper")
	}
	url := os.Getenv("WF_PAUSE_CHILD_URL")
	survivor := os.Getenv("WF_PAUSE_CHILD_SURVIVOR")
	marker := os.Getenv("WF_PAUSE_CHILD_MARKER")
	release := os.Getenv("WF_PAUSE_CHILD_RELEASE")
	result := os.Getenv("WF_PAUSE_CHILD_RESULT")
	if url == "" || survivor == "" || marker == "" || release == "" || result == "" {
		t.Fatal("missing pause-child configuration")
	}
	nc, err := nats.Connect(url, nats.NoReconnect(), nats.IgnoreDiscoveredServers())
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	handler := func(c *wf.Context, _ json.RawMessage) (json.RawMessage, error) {
		records, tail, err := journal.New(js).Read(c.Context(), "processpause", "stale-tail")
		if err != nil || len(records) != 1 || records[0].Kind != journal.Started {
			return nil, fmt.Errorf("initial journal: records=%+v err=%v", records, err)
		}
		if err := os.WriteFile(marker, []byte("ready"), 0644); err != nil {
			return nil, err
		}
		for {
			if _, err := os.Stat(release); err == nil {
				break
			}
			time.Sleep(10 * time.Millisecond)
		}
		fresh, err := nats.Connect(survivor, nats.NoReconnect(), nats.IgnoreDiscoveredServers())
		if err != nil {
			return nil, err
		}
		defer fresh.Close()
		freshJS, err := jetstream.New(fresh)
		if err != nil {
			return nil, err
		}
		attemptCtx, done := context.WithTimeout(context.Background(), 10*time.Second)
		defer done()
		_, appendErr := journal.New(freshJS).Append(attemptCtx, "processpause", "stale-tail", journal.Entry{Epoch: records[0].Epoch, Index: 1, Kind: journal.Completed, Payload: json.RawMessage(`{}`), WorkerID: "pause-old"}, tail)
		outcome := "success"
		if errors.Is(appendErr, journal.ErrStale) {
			outcome = "stale"
		} else if appendErr != nil {
			outcome = fmt.Sprintf("error: %v", appendErr)
		}
		if err := os.WriteFile(result, []byte(outcome), 0644); err != nil {
			return nil, err
		}
		return nil, appendErr
	}
	w, err := worker.New(context.Background(), js, "pause-old", map[string]worker.Handler{"processpause": handler})
	if err != nil {
		t.Fatal(err)
	}
	if err := w.RunKVAssignments(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestStoppedWorkerCannotAppendAfterLeaseExpiry(t *testing.T) {
	if os.Getenv("WF_PROCESS_PAUSE") == "" {
		t.Skip("set WF_PROCESS_PAUSE=1 for the 45-second process-pause proof")
	}
	all, cluster := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Second)
	defer cancel()
	const typ, id = "processpause", "stale-tail"
	partition := identity.Partition(typ, id, provision.Partitions)
	assignments, err := assignment.New(ctx, all[0])
	if err != nil {
		t.Fatal(err)
	}
	revision, err := assignments.Assign(ctx, partition, "pause-old", 0)
	if err != nil {
		t.Fatal(err)
	}
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	marker, release, result := filepath.Join(root, "ready"), filepath.Join(root, "release"), filepath.Join(root, "result")
	logFile, err := os.Create(filepath.Join(root, "child.log"))
	if err != nil {
		t.Fatal(err)
	}
	defer logFile.Close()
	cmd := exec.Command(executable, "-test.run=^TestPausedWorkerChild$")
	cmd.Env = append(os.Environ(), "WF_PAUSE_CHILD=1", "WF_PAUSE_CHILD_URL="+cluster.Servers[0].ClientURL(), "WF_PAUSE_CHILD_SURVIVOR="+cluster.Servers[1].ClientURL(), "WF_PAUSE_CHILD_MARKER="+marker, "WF_PAUSE_CHILD_RELEASE="+release, "WF_PAUSE_CHILD_RESULT="+result)
	cmd.Stdout, cmd.Stderr = logFile, logFile
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	childWaited := false
	defer func() {
		if !childWaited {
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
		}
	}()
	c := client.New(all[1])
	if _, err := c.Start(ctx, typ, id, []byte(`null`)); err != nil {
		t.Fatal(err)
	}
	for {
		if _, err := os.Stat(marker); err == nil {
			break
		}
		if ctx.Err() != nil {
			t.Fatal("child did not enter handler")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err := cmd.Process.Signal(syscall.SIGSTOP); err != nil {
		t.Fatal(err)
	}
	pausedAt := time.Now()
	if _, err := assignments.Assign(ctx, partition, "pause-new", revision); err != nil {
		t.Fatal(err)
	}
	newOwner, err := worker.New(ctx, all[2], "pause-new", map[string]worker.Handler{typ: func(*wf.Context, json.RawMessage) (json.RawMessage, error) {
		return json.RawMessage(`2`), nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	workerCtx, stop := context.WithCancel(ctx)
	newDone := make(chan error, 1)
	go func() { newDone <- newOwner.RunKVAssignments(workerCtx) }()
	defer func() {
		stop()
		if err := <-newDone; err != nil {
			t.Error(err)
		}
	}()
	value, err := c.Await(ctx, typ, id)
	if err != nil || string(value) != "2" {
		t.Fatalf("new owner result=%s err=%v", value, err)
	}
	if remaining := 45*time.Second - time.Since(pausedAt); remaining > 0 {
		select {
		case <-time.After(remaining):
		case <-ctx.Done():
			t.Fatal("process did not remain stopped for 45 seconds")
		}
	}
	if err := cmd.Process.Signal(syscall.SIGCONT); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(release, []byte("resume"), 0644); err != nil {
		t.Fatal(err)
	}
	for {
		outcome, err := os.ReadFile(result)
		if err == nil {
			if string(outcome) != "stale" {
				t.Fatalf("resumed worker append outcome=%q", outcome)
			}
			break
		}
		if ctx.Err() != nil {
			t.Fatalf("resumed worker did not report CAS outcome: %v", err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err := cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	if err := cmd.Wait(); err == nil {
		t.Fatal("paused worker process unexpectedly exited cleanly")
	}
	childWaited = true
	records, _, err := journal.New(all[0]).Read(ctx, typ, id)
	if err != nil || len(records) != 2 || records[0].Kind != journal.Started || records[1].Kind != journal.Completed || records[1].Epoch <= records[0].Epoch {
		t.Fatalf("journal after pause=%+v err=%v", records, err)
	}
	if report, err := integrity.Check(ctx, all[1]); err != nil || report.Terminal != 1 || report.Invocations != 1 {
		t.Fatalf("integrity=%+v err=%v", report, err)
	}
}
