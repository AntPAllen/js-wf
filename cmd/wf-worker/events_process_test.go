package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"js-wf/client"
	"js-wf/identity"
	"js-wf/provision"
	"js-wf/reconcile"
	"js-wf/testcluster"
)

func TestWorkerEventProcessHelper(t *testing.T) {
	if os.Getenv("WF_EVENT_PROCESS_HELPER") != "1" {
		t.Skip("subprocess helper")
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	args := []string{"-url", os.Getenv("WF_EVENT_PROCESS_URL"), "-id", "event-process", "-replicas", "1", "-timer-backend", "native", "-handler-plugin", os.Getenv("WF_EVENT_PROCESS_PLUGIN"), "-metrics-addr", "127.0.0.1:0", "-events-file", os.Getenv("WF_EVENT_PROCESS_FILE"), "-reconcile-interval", "100ms"}
	if err := run(ctx, args); err != nil {
		t.Fatal(err)
	}
}

func TestWorkerEventProcessRepairsLostWakeupAndFlushesOnSIGTERM(t *testing.T) {
	pluginPath := testWorkerPlugin(t)
	cluster, err := testcluster.Start(t.TempDir(), 1)
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Close()
	js, err := jetstream.New(cluster.Clients[0])
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	if err = provision.Ensure(ctx, js, 1); err != nil {
		t.Fatal(err)
	}
	// Leave the durable invocation without its second-write run wakeup. Only
	// the production start scanner in the child process can repair this cut.
	ack, err := js.Publish(ctx, identity.InvocationSubject("worker-smoke", "lost-wakeup"), []byte(`42`))
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "worker-events.jsonl")
	cmd := exec.Command(os.Args[0], "-test.run=^TestWorkerEventProcessHelper$", "-test.timeout=45s")
	cmd.Env = append(os.Environ(), "WF_EVENT_PROCESS_HELPER=1", "WF_EVENT_PROCESS_URL="+cluster.Servers[0].ClientURL(), "WF_EVENT_PROCESS_PLUGIN="+pluginPath, "WF_EVENT_PROCESS_FILE="+path)
	var output bytes.Buffer
	cmd.Stdout = &output
	cmd.Stderr = &output
	if err = cmd.Start(); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	awaitCtx, cancelAwait := context.WithCancel(ctx)
	defer cancelAwait()
	go func() { err := cmd.Wait(); cancelAwait(); done <- err }()
	waited := false
	defer func() {
		if !waited {
			_ = cmd.Process.Kill()
			<-done
		}
		if t.Failed() {
			t.Log(output.String())
		}
	}()
	result, err := client.New(js).Await(awaitCtx, "worker-smoke", "lost-wakeup")
	if err != nil || string(result) != "42" {
		t.Fatalf("repaired result=%s err=%v", result, err)
	}
	if err = cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	select {
	case err = <-done:
		waited = true
		if err != nil {
			t.Fatalf("graceful worker exit: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("worker did not stop")
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	records := bytes.Split(bytes.TrimSpace(raw), []byte("\n"))
	matched := false
	for i, line := range records {
		var record struct {
			Version  int
			Worker   string `json:"worker_id"`
			PID      int
			Sequence uint64
			Kind     string
			Event    json.RawMessage
		}
		if err = json.Unmarshal(line, &record); err != nil {
			t.Fatal(err)
		}
		if record.Version != 1 || record.Worker != "event-process" || record.PID != cmd.Process.Pid || record.Sequence != uint64(i+1) {
			t.Fatalf("invalid process envelope=%s", line)
		}
		if record.Kind == "repair" {
			var event reconcile.RepairEvent
			if err = json.Unmarshal(record.Event, &event); err != nil {
				t.Fatal(err)
			}
			if event.Kind == "start" && event.ID == "lost-wakeup" && event.Type == "worker-smoke" && event.SourceSequence == ack.Sequence && event.InvocationSequence == ack.Sequence && event.Reason == "missing_journal" && event.Outcome == "acknowledged" {
				matched = true
			}
		}
	}
	if !matched {
		t.Fatalf("missing actual repair decision in %s", raw)
	}
	t.Logf("child pid=%d result=%s events=%d JSONL=%s", cmd.Process.Pid, result, len(records), raw)
	if root := os.Getenv("WF_WORKER_EVENT_ARTIFACT_ROOT"); root != "" {
		if err = os.MkdirAll(root, 0755); err != nil {
			t.Fatal(err)
		}
		if err = os.WriteFile(filepath.Join(root, "worker-events.jsonl"), raw, 0644); err != nil {
			t.Fatal(err)
		}
		if err = os.WriteFile(filepath.Join(root, "result.txt"), []byte(fmt.Sprintf("pid=%d invocation_sequence=%d result=%s graceful_exit=true\n", cmd.Process.Pid, ack.Sequence, result)), 0644); err != nil {
			t.Fatal(err)
		}
	}
}
