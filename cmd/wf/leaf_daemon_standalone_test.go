package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"js-wf/identity"
	"js-wf/provision"
	"js-wf/retention"
	"js-wf/testcluster"
)

// Keep the helper-based default/domain tests: this separately exercises the
// actual packaged CLI, with a wire barrier rather than an injected ClientTrace.
func TestOperatorStandaloneDaemonSignalsThroughLeaf(t *testing.T) {
	if os.Getenv("WF_OPERATOR_STANDALONE") != "1" {
		t.Skip("set WF_OPERATOR_STANDALONE=1")
	}
	binary := operatorStandalone(t)
	expectedSHA, err := operatorFileSHA(binary)
	if err != nil {
		t.Fatal(err)
	}
	root := operatorTempDir(t)
	cluster, err := testcluster.StartWithLeafDomain(root, 3, "WFOPS")
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Close()
	js, err := jetstream.NewWithDomain(cluster.Clients[0], "WFOPS")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	ready, stop := context.WithTimeout(ctx, 30*time.Second)
	for ready.Err() == nil {
		call, finish := context.WithTimeout(ready, 4*time.Second)
		err = provision.Ensure(call, js, 3)
		finish()
		if err == nil {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	stop()
	if err != nil {
		t.Fatal(err)
	}
	endpoint, finishLeaf := operatorLeafEndpoint(t, ctx, cluster, "WFOPS")
	defer finishLeaf()
	for _, command := range []string{"project", "tombstone-loop"} {
		for _, stage := range []string{"startup", "running"} {
			t.Run(command+"/"+stage, func(t *testing.T) { runPackagedLeafDaemon(t, ctx, js, binary, expectedSHA, endpoint, command, stage) })
		}
	}
	if err = js.DeleteStream(ctx, "WF_INV"); err != nil {
		t.Fatal(err)
	}
	t.Run("project/fatal", func(t *testing.T) {
		runPackagedLeafDaemon(t, ctx, js, binary, expectedSHA, endpoint, "project", "fatal")
	})
}

func runPackagedLeafDaemon(t *testing.T, ctx context.Context, js jetstream.JetStream, binary, expectedSHA, endpoint, command, stage string) {
	t.Helper()
	root := operatorTempDir(t)
	persist := func(name string, value any) {
		data, err := json.MarshalIndent(value, "", "  ")
		if err != nil {
			t.Error(err)
			return
		}
		if err = os.WriteFile(filepath.Join(root, name), append(data, '\n'), 0600); err != nil {
			t.Error(err)
		}
	}
	proxy, err := testcluster.NewClientProxy(endpoint)
	if err != nil {
		t.Fatal(err)
	}
	defer proxy.Close()
	if err = proxy.EnableTrafficTrace(16 << 20); err != nil {
		t.Fatal(err)
	}
	if stage == "startup" {
		if err = proxy.HoldFirstAPI("$JS.WFOPS.API."); err != nil {
			t.Fatal(err)
		}
	}
	args := []string{"-url", proxy.URL(), "-domain", "WFOPS", "-interval", "100ms", command}
	output, err := os.Create(filepath.Join(root, "stdout-stderr.log"))
	if err != nil {
		t.Fatal(err)
	}
	defer output.Close()
	child := exec.CommandContext(ctx, binary, args...)
	child.Stdout = output
	child.Stderr = output
	if err = child.Start(); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- child.Wait() }()
	joined := false
	proof := map[string]any{"test": t.Name(), "command": command, "stage": stage, "domain": "WFOPS", "leaf_endpoint": endpoint, "operator_args": args, "pid": child.Process.Pid, "exe": binary, "build_info": operatorStandaloneBuildInfo, "root": root}
	defer func() {
		if !joined {
			_ = child.Process.Kill()
			<-done
		}
		proxy.Close()
		proof["reaped"] = child.ProcessState != nil
		if child.ProcessState != nil {
			proof["exit_code"] = child.ProcessState.ExitCode()
		}
		proof["pending_api"] = proxy.PendingAPI()
		proof["scenario_passed"] = !t.Failed()
		persist("daemon.process.json", proof)
		persist("traffic.json", proxy.TrafficTrace())
		persist("proxy-final.json", proxy.Stats())
	}()
	proc := filepath.Join("/proc", fmt.Sprint(child.Process.Pid))
	stat, err := os.ReadFile(filepath.Join(proc, "stat"))
	if err != nil {
		t.Fatal(err)
	}
	argv, err := os.ReadFile(filepath.Join(proc, "cmdline"))
	if err != nil {
		t.Fatal(err)
	}
	hash, err := operatorFileSHA(filepath.Join(proc, "exe"))
	if err != nil || hash != expectedSHA {
		t.Fatalf("actual packaged exe hash=%s want=%s err=%v", hash, expectedSHA, err)
	}
	if string(argv) != strings.Join(child.Args, "\x00")+"\x00" {
		t.Fatal("actual packaged argv mismatch")
	}
	proof["stat"] = string(stat)
	proof["argv"] = child.Args
	proof["exe_sha256"] = hash
	if stage != "fatal" {
		var state jetstream.KeyValue
		var key string
		if stage == "running" && command == "tombstone-loop" {
			state, err = js.KeyValue(ctx, "WF_STATE")
			if err != nil {
				t.Fatal(err)
			}
			key = identity.Key("daemon", "expired")
			data, err := json.Marshal(retention.Tombstone{Tombstone: true, InvSeq: 1, PurgedAt: time.Now().Add(-2 * time.Hour), ExpiresAt: time.Now().Add(-time.Hour)})
			if err != nil {
				t.Fatal(err)
			}
			if _, err = state.Put(ctx, key, data); err != nil {
				t.Fatal(err)
			}
		}
		for {
			ready := false
			if stage == "startup" {
				pending := proxy.PendingAPI()
				ready = pending != nil
				if ready && (pending.Subject == "" || !strings.HasPrefix(pending.Subject, "$JS.WFOPS.API.") || pending.Disposition != "held" || pending.ForwardedBytes != 0) {
					t.Fatalf("startup barrier %+v", pending)
				}
			} else if command == "project" {
				consumer, e := js.Consumer(ctx, "WF_JRN", "WF_VIEW")
				if e == nil {
					info, e := consumer.Info(ctx)
					ready = e == nil && info.NumWaiting > 0
				}
			} else {
				_, e := state.Get(ctx, key)
				ready = errors.Is(e, jetstream.ErrKeyNotFound)
				if e != nil && !ready {
					t.Fatal(e)
				}
			}
			if ready {
				break
			}
			select {
			case e := <-done:
				joined = true
				t.Fatalf("daemon exited before %s readiness: %v", stage, e)
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			default:
			}
			time.Sleep(10 * time.Millisecond)
		}
		signal := syscall.SIGTERM
		if stage == "running" {
			signal = syscall.SIGINT
		}
		proof["signal"] = signal.String()
		if err = child.Process.Signal(signal); err != nil {
			t.Fatal(err)
		}
	}
	select {
	case err = <-done:
		joined = true
	case <-ctx.Done():
		t.Fatal("packaged daemon did not exit", ctx.Err())
	}
	proxy.Close()
	log, readErr := os.ReadFile(filepath.Join(root, "stdout-stderr.log"))
	if readErr != nil {
		t.Fatal(readErr)
	}
	if stage == "fatal" {
		if err == nil || child.ProcessState.ExitCode() != 1 || !strings.Contains(string(log), "stream not found") {
			t.Fatalf("fatal source exit=%v log=%s", err, log)
		}
	} else if err != nil {
		t.Fatalf("graceful daemon exit=%v log=%s", err, log)
	}
	stats, trace := proxy.Stats(), proxy.TrafficTrace()
	if stats.AcceptedConnections != 1 || stats.UpstreamDialFailures != 0 || stats.Active != 0 || stats.BufferedBytes != 0 || stats.BufferOverflows != 0 || len(trace.Connections) != 1 || trace.Truncated {
		t.Fatalf("incomplete daemon wire stats=%+v trace=%+v", stats, trace)
	}
	if stage == "startup" {
		pending := proxy.PendingAPI()
		if pending == nil || pending.Disposition != "cancelled" || pending.ForwardedBytes != 0 {
			t.Fatalf("startup held packet after exit %+v", pending)
		}
	}
	t.Logf("packaged leaf daemon: command=%s stage=%s pid=%d exit=%d root=%s client_bytes=%d server_bytes=%d", command, stage, child.Process.Pid, child.ProcessState.ExitCode(), root, stats.ClientToServer, stats.ServerToClient)
}
