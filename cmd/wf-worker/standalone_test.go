package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

var workerStandaloneOnce sync.Once
var workerStandaloneDir, workerStandalonePath, workerStandaloneInfo string
var workerStandaloneErr error

func workerExecutableSHA(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

func standaloneWorker(t *testing.T) string {
	t.Helper()
	workerStandaloneOnce.Do(func() {
		workerStandaloneDir, workerStandaloneErr = os.MkdirTemp(os.Getenv("WF_WORKER_TEST_ROOT"), "wf-worker-standalone-")
		if workerStandaloneErr != nil {
			return
		}
		workerStandalonePath = filepath.Join(workerStandaloneDir, "wf-worker")
		args := []string{"build", "-buildvcs=true"}
		if workerPluginRace {
			args = append(args, "-race")
		}
		args = append(args, "-o", workerStandalonePath, ".")
		if output, err := exec.Command("go", args...).CombinedOutput(); err != nil {
			workerStandaloneErr = fmt.Errorf("build wf-worker: %w: %s", err, output)
			return
		}
		info, err := exec.Command("go", "version", "-m", workerStandalonePath).Output()
		workerStandaloneErr, workerStandaloneInfo = err, string(info)
	})
	if workerStandaloneErr != nil {
		t.Fatal(workerStandaloneErr)
	}
	if !strings.Contains(workerStandaloneInfo, "vcs.modified=false") || !strings.Contains(workerStandaloneInfo, "\tpath\tjs-wf/cmd/wf-worker\n") {
		t.Fatalf("require clean standalone worker source: %s", workerStandaloneInfo)
	}
	return workerStandalonePath
}

// The case's original context controls a real SIGTERM. A stuck child is killed
// only after the same ten-second shutdown bound used by the shared fixture.
func standaloneWorkerInvoker(t *testing.T, domain string) func(context.Context, []string) error {
	t.Helper()
	binary := standaloneWorker(t)
	expected, err := workerExecutableSHA(binary)
	if err != nil {
		t.Fatal(err)
	}
	root, err := os.MkdirTemp(os.Getenv("WF_WORKER_TEST_ROOT"), "wf-worker-process-")
	if err != nil {
		t.Fatal(err)
	}
	if os.Getenv("WF_WORKER_TEST_ROOT") == "" {
		t.Cleanup(func() { _ = os.RemoveAll(root) })
	}
	return func(ctx context.Context, args []string) error {
		stdout, err := os.Create(filepath.Join(root, "stdout"))
		if err != nil {
			return err
		}
		defer stdout.Close()
		stderr, err := os.Create(filepath.Join(root, "stderr"))
		if err != nil {
			return err
		}
		defer stderr.Close()
		cmd := exec.Command(binary, args...)
		cmd.Stdout, cmd.Stderr = stdout, stderr
		if err := cmd.Start(); err != nil {
			return err
		}
		joined := false
		defer func() {
			if !joined {
				_ = cmd.Process.Kill()
				_ = cmd.Wait()
			}
		}()
		proc := filepath.Join("/proc", fmt.Sprint(cmd.Process.Pid))
		stat, err := os.ReadFile(filepath.Join(proc, "stat"))
		if err != nil {
			return err
		}
		argv, err := os.ReadFile(filepath.Join(proc, "cmdline"))
		if err != nil {
			return err
		}
		actual, err := workerExecutableSHA(filepath.Join(proc, "exe"))
		if err != nil {
			return err
		}
		if actual != expected || string(argv) != strings.Join(append([]string{binary}, args...), "\x00")+"\x00" {
			return fmt.Errorf("actual worker executable/argv mismatch")
		}
		record := map[string]any{"pid": cmd.Process.Pid, "stat": string(stat), "argv": append([]string{binary}, args...), "exe": binary, "exe_sha256": actual, "build_info": workerStandaloneInfo, "domain": domain, "test": t.Name()}
		persist := func() error {
			data, err := json.MarshalIndent(record, "", "  ")
			if err != nil {
				return err
			}
			return os.WriteFile(filepath.Join(root, "standalone.process.json"), append(data, '\n'), 0600)
		}
		if err := persist(); err != nil {
			return err
		}
		done := make(chan error, 1)
		go func() { done <- cmd.Wait() }()
		var waitErr error
		select {
		case waitErr = <-done:
		case <-ctx.Done():
			record["signal"] = "SIGTERM"
			if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
				_ = cmd.Process.Kill()
				<-done
				joined = true
				return err
			}
			select {
			case waitErr = <-done:
			case <-time.After(10 * time.Second):
				_ = cmd.Process.Kill()
				<-done
				joined = true
				return fmt.Errorf("standalone worker did not join after SIGTERM")
			}
		}
		joined = true
		record["exit_code"] = cmd.ProcessState.ExitCode()
		for _, name := range []string{"stdout", "stderr"} {
			hash, err := workerExecutableSHA(filepath.Join(root, name))
			if err != nil {
				return err
			}
			record[name+"_sha256"] = hash
		}
		if err := persist(); err != nil {
			return err
		}
		t.Logf("worker standalone process domain=%q pid=%d signal=%v exit=%d exe_sha256=%s", domain, cmd.Process.Pid, record["signal"], cmd.ProcessState.ExitCode(), actual)
		return waitErr
	}
}

func runStandaloneWorkerCommands(t *testing.T, domain string) {
	if os.Getenv("WF_WORKER_STANDALONE") != "1" {
		t.Skip("set WF_WORKER_STANDALONE=1 for built worker process controls")
	}
	plugin := testWorkerPlugin(t)
	for _, mode := range []string{"static", "kv", "auto"} {
		t.Run(mode, func(t *testing.T) {
			runWorkerSmokeWithInvoker(t, plugin, mode, domain, standaloneWorkerInvoker(t, domain))
		})
	}
}

func TestWorkerStandaloneCommands(t *testing.T) { runStandaloneWorkerCommands(t, "") }
func TestWorkerStandaloneCommandsInJetStreamDomain(t *testing.T) {
	runStandaloneWorkerCommands(t, "WFWORKER")
}
func TestWorkerStandaloneStartsAfterServerRestart(t *testing.T) {
	if os.Getenv("WF_WORKER_STANDALONE") != "1" {
		t.Skip("set WF_WORKER_STANDALONE=1")
	}
	runWorkerRestartWithInvoker(t, "", standaloneWorkerInvoker(t, ""))
}
func TestWorkerStandaloneStartsAfterServerRestartInJetStreamDomain(t *testing.T) {
	if os.Getenv("WF_WORKER_STANDALONE") != "1" {
		t.Skip("set WF_WORKER_STANDALONE=1")
	}
	runWorkerRestartWithInvoker(t, "WFWORKER", standaloneWorkerInvoker(t, "WFWORKER"))
}
