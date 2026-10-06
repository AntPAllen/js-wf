package main

import (
	"bytes"
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
	"testing"

	"github.com/nats-io/nats.go/jetstream"
)

var operatorStandaloneOnce sync.Once
var operatorStandalonePath, operatorStandaloneDir, operatorStandaloneBuildInfo string
var operatorStandaloneErr error

type operatorCommandProcessError struct {
	stderr   string
	exitCode int
}

func (e *operatorCommandProcessError) Error() string {
	return fmt.Sprintf("wf exit=%d: %s", e.exitCode, e.stderr)
}

func operatorFileSHA(path string) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer file.Close()
	hash := sha256.New()
	if _, err = io.Copy(hash, file); err != nil {
		return "", err
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}

func operatorStandalone(t *testing.T) string {
	t.Helper()
	operatorStandaloneOnce.Do(func() {
		operatorStandaloneDir, operatorStandaloneErr = os.MkdirTemp(os.Getenv("WF_OPERATOR_TEST_ROOT"), "js-wf-standalone-")
		if operatorStandaloneErr != nil {
			return
		}
		operatorStandalonePath = filepath.Join(operatorStandaloneDir, "wf")
		args := []string{"build", "-buildvcs=true"}
		if replayPluginRace {
			args = append(args, "-race")
		}
		args = append(args, "-o", operatorStandalonePath, ".")
		if output, err := exec.Command("go", args...).CombinedOutput(); err != nil {
			operatorStandaloneErr = fmt.Errorf("build standalone wf: %w: %s", err, output)
			return
		}
		info, err := exec.Command("go", "version", "-m", operatorStandalonePath).Output()
		operatorStandaloneErr = err
		operatorStandaloneBuildInfo = string(info)
	})
	if operatorStandaloneErr != nil {
		t.Fatal(operatorStandaloneErr)
	}
	if !strings.Contains(operatorStandaloneBuildInfo, "vcs.modified=false") ||
		!strings.Contains(operatorStandaloneBuildInfo, "\tpath\tjs-wf/cmd/wf\n") {
		t.Fatalf("standalone wf must be built from clean Git source: %s", operatorStandaloneBuildInfo)
	}
	return operatorStandalonePath
}

func TestOperatorStandaloneCommands(t *testing.T) {
	if os.Getenv("WF_OPERATOR_STANDALONE") != "1" {
		t.Skip("set WF_OPERATOR_STANDALONE=1 for actual compiled CLI controls")
	}
	runOperatorStandaloneCommands(t, "")
}

func TestOperatorStandaloneCommandsInJetStreamDomain(t *testing.T) {
	if os.Getenv("WF_OPERATOR_STANDALONE") != "1" {
		t.Skip("set WF_OPERATOR_STANDALONE=1 for actual compiled CLI controls")
	}
	runOperatorStandaloneCommands(t, "WFOPS")
}

func runOperatorStandaloneCommands(t *testing.T, domain string) {
	binary := operatorStandalone(t)
	expectedSHA, err := operatorFileSHA(binary)
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	invoke := func(ctx context.Context, args []string, output io.Writer, options ...jetstream.JetStreamOpt) error {
		t.Helper()
		if len(options) != 0 {
			t.Fatal("standalone execution cannot inherit parent client-trace options")
		}
		root := operatorTempDir(t)
		stdout, err := os.Create(filepath.Join(root, "stdout"))
		if err != nil {
			t.Fatal(err)
		}
		defer stdout.Close()
		stderr, err := os.Create(filepath.Join(root, "stderr"))
		if err != nil {
			t.Fatal(err)
		}
		defer stderr.Close()
		process := exec.CommandContext(ctx, binary, args...)
		process.Stdout, process.Stderr = stdout, stderr
		if err := process.Start(); err != nil {
			t.Fatal(err)
		}
		joined := false
		defer func() {
			if !joined {
				_ = process.Process.Kill()
				_ = process.Wait()
			}
		}()
		proc := fmt.Sprintf("/proc/%d", process.Process.Pid)
		stat, err := os.ReadFile(filepath.Join(proc, "stat"))
		if err != nil {
			t.Fatal(err)
		}
		cmdline, err := os.ReadFile(filepath.Join(proc, "cmdline"))
		if err != nil {
			t.Fatal(err)
		}
		argv := strings.Split(strings.TrimRight(string(cmdline), "\x00"), "\x00")
		if strings.Join(argv, "\x00") != strings.Join(append([]string{binary}, args...), "\x00") {
			t.Fatalf("actual standalone argv=%q expected=%q", argv, args)
		}
		actualSHA, err := operatorFileSHA(filepath.Join(proc, "exe"))
		if err != nil || actualSHA != expectedSHA {
			t.Fatalf("actual standalone executable: %s err=%v", actualSHA, err)
		}
		waitErr := process.Wait()
		joined = true
		data, err := os.ReadFile(filepath.Join(root, "stdout"))
		if err != nil {
			t.Fatal(err)
		}
		diagnostic, err := os.ReadFile(filepath.Join(root, "stderr"))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := io.Copy(output, bytes.NewReader(data)); err != nil {
			t.Fatal(err)
		}
		record := map[string]any{"pid": process.Process.Pid, "stat": string(stat), "argv": argv,
			"exe_sha256": actualSHA, "build_info": operatorStandaloneBuildInfo,
			"domain": domain, "exit_code": process.ProcessState.ExitCode(), "stdout": string(data), "stderr": string(diagnostic)}
		encoded, err := json.MarshalIndent(record, "", "  ")
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(root, "standalone.process.json"), append(encoded, '\n'), 0600); err != nil {
			t.Fatal(err)
		}
		count++
		t.Logf("operator standalone process domain=%q pid=%d exit=%d", domain, process.Process.Pid, process.ProcessState.ExitCode())
		if waitErr != nil {
			return &operatorCommandProcessError{stderr: string(diagnostic), exitCode: process.ProcessState.ExitCode()}
		}
		return nil
	}
	runOperatorCommandsWithInvoker(t, domain, invoke)
	t.Logf("operator standalone commands domain=%q processes=%d exe_sha256=%s", domain, count, expectedSHA)
}
