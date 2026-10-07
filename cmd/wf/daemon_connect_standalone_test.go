package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

// The protocol fixture intentionally withholds INFO or the CONNECT/PING reply.
// This is a connection boundary test, not a JetStream or authentication test.
func TestOperatorStandaloneConnectionSignals(t *testing.T) {
	if os.Getenv("WF_OPERATOR_STANDALONE") != "1" {
		t.Skip("set WF_OPERATOR_STANDALONE=1 for actual compiled CLI controls")
	}
	binary := operatorStandalone(t)
	for _, command := range []string{"project", "tombstone-loop"} {
		for _, stage := range []string{"INFO", "PONG"} {
			for _, sig := range []syscall.Signal{syscall.SIGTERM, syscall.SIGINT} {
				t.Run(fmt.Sprintf("%s/%s/%s", command, stage, sig), func(t *testing.T) {
					runStandaloneConnectionSignal(t, binary, command, stage, sig)
				})
			}
		}
	}
}

func runStandaloneConnectionSignal(t *testing.T, binary, command, stage string, sig syscall.Signal) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	root := operatorTempDir(t)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	accepted := make(chan net.Conn, 1)
	go func() {
		conn, err := listener.Accept()
		if err == nil {
			accepted <- conn
		}
	}()
	args := []string{"-url", "nats://" + listener.Addr().String(), command}
	output, err := os.Create(filepath.Join(root, "child.log"))
	if err != nil {
		t.Fatal(err)
	}
	defer output.Close()
	child := exec.CommandContext(ctx, binary, args...)
	child.Env = append(os.Environ(), "WF_POSTGRES_DSN=")
	child.Stdout, child.Stderr = output, output
	if err = child.Start(); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- child.Wait() }()
	joined := false
	proof := map[string]any{"command": command, "stage": stage, "signal": sig.String(), "pid": child.Process.Pid, "args": args, "binary": binary, "build_info": operatorStandaloneBuildInfo, "fixture": "controlled NATS protocol; no JetStream server"}
	defer func() {
		if !joined {
			_ = child.Process.Kill()
			<-done
		}
		proof["exit_code"] = child.ProcessState.ExitCode()
		proof["scenario_passed"] = !t.Failed()
		data, err := json.MarshalIndent(proof, "", "  ")
		if err == nil {
			err = os.WriteFile(filepath.Join(root, "connection.process.json"), append(data, '\n'), 0600)
		}
		if err != nil {
			t.Error(err)
		}
	}()
	var conn net.Conn
	select {
	case conn = <-accepted:
	case <-ctx.Done():
		t.Fatal("child did not dial controlled handshake")
	}
	defer conn.Close()
	if err = conn.SetDeadline(time.Now().Add(3 * time.Second)); err != nil {
		t.Fatal(err)
	}
	reader := bufio.NewReader(conn)
	var clientWire, serverWire string
	if stage == "PONG" {
		serverWire = "INFO {\"server_id\":\"CONNECTION_FIXTURE\",\"version\":\"2.15.0\",\"proto\":1,\"max_payload\":1048576}\r\n"
		if _, err = io.WriteString(conn, serverWire); err != nil {
			t.Fatal(err)
		}
		for _, prefix := range []string{"CONNECT ", "PING\r\n"} {
			line, err := reader.ReadString('\n')
			if err != nil || !strings.HasPrefix(line, prefix) {
				t.Fatalf("handshake admission line=%q err=%v", line, err)
			}
			clientWire += line
		}
	}
	proc := filepath.Join("/proc", fmt.Sprint(child.Process.Pid))
	stat, err := os.ReadFile(filepath.Join(proc, "stat"))
	if err != nil {
		t.Fatal(err)
	}
	argv, err := os.ReadFile(filepath.Join(proc, "cmdline"))
	if err != nil || string(argv) != strings.Join(child.Args, "\x00")+"\x00" {
		t.Fatal("actual child argv", err)
	}
	hash, err := operatorFileSHA(filepath.Join(proc, "exe"))
	expected, expectedErr := operatorFileSHA(binary)
	if err != nil || expectedErr != nil || hash != expected {
		t.Fatal("actual child executable", err, expectedErr)
	}
	proof["stat"], proof["argv"], proof["exe_sha256"] = string(stat), child.Args, hash
	proof["admitted_at"] = time.Now().UTC()
	sentAt := time.Now()
	proof["signal_sent_at"] = sentAt.UTC()
	if err = child.Process.Signal(sig); err != nil {
		t.Fatal(err)
	}
	select {
	case err = <-done:
		joined = true
	case <-time.After(3 * time.Second):
		t.Fatal("signal did not interrupt the held handshake within3s")
	}
	proof["joined_at"] = time.Now().UTC()
	if err != nil || child.ProcessState.ExitCode() != 0 {
		t.Fatalf("controlled %s shutdown: %v", stage, err)
	}
	tail, err := io.ReadAll(reader)
	if err != nil {
		t.Fatal("child did not close its handshake socket", err)
	}
	clientWire += string(tail)
	proof["client_wire"], proof["server_wire"] = clientWire, serverWire
	if len(tail) != 0 || (stage == "INFO" && clientWire != "") {
		t.Fatalf("unexpected traffic while startup held: %q", clientWire)
	}
	t.Logf("operator connection signal command=%s stage=%s signal=%s exit=0 elapsed=%s pid=%d", command, stage, sig, time.Since(sentAt), child.Process.Pid)
}
