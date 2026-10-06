package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
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

	"github.com/nats-io/nats.go/jetstream"
	"js-wf/identity"
	"js-wf/provision"
	"js-wf/retention"
	"js-wf/testcluster"
)

func TestOperatorDaemonProcessHelper(t *testing.T) {
	encoded := os.Getenv("WF_OPERATOR_DAEMON_ARGS")
	if encoded == "" {
		t.Skip("subprocess helper")
	}
	var args []string
	if err := json.Unmarshal([]byte(encoded), &args); err != nil {
		t.Fatal(err)
	}
	var once sync.Once
	trace := &jetstream.ClientTrace{RequestSent: func(subject string, _ []byte) {
		once.Do(func() {
			fmt.Printf("operator daemon first request=%s\n", subject)
			if err := os.WriteFile(os.Getenv("WF_OPERATOR_DAEMON_READY"), []byte(subject), 0600); err != nil {
				panic(err)
			}
			if os.Getenv("WF_OPERATOR_DAEMON_STAGE") == "startup" {
				time.Sleep(2 * time.Second)
			}
		})
	}}
	if err := runWithJetStreamOptions(args, os.Stdout, jetstream.WithClientTrace(trace)); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	os.Exit(0)
}

func TestOperatorDaemonSignals(t *testing.T)                  { runOperatorDaemonSignals(t, "") }
func TestOperatorDaemonSignalsInJetStreamDomain(t *testing.T) { runOperatorDaemonSignals(t, "WFOPS") }

func runOperatorDaemonSignals(t *testing.T, domain string) {
	count := 1
	if domain != "" {
		count = 3
	}
	root := operatorTempDir(t)
	var cluster *testcluster.Cluster
	var err error
	if domain == "" {
		cluster, err = testcluster.Start(root, count)
	} else {
		cluster, err = testcluster.StartWithDomain(root, count, domain)
	}
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Close()
	var js jetstream.JetStream
	if domain == "" {
		js, err = jetstream.New(cluster.Clients[0])
	} else {
		js, err = jetstream.NewWithDomain(cluster.Clients[0], domain)
	}
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	ready, finish := context.WithTimeout(ctx, 30*time.Second)
	for ready.Err() == nil {
		call, stop := context.WithTimeout(ready, 4*time.Second)
		err = provision.Ensure(call, js, count)
		stop()
		if err == nil {
			break
		}
		select {
		case <-ready.Done():
		case <-time.After(50 * time.Millisecond):
		}
	}
	finish()
	if err != nil {
		t.Fatal(err)
	}
	if domain != "" {
		for node, nc := range cluster.Clients {
			peer, err := jetstream.NewWithDomain(nc, domain)
			if err != nil {
				t.Fatal(err)
			}
			info, err := peer.AccountInfo(ctx)
			if err != nil || info.Domain != domain || nc.ConnectedDomain() != domain {
				t.Fatalf("daemon admission node=%d info=%+v err=%v", node, info, err)
			}
			t.Logf("operator daemon domain admitted node=%d domain=%s server_id=%s", node, domain, nc.ConnectedServerId())
		}
	}
	for _, command := range []string{"project", "tombstone-loop"} {
		for _, stage := range []string{"startup", "running"} {
			t.Run(command+"/"+stage, func(t *testing.T) {
				args := []string{"-url", cluster.Servers[0].ClientURL(), "-interval", "100ms"}
				if domain != "" {
					args = append(args, "-domain", domain)
				}
				args = append(args, command)
				encoded, err := json.Marshal(args)
				if err != nil {
					t.Fatal(err)
				}
				name := strings.ReplaceAll(t.Name(), "/", "_")
				marker := filepath.Join(root, name+".ready")
				logPath := filepath.Join(root, name+".log")
				log, err := os.Create(logPath)
				if err != nil {
					t.Fatal(err)
				}
				defer log.Close()
				process := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestOperatorDaemonProcessHelper$")
				process.Env = append(os.Environ(), "WF_OPERATOR_DAEMON_ARGS="+string(encoded), "WF_OPERATOR_DAEMON_READY="+marker, "WF_OPERATOR_DAEMON_STAGE="+stage)
				process.Stdout = log
				process.Stderr = log
				if err := process.Start(); err != nil {
					t.Fatal(err)
				}
				done := make(chan error, 1)
				go func() { done <- process.Wait() }()
				joined := false
				defer func() {
					if !joined {
						_ = process.Process.Kill()
						<-done
					}
				}()
				for ctx.Err() == nil {
					if _, err := os.Stat(marker); err == nil {
						break
					}
					select {
					case err := <-done:
						joined = true
						t.Fatalf("daemon exited before ready: %v", err)
					default:
					}
					time.Sleep(10 * time.Millisecond)
				}
				if ctx.Err() != nil {
					t.Fatal(ctx.Err())
				}
				proc := filepath.Join("/proc", fmt.Sprint(process.Process.Pid))
				exe, err := os.Open(filepath.Join(proc, "exe"))
				if err != nil {
					t.Fatal(err)
				}
				digest := sha256.New()
				_, err = io.Copy(digest, exe)
				_ = exe.Close()
				if err != nil {
					t.Fatal(err)
				}
				stat, err := os.ReadFile(filepath.Join(proc, "stat"))
				if err != nil {
					t.Fatal(err)
				}
				argv, err := os.ReadFile(filepath.Join(proc, "cmdline"))
				if err != nil {
					t.Fatal(err)
				}
				record := map[string]any{"pid": process.Process.Pid, "stat": string(stat), "exe_sha256": hex.EncodeToString(digest.Sum(nil)), "argv": strings.Split(strings.TrimRight(string(argv), "\x00"), "\x00"), "operator_args": args, "domain": domain, "stage": stage, "command": command}
				data, err := json.MarshalIndent(record, "", "  ")
				if err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(root, name+".process.json"), data, 0600); err != nil {
					t.Fatal(err)
				}
				subject, err := os.ReadFile(marker)
				if err != nil {
					t.Fatal(err)
				}
				prefix := "$JS.API."
				if domain != "" {
					prefix = "$JS." + domain + ".API."
				}
				if !strings.HasPrefix(string(subject), prefix) {
					t.Fatalf("daemon selected wrong API: %s", subject)
				}
				if stage == "running" {
					if command == "project" {
						for ctx.Err() == nil {
							consumer, err := js.Consumer(ctx, "WF_JRN", "WF_VIEW")
							if err == nil {
								info, err := consumer.Info(ctx)
								if err == nil && info.NumWaiting > 0 {
									break
								}
							}
							time.Sleep(10 * time.Millisecond)
						}
					} else {
						state, err := js.KeyValue(ctx, "WF_STATE")
						if err != nil {
							t.Fatal(err)
						}
						tombstone, err := json.Marshal(retention.Tombstone{Tombstone: true, InvSeq: 1, PurgedAt: time.Now().Add(-2 * time.Hour), ExpiresAt: time.Now().Add(-time.Hour)})
						if err != nil {
							t.Fatal(err)
						}
						key := identity.Key("daemon", "expired")
						if _, err := state.Put(ctx, key, tombstone); err != nil {
							t.Fatal(err)
						}
						for ctx.Err() == nil {
							_, err := state.Get(ctx, key)
							if errors.Is(err, jetstream.ErrKeyNotFound) {
								break
							}
							if err != nil {
								t.Fatal(err)
							}
							time.Sleep(10 * time.Millisecond)
						}
					}
					if ctx.Err() != nil {
						t.Fatal(ctx.Err())
					}
				}
				signal := syscall.SIGTERM
				if stage == "running" {
					signal = syscall.SIGINT
				}
				if err := process.Process.Signal(signal); err != nil {
					t.Fatal(err)
				}
				select {
				case err := <-done:
					joined = true
					if err != nil {
						data, _ := os.ReadFile(logPath)
						t.Fatalf("daemon graceful signal=%s exit: %v log=%s", signal, err, data)
					}
				case <-ctx.Done():
					t.Fatal("daemon did not stop", ctx.Err())
				}
				t.Logf("operator daemon stopped command=%s stage=%s domain=%q signal=%s exit=0 pid=%d", command, stage, domain, signal, process.Process.Pid)
			})
		}
	}
	if err := js.DeleteStream(ctx, "WF_INV"); err != nil {
		t.Fatal(err)
	}
	args := []string{"-url", cluster.Servers[0].ClientURL()}
	if domain != "" {
		args = append(args, "-domain", domain)
	}
	args = append(args, "project")
	encoded, err := json.Marshal(args)
	if err != nil {
		t.Fatal(err)
	}
	process := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestOperatorDaemonProcessHelper$")
	process.Env = append(os.Environ(), "WF_OPERATOR_DAEMON_ARGS="+string(encoded), "WF_OPERATOR_DAEMON_READY="+filepath.Join(root, "failure.ready"), "WF_OPERATOR_DAEMON_STAGE=failure")
	output, err := process.CombinedOutput()
	if err == nil || !strings.Contains(string(output), "stream not found") {
		t.Fatalf("missing source did not remain fatal: err=%v output=%s", err, output)
	}
	if err := os.WriteFile(filepath.Join(root, "fatal-startup.log"), output, 0600); err != nil {
		t.Fatal(err)
	}
	t.Logf("operator daemon fatal startup domain=%q error=stream-not-found exit=1", domain)

}
