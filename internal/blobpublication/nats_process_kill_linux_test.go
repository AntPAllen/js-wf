package blobpublication

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"js-wf/testcluster"
)

const nativeBlobChildMode = "WF_BLOB_PROCESS_MODE"
const nativeBlobChildURL = "WF_BLOB_PROCESS_URL"

func nativeKillPayload() []byte { return bytes.Repeat([]byte("process-kill-native"), 17000) }

// This helper runs only inside an admitted subprocess of the actual test SDK.
// The parent holds a complete request before killing/reaping the process. It
// returns normally when the ordinary package run has no helper mode configured.
func TestNativeBlobProcessChild(t *testing.T) {
	mode := os.Getenv(nativeBlobChildMode)
	if mode == "" {
		return
	}
	if mode != "upload-metadata" && mode != "upload-root" && mode != "collect-purge" {
		t.Fatal("invalid child mode", mode)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	conn, err := nats.Connect(os.Getenv(nativeBlobChildURL), nats.NoReconnect())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	js, err := jetstream.New(conn)
	if err != nil {
		t.Fatal(err)
	}
	authority, err := OpenNativeAuthority(ctx, js, "BLOB_AUTH", "wf.blob.authority")
	if err != nil {
		t.Fatal(err)
	}
	port, err := OpenNativePort(ctx, authority, "RECOVERABLE_BLOB")
	if err != nil {
		t.Fatal(err)
	}
	p := nativeAttemptProtocol(port)
	if mode == "collect-purge" {
		if _, err = p.Sweep(ctx, time.Now().Add(time.Hour)); err != nil {
			t.Fatal(err)
		}
	} else {
		prepared, err := p.Prepare(ctx, "killed", []byte("process publication"), [][]byte{nativeKillPayload()}, time.Now().Add(time.Minute))
		if err != nil {
			t.Fatal(err)
		}
		if _, err = p.Commit(ctx, prepared); err != nil {
			t.Fatal(err)
		}
	}
	t.Fatal("child completed instead of waiting at its held request")
}

type nativeBlobChild struct {
	cmd    *exec.Cmd
	done   chan error
	waited bool
}

func startNativeBlobChild(t *testing.T, ctx context.Context, directory, mode, url string) *nativeBlobChild {
	t.Helper()
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.CommandContext(ctx, executable, "-test.run=^TestNativeBlobProcessChild$", "-test.v", "-test.count=1", "-test.timeout=30s")
	// Replace rather than duplicate the controlling environment entries.
	var env []string
	for _, entry := range os.Environ() {
		name, _, _ := strings.Cut(entry, "=")
		if name != nativeBlobChildMode && name != nativeBlobChildURL {
			env = append(env, entry)
		}
	}
	cmd.Env = append(env, nativeBlobChildMode+"="+mode, nativeBlobChildURL+"="+url)
	log, err := os.OpenFile(filepath.Join(directory, "child.log"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	cmd.Stdout, cmd.Stderr = log, log
	if err = cmd.Start(); err != nil {
		log.Close()
		t.Fatal(err)
	}
	child := &nativeBlobChild{cmd: cmd, done: make(chan error, 1)}
	go func() { child.done <- cmd.Wait() }()
	t.Cleanup(func() {
		if !child.waited {
			_ = cmd.Process.Kill()
			<-child.done
			child.waited = true
		}
		_ = log.Close()
	})
	return child
}
func nativeExecutableHash(path string) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer file.Close()
	h := sha256.New()
	if _, err = io.Copy(h, file); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}
func nativeChildSnapshot(t *testing.T, child *nativeBlobChild, mode, url string) map[string]any {
	t.Helper()
	proc := filepath.Join("/proc", strconv.Itoa(child.cmd.Process.Pid))
	stat, err := os.ReadFile(filepath.Join(proc, "stat"))
	if err != nil {
		t.Fatal(err)
	}
	closing := strings.LastIndex(string(stat), ") ")
	if closing < 0 {
		t.Fatal("invalid process stat")
	}
	fields := strings.Fields(string(stat)[closing+2:])
	if len(fields) < 20 {
		t.Fatal("short process stat")
	}
	args, err := os.ReadFile(filepath.Join(proc, "cmdline"))
	if err != nil {
		t.Fatal(err)
	}
	argv := strings.Split(strings.TrimSuffix(string(args), "\x00"), "\x00")
	if !reflect.DeepEqual(argv, child.cmd.Args) {
		t.Fatal("child argv mismatch", argv)
	}
	environ, err := os.ReadFile(filepath.Join(proc, "environ"))
	if err != nil {
		t.Fatal(err)
	}
	values := map[string]string{}
	for _, line := range strings.Split(string(environ), "\x00") {
		name, value, ok := strings.Cut(line, "=")
		if ok {
			values[name] = value
		}
	}
	profile := map[string]string{}
	for _, name := range []string{"GOMAXPROCS", "GOMEMLIMIT", nativeBlobChildMode, nativeBlobChildURL} {
		profile[name] = values[name]
	}
	if profile[nativeBlobChildMode] != mode || profile[nativeBlobChildURL] != url || profile["GOMAXPROCS"] != os.Getenv("GOMAXPROCS") || profile["GOMEMLIMIT"] != os.Getenv("GOMEMLIMIT") {
		t.Fatal("child profile mismatch", profile)
	}
	digest, err := nativeExecutableHash(filepath.Join(proc, "exe"))
	if err != nil {
		t.Fatal(err)
	}
	parent, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	expected, err := nativeExecutableHash(parent)
	if err != nil || digest != expected {
		t.Fatal("child executable differs from parent SDK", err)
	}
	return map[string]any{"pid": child.cmd.Process.Pid, "start_ticks": fields[19], "args": argv, "exe_sha256": digest, "environment": profile}
}
func killNativeBlobChild(t *testing.T, ctx context.Context, child *nativeBlobChild) map[string]any {
	t.Helper()
	if err := child.cmd.Process.Kill(); err != nil {
		t.Fatal("SIGKILL failed", err)
	}
	var err error
	select {
	case err = <-child.done:
		child.waited = true
	case <-ctx.Done():
		t.Fatal("killed child did not join", ctx.Err())
	}
	var exit *exec.ExitError
	if !errors.As(err, &exit) {
		t.Fatal("child not killed", err)
	}
	status, ok := child.cmd.ProcessState.Sys().(syscall.WaitStatus)
	if !ok || !status.Signaled() || status.Signal() != syscall.SIGKILL {
		t.Fatal("wrong child disposition", status)
	}
	if _, err = os.Stat(filepath.Join("/proc", strconv.Itoa(child.cmd.Process.Pid))); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("killed child remains", err)
	}
	return map[string]any{"signal": int(status.Signal()), "exit_code": child.cmd.ProcessState.ExitCode(), "joined": true, "proc_absent": true}
}

func TestNativeBlobUploaderAndCollectorSIGKILL(t *testing.T) {
	for _, replicas := range []int{1, 3} {
		for _, mode := range []string{"upload-metadata", "upload-root", "collect-purge"} {
			t.Run(fmt.Sprintf("R%d/%s", replicas, mode), func(t *testing.T) {
				cluster, controller, ctx, directory := nativeObjectFixture(t, replicas)
				payload := nativeKillPayload()
				name := objectName(key(payload), 1, "attempt")
				var retired Root
				if mode == "collect-purge" {
					live := nativeCommit(t, Protocol{Port: controller}, ctx, nativePrepare(t, Protocol{Port: controller}, ctx, "killed", payload))
					name = live.Blobs[key(payload)].Object
					if err := (&Protocol{Port: controller}).Retire(ctx, "killed", live.Head); err != nil {
						t.Fatal(err)
					}
					var err error
					retired, err = controller.ReadRoot(ctx, "killed")
					if err != nil || retired.Head != 2 || retired.Token != "" {
						t.Fatal("collector root not retired", retired, err)
					}
				}
				subject := controller.metaSubject(name)
				if mode == "upload-root" {
					subject = controller.subject("root", "killed")
				}
				if mode == "collect-purge" {
					subject = "$JS.API.STREAM.PURGE.OBJ_RECOVERABLE_BLOB"
				}
				proxy, err := testcluster.NewClientProxy(cluster.Servers[0].ClientURL())
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(proxy.Close)
				if err = proxy.HoldFirstPublicationExceptHeader(subject, "Wf-Authority-Read-Witness", "1"); err != nil {
					t.Fatal(err)
				}
				if err = proxy.EnableTrafficTrace(2 << 20); err != nil {
					t.Fatal(err)
				}
				child := startNativeBlobChild(t, ctx, directory, mode, proxy.URL())
				nativeWait(t, ctx, func() bool { return proxy.PendingAPI() != nil })
				held := proxy.PendingAPI()
				if held.Subject != subject || held.ForwardedBytes != 0 || held.Disposition != "held" {
					t.Fatal("wrong process kill barrier", held)
				}
				first := nativeChildSnapshot(t, child, mode, proxy.URL())
				second := nativeChildSnapshot(t, child, mode, proxy.URL())
				if !reflect.DeepEqual(first, second) {
					t.Fatal("child identity changed during admission")
				}
				before, err := controller.objectStream.Info(ctx, jetstream.WithSubjectFilter("$O."+controller.bucket+".>"))
				if err != nil {
					t.Fatal(err)
				}
				blob, err := controller.ReadBlob(ctx, key(payload))
				if err != nil {
					t.Fatal(err)
				}
				metadata, _, err := controller.metadata(ctx, name)
				if err != nil {
					t.Fatal(err)
				}
				switch mode {
				case "upload-metadata":
					if before.State.Msgs != 3 || before.State.NumSubjects != 1 || metadata != nil || blob.Fence.Phase != "uploading" {
						t.Fatal("partial upload not captured", before, metadata, blob)
					}
				case "upload-root":
					if before.State.Msgs != 4 || before.State.NumSubjects != 2 || metadata == nil || metadata.Deleted || blob.Fence.Phase != "ready" {
						t.Fatal("ready publication not captured", before, metadata, blob)
					}
				case "collect-purge":
					if before.State.Msgs != 4 || before.State.NumSubjects != 2 || metadata == nil || !metadata.Deleted || metadata.Chunks != 0 || blob.Fence.Phase != "closed" {
						t.Fatal("collector tombstone boundary not captured", before, metadata, blob)
					}
				}
				death := killNativeBlobChild(t, ctx, child)
				proxy.Close()
				packet := proxy.PendingAPI()
				if packet.Disposition != "cancelled" || packet.ForwardedBytes != 0 {
					t.Fatal("killed process packet forwarded", packet)
				}
				if mode != "collect-purge" {
					if n := nativeSweep(t, Protocol{Port: controller}, ctx); n != 1 {
						t.Fatal("killed uploader orphan not reclaimed", n)
					}
					nativeNoChunks(t, controller, ctx)
				}
				fenced, err := controller.ReadRoot(ctx, "killed")
				if err != nil || fenced.Token != "" || len(fenced.Blobs) != 0 || fenced.Head != 1 && mode != "collect-purge" || fenced.Head != 2 && mode == "collect-purge" {
					t.Fatal("killed root not permanently fenced", fenced, err)
				}
				closed, err := controller.ReadBlob(ctx, key(payload))
				if err != nil || closed.Fence.Generation != 1 || closed.Fence.Phase != "closed" {
					t.Fatal("killed generation not closed", closed, err)
				}
				fresh := nativeCommit(t, Protocol{Port: controller}, ctx, nativePrepare(t, Protocol{Port: controller}, ctx, "fresh", payload))
				if fresh.Blobs[key(payload)].Generation != 2 || fresh.Blobs[key(payload)].Object == name {
					t.Fatal("killed generation reused")
				}
				deleted := nativeSweep(t, Protocol{Port: controller}, ctx)
				if mode == "collect-purge" && deleted != 1 || mode != "collect-purge" && deleted != 0 {
					t.Fatal("collector retry touched wrong object", deleted)
				}
				nativeCheckRoot(t, controller, ctx, "fresh")
				old, _, err := controller.metadata(ctx, name)
				if err != nil || old == nil || !old.Deleted {
					t.Fatal("killed attempt tombstone absent", old, err)
				}
				if err = (&Protocol{Port: controller}).Retire(ctx, "fresh", fresh.Head); err != nil {
					t.Fatal(err)
				}
				if n := nativeSweep(t, Protocol{Port: controller}, ctx); n != 1 {
					t.Fatal("fresh retirement failed", n)
				}
				nativeNoChunks(t, controller, ctx)
				after, err := controller.objectStream.Info(ctx, jetstream.WithSubjectFilter("$O."+controller.bucket+".>"))
				if err != nil || after.State.Msgs != 2 || after.State.NumSubjects != 2 {
					t.Fatal("kill final state", after, err)
				}
				nativeObjectProof(t, directory, map[string]any{"scenario": "process-sigkill-" + mode, "replicas": replicas, "child": first, "stable_child_identity_observed_twice": true, "death": death, "held": held, "final_packet": packet, "wire": proxy.TrafficTrace(), "partial_before": before, "before_fence": blob, "before_metadata": metadata, "retired_root": retired, "fenced_root": fenced, "closed_generation": closed, "deleted_partial": old, "fresh_root": fresh, "old_physical_name": name, "object_stream": after, "fresh_root_survived_old_delete": true, "all_chunks_reclaimed": true})
				t.Logf("native process SIGKILL: R%d mode=%s child=%d signal=9 joined/absent; held packet cancelled with zero forwarded bytes; generation2 readable; final chunks=0", replicas, mode, child.cmd.Process.Pid)
			})
		}
	}
}
