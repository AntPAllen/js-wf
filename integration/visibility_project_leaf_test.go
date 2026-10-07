package integration_test

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"js-wf/testcluster"
)

type projectionLeafFault struct {
	kill    func()
	restart func()
}

func projectionLeafEndpoint(t *testing.T, ctx context.Context, hub *testcluster.Cluster, domain, fixtureRoot string, faultHooks ...*projectionLeafFault) (string, func()) {
	t.Helper()
	for {
		current := true
		admitted := false
		for _, s := range hub.Servers {
			current = current && s.JetStreamIsCurrent()
		}
		for _, s := range hub.Servers {
			if !s.JetStreamIsLeader() {
				continue
			}
			peers := s.JetStreamClusterPeers()
			sort.Strings(peers)
			admitted = current && strings.Join(peers, ",") == "wf-test-0,wf-test-1,wf-test-2"
		}
		if admitted {
			break
		}
		if ctx.Err() != nil {
			t.Fatal("hub metadata admission", ctx.Err())
		}
		time.Sleep(25 * time.Millisecond)
	}
	root := filepath.Join(fixtureRoot, "leaf-route")
	if err := os.Mkdir(root, 0700); err != nil {
		t.Fatal(err)
	}
	leaf, err := testcluster.StartLeafProcess(filepath.Join(root, "leaf-process"), "WFEDGE", hub.LeafURLs())
	if err != nil {
		t.Fatal(err)
	}
	// Always join the broker, even when setup or an assertion fails.
	t.Cleanup(leaf.Close)
	nc, err := nats.Connect(leaf.ClientURL(0), nats.IgnoreDiscoveredServers())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)
	local, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	before, err := local.AccountInfo(ctx)
	if err != nil || before.Domain != "WFEDGE" || before.Streams != 0 {
		t.Fatalf("leaf before=%+v err=%v", before, err)
	}
	pid := leaf.Commands[0].Process.Pid
	record := projectionLeafProcessRecord(t, leaf.Commands[0])
	currentRecord := record
	currentFile := "leaf.process.json"
	persist := func(name string, value any) {
		data, e := json.MarshalIndent(value, "", "  ")
		if e != nil {
			t.Error(e)
			return
		}
		if e = os.WriteFile(filepath.Join(root, name), append(data, '\n'), 0600); e != nil {
			t.Error(e)
		}
	}
	persist("leaf.process.json", record)
	leafID := nc.ConnectedServerId()
	proof := map[string]any{"test": t.Name(), "remote_domain": domain, "local_domain": before.Domain, "leaf_id": leafID, "leaf_pid": pid, "leaf_url": leaf.ClientURL(0), "local_streams_before": before.Streams}
	var beforeLeaf []byte
	remote, e := jetstream.NewWithDomain(nc, domain)
	if e != nil {
		t.Fatal(e)
	}
	for {
		beforeLeaf, e = leaf.Diagnostic(ctx, 0, "leaf")
		if e != nil {
			t.Fatal(e)
		}
		var topology struct {
			ServerID  string `json:"server_id"`
			LeafNodes int    `json:"leafnodes"`
		}
		if e = json.Unmarshal(beforeLeaf, &topology); e != nil {
			t.Fatal(e)
		}
		attempt, stop := context.WithTimeout(ctx, 250*time.Millisecond)
		account, e := remote.AccountInfo(attempt)
		stop()
		if e == nil && account.Domain != domain {
			t.Fatal("wrong remote domain", account.Domain)
		}
		if topology.ServerID == leafID && topology.LeafNodes == 1 && e == nil {
			break
		}
		if ctx.Err() != nil {
			t.Fatal("leaf remote readiness", ctx.Err())
		}
		time.Sleep(25 * time.Millisecond)
	}
	proof["leaf_before"] = json.RawMessage(beforeLeaf)
	peers := []map[string]string{}
	for _, s := range hub.Servers {
		peers = append(peers, map[string]string{"name": s.Name(), "id": s.ID(), "domain": domain})
	}
	proof["hubs"] = peers
	if len(faultHooks) > 0 {
		hook := faultHooks[0]
		hook.kill = func() {
			started := time.Now().UTC()
			if err := leaf.KillNode(0); err != nil {
				t.Fatal(err)
			}
			status, ok := leaf.Commands[0].ProcessState.Sys().(syscall.WaitStatus)
			if !ok || !status.Signaled() || status.Signal() != syscall.SIGKILL {
				t.Fatal("leaf not reaped SIGKILL")
			}
			record["reaped"] = true
			record["exit_code"] = -1
			record["exit_signal"] = "killed"
			record["kill_started_at"] = started
			record["reaped_at"] = time.Now().UTC()
			persist("leaf.process.json", record)
			proof["leaf_sigkill"] = true
			t.Logf("SQL leaf SIGKILL: pid=%d id=%s reaped=true", pid, leafID)
		}
		hook.restart = func() {
			if err := leaf.RestartNode(0); err != nil {
				t.Fatal(err)
			}
			currentRecord = projectionLeafProcessRecord(t, leaf.Commands[0])
			currentFile = "leaf.replacement.process.json"
			currentRecord["admitted_at"] = time.Now().UTC()
			persist(currentFile, currentRecord)
			for {
				attempt, stop := context.WithTimeout(ctx, 250*time.Millisecond)
				account, e := remote.AccountInfo(attempt)
				stop()
				if e == nil && account.Domain == domain && nc.ConnectedServerId() != leafID {
					break
				}
				if ctx.Err() != nil {
					t.Fatal("replacement leaf readiness", ctx.Err())
				}
				time.Sleep(25 * time.Millisecond)
			}
			proof["leaf_replacement_id"] = nc.ConnectedServerId()
			proof["leaf_replacement_pid"] = leaf.Commands[0].Process.Pid
			proof["leaf_replacement_ready_at"] = time.Now().UTC()
			t.Logf("SQL leaf replacement: old_pid=%d pid=%d old_id=%s id=%s same_ports_store=true", pid, leaf.Commands[0].Process.Pid, leafID, nc.ConnectedServerId())
		}
	}
	return leaf.ClientURL(0), func() {
		observe, stop := context.WithTimeout(context.Background(), 3*time.Second)
		defer stop()
		after, e := local.AccountInfo(observe)
		if e != nil || after.Domain != "WFEDGE" || after.Streams != 0 {
			t.Errorf("leaf after=%+v err=%v", after, e)
		} else {
			proof["local_streams_after"] = after.Streams
		}
		afterLeaf, e := leaf.Diagnostic(observe, 0, "leaf")
		if e != nil {
			t.Error(e)
		} else {
			proof["leaf_after"] = json.RawMessage(afterLeaf)
			afterHubs := []map[string]string{}
			for _, server := range hub.Servers {
				afterHubs = append(afterHubs, map[string]string{"name": server.Name(), "id": server.ID(), "domain": domain})
			}
			proof["hubs_after"] = afterHubs
		}
		leaf.Close()
		currentRecord["reaped"] = leaf.Commands[0].ProcessState != nil
		if leaf.Commands[0].ProcessState != nil {
			currentRecord["exit_code"] = leaf.Commands[0].ProcessState.ExitCode()
		}
		persist(currentFile, currentRecord)
		proof["scenario_passed"] = !t.Failed()
		persist("leaf-proof.json", proof)
		t.Logf("SQL projector leaf: test=%s local=WFEDGE remote=%s leaf_pid=%d local_streams=0", t.Name(), domain, pid)
	}
}

func projectionLeafProcessRecord(t *testing.T, command *exec.Cmd) map[string]any {
	t.Helper()
	pid := command.Process.Pid
	proc := filepath.Join("/proc", fmt.Sprint(pid))
	stat, err := os.ReadFile(filepath.Join(proc, "stat"))
	if err != nil {
		t.Fatal(err)
	}
	argv, err := os.ReadFile(filepath.Join(proc, "cmdline"))
	if err != nil {
		t.Fatal(err)
	}
	hash, err := projectionFileSHA(filepath.Join(proc, "exe"))
	if err != nil {
		t.Fatal(err)
	}
	if string(argv) != strings.Join(command.Args, "\x00")+"\x00" {
		t.Fatal("actual leaf argv mismatch")
	}
	info, err := exec.Command("go", "version", "-m", command.Args[0]).Output()
	if err != nil {
		t.Fatal(err)
	}
	record := map[string]any{"pid": pid, "stat": string(stat), "argv": command.Args, "exe": command.Args[0], "exe_sha256": hash, "build_info": string(info)}
	return record
}

func projectionFileSHA(path string) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer file.Close()
	hash := sha256.New()
	if _, err = io.Copy(hash, file); err != nil {
		return "", err
	}
	return fmt.Sprintf("%x", hash.Sum(nil)), nil
}
