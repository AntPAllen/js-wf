//go:build linux

package integration_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"js-wf/identity"
	"js-wf/lease"
	"js-wf/testcluster"
)

type leaseDiskMeasurement struct {
	Operation       string        `json:"operation"`
	Calls           int           `json:"calls"`
	Duration        time.Duration `json:"duration_ns"`
	Start           time.Time     `json:"start"`
	End             time.Time     `json:"end"`
	RaftBytesBefore int           `json:"raft_bytes_before"`
	RaftBytesAfter  int           `json:"raft_bytes_after"`
	RaftHashBefore  string        `json:"raft_hash_before"`
	RaftHashAfter   string        `json:"raft_hash_after"`
}

// This leaf contract holds one initialized production lease on one fixed R3
// cluster. It measures rejected acquisition versus reads and confirmed renewal;
// it does not attribute a mixed workload's latency to this isolated case.
func TestLeaseContentionDiskContract(t *testing.T) {
	if os.Getenv("WF_LEASE_DISK_CONTRACT") != "1" {
		t.Skip("set WF_LEASE_DISK_CONTRACT=1 for the real disk/lease contention contract")
	}
	if _, err := exec.LookPath("strace"); err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	cluster, err := testcluster.StartProcesses(root, 3)
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Close()
	ctx, stop := context.WithTimeout(context.Background(), time.Minute)
	defer stop()
	js, err := jetstream.New(cluster.Clients[0])
	if err != nil {
		t.Fatal(err)
	}
	var kv jetstream.KeyValue
	for ctx.Err() == nil {
		attempt, cancel := context.WithTimeout(ctx, time.Second)
		kv, err = js.CreateKeyValue(attempt, jetstream.KeyValueConfig{Bucket: "LEASE_PROBE", Replicas: 3, Storage: jetstream.FileStorage, TTL: time.Minute})
		cancel()
		if err == nil {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if err != nil {
		t.Fatal(err)
	}
	store := lease.NewWithKeyValue(kv)
	owner, err := store.Acquire(ctx, "test", "held", "owner")
	if err != nil {
		t.Fatal(err)
	}
	key := identity.Key("test", "held")
	original, err := kv.Get(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	stream, err := js.Stream(ctx, "KV_LEASE_PROBE")
	if err != nil {
		t.Fatal(err)
	}
	info, err := stream.Info(ctx)
	if err != nil || info.Cluster == nil {
		t.Fatalf("leader=%+v err=%v", info, err)
	}
	leader, err := strconv.Atoi(strings.TrimPrefix(info.Cluster.Leader, "wf-process-"))
	if err != nil || leader < 0 || leader >= 3 {
		t.Fatalf("leader=%+v err=%v", info.Cluster, err)
	}
	// Reads in the measured rows use the pinned leader, so replica visibility
	// cannot masquerade as a changed owner or stale revision.
	pinned, err := jetstream.New(cluster.Clients[leader])
	if err != nil {
		t.Fatal(err)
	}
	kv, err = pinned.KeyValue(ctx, "LEASE_PROBE")
	if err != nil {
		t.Fatal(err)
	}
	store = lease.NewWithKeyValue(kv)
	original, err = kv.Get(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	var held lease.Value
	if err := json.Unmarshal(original.Value(), &held); err != nil || held.Worker != "owner" || held.Epoch != owner.Epoch() {
		t.Fatalf("initialized owner=%+v err=%v", held, err)
	}
	raw, err := cluster.Diagnostic(ctx, leader, "jetstream")
	if err != nil {
		t.Fatal(err)
	}
	var report struct {
		Accounts []struct {
			Streams []struct {
				Name  string `json:"name"`
				Group string `json:"stream_raft_group"`
			} `json:"stream_detail"`
		} `json:"account_details"`
	}
	if err := json.Unmarshal(raw, &report); err != nil {
		t.Fatal(err)
	}
	group := ""
	for _, account := range report.Accounts {
		for _, s := range account.Streams {
			if s.Name == "KV_LEASE_PROBE" {
				group = s.Group
			}
		}
	}
	if group == "" {
		t.Fatal("missing stream Raft group")
	}
	wal := func() ([]byte, error) {
		paths, err := filepath.Glob(filepath.Join(root, fmt.Sprintf("node-%d", leader), "jetstream", "$SYS", "_js_", group, "msgs", "*.blk"))
		if err != nil {
			return nil, err
		}
		if len(paths) == 0 {
			return nil, fmt.Errorf("no Raft WAL blocks for %s", group)
		}
		var data []byte
		for _, path := range paths {
			b, err := os.ReadFile(path)
			if err != nil {
				return nil, err
			}
			data = append(data, b...)
		}
		return data, nil
	}
	const diskDelay = 85 * time.Millisecond
	if err := cluster.SlowDisk(leader, diskDelay); err != nil {
		t.Fatal(err)
	}
	var rows []leaseDiskMeasurement
	defer func() {
		_ = cluster.StopSlowDisk(leader)
		if prefix := os.Getenv("LEASE_DISK_REPORT"); prefix != "" {
			data, err := json.MarshalIndent(struct {
				Leader       string
				RaftGroup    string
				Epoch        uint64
				Measurements []leaseDiskMeasurement
			}{info.Cluster.Leader, group, owner.Epoch(), rows}, "", "  ")
			if err != nil {
				t.Error(err)
			} else if err = os.WriteFile(prefix+".json", data, 0600); err != nil {
				t.Error(err)
			}
			if err := os.WriteFile(prefix+"-jetstream.json", raw, 0600); err != nil {
				t.Error(err)
			}
			trace, err := os.ReadFile(cluster.DiskTracePath(leader))
			if err != nil {
				t.Error(err)
			} else if err = os.WriteFile(prefix+"-disk.log", trace, 0600); err != nil {
				t.Error(err)
			}
		}
	}()
	for _, operation := range []string{"rejected_create", "held_read", "renew"} {
		before, err := wal()
		if err != nil {
			t.Fatal(err)
		}
		start := time.Now()
		for i := 0; i < 8; i++ {
			switch operation {
			case "rejected_create":
				contender, err := store.Acquire(ctx, "test", "held", fmt.Sprintf("contender-%d", i))
				if contender != nil || !errors.Is(err, lease.ErrHeld) {
					t.Fatalf("contender=%v err=%v", contender, err)
				}
			case "held_read":
				value, err := kv.Get(ctx, key)
				if err != nil || value.Revision() != original.Revision() || !bytes.Equal(value.Value(), original.Value()) {
					t.Fatalf("held read changed: %v", err)
				}
			case "renew":
				if err := owner.Renew(ctx); err != nil {
					t.Fatal(err)
				}
			}
		}
		end := time.Now()
		after, err := wal()
		if err != nil {
			t.Fatal(err)
		}
		hashBefore, hashAfter := sha256.Sum256(before), sha256.Sum256(after)
		rows = append(rows, leaseDiskMeasurement{Operation: operation, Calls: 8, Duration: end.Sub(start), Start: start, End: end, RaftBytesBefore: len(before), RaftBytesAfter: len(after), RaftHashBefore: hex.EncodeToString(hashBefore[:]), RaftHashAfter: hex.EncodeToString(hashAfter[:])})
		if operation != "renew" && !bytes.Equal(before, after) {
			t.Fatalf("%s modified leader Raft WAL: before=%d after=%d", operation, len(before), len(after))
		}
		if operation == "renew" && (bytes.Equal(before, after) || len(after) <= len(before)) {
			t.Fatal("confirmed renewals did not append Raft WAL positive control")
		}
		if operation == "renew" && end.Sub(start) < 8*diskDelay {
			t.Fatalf("sequential confirmed renewals missed disk delay lower bound: elapsed=%v want>=%v", end.Sub(start), 8*diskDelay)
		}
		current, err := kv.Get(ctx, key)
		if err != nil || !bytes.Equal(current.Value(), original.Value()) {
			t.Fatalf("owner changed: %v", err)
		}
		if operation != "renew" && current.Revision() != original.Revision() {
			t.Fatal("read/rejected acquire changed revision")
		}
		if operation == "renew" && current.Revision() <= original.Revision() {
			t.Fatal("renewals did not advance revision")
		}
		currentInfo, err := stream.Info(ctx)
		if err != nil || currentInfo.Cluster == nil || currentInfo.Cluster.Leader != info.Cluster.Leader {
			t.Fatalf("placement changed: info=%+v err=%v", currentInfo, err)
		}
		t.Logf("lease disk contract: operation=%s calls=8 elapsed=%v raft_bytes=%d->%d", operation, end.Sub(start), len(before), len(after))
	}
	if err := owner.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := kv.Get(ctx, key); !errors.Is(err, jetstream.ErrKeyNotFound) {
		t.Fatalf("release failed: %v", err)
	}
	if err := cluster.StopSlowDisk(leader); err != nil {
		t.Fatal(err)
	}
	trace, err := os.ReadFile(cluster.DiskTracePath(leader))
	if err != nil || !bytes.Contains(trace, []byte("(DELAYED)")) || !bytes.Contains(trace, []byte("/"+group+"/msgs/")) {
		t.Fatalf("missing delayed syscall / selected Raft group evidence: %v", err)
	}
}
