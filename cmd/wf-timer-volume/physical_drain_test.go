package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func validPhysicalAudit(partitions uint32) physicalDrainAudit {
	return inspectPhysicalDrain(context.Background(), 3, func(_ context.Context, node int) ([]byte, error) {
		consumers := []any{}
		for p := uint32(0); p < partitions; p++ {
			consumers = append(consumers, map[string]any{"name": fmt.Sprintf("volume-%d", p), "num_pending": 0, "num_ack_pending": 0})
		}
		return json.Marshal(map[string]any{"server_id": fmt.Sprintf("server-%d", node), "account_details": []any{map[string]any{"stream_detail": []any{map[string]any{"name": "WF_RUN", "state": map[string]any{"messages": 0, "last_seq": 2000000, "consumer_count": partitions}, "consumer_detail": consumers, "cluster": map[string]any{"replicas": []any{map[string]any{"current": true}}}}}}}})
	})
}

func mutatePhysicalReplica(t *testing.T, a *physicalDrainAudit, node int, mutate func(map[string]any, map[string]any)) {
	t.Helper()
	var root map[string]any
	if err := json.Unmarshal(a.Replicas[node].Data, &root); err != nil {
		t.Fatal(err)
	}
	account := root["account_details"].([]any)[0].(map[string]any)
	stream := account["stream_detail"].([]any)[0].(map[string]any)
	mutate(root, stream)
	data, err := json.Marshal(root)
	if err != nil {
		t.Fatal(err)
	}
	a.Replicas[node].Data = data
}

func TestPhysicalDrainRejectsFalseEmptyReplicas(t *testing.T) {
	for _, mode := range []string{"valid", "retained_sources", "pending", "ack_pending", "missing_count", "missing_pending", "partial_consumers", "duplicate_consumer", "duplicate_server", "missing_server", "wrong_sequence", "wrong_node", "partial_replicas", "missing_response", "error", "unknown_version"} {
		t.Run(mode, func(t *testing.T) {
			a := validPhysicalAudit(64)
			mutatePhysicalReplica(t, &a, 1, func(root, stream map[string]any) {
				state := stream["state"].(map[string]any)
				consumers := stream["consumer_detail"].([]any)
				switch mode {
				case "retained_sources":
					state["messages"] = 141
				case "pending":
					consumers[0].(map[string]any)["num_pending"] = 1
				case "ack_pending":
					consumers[0].(map[string]any)["num_ack_pending"] = 1
				case "missing_count":
					delete(state, "messages")
				case "missing_pending":
					delete(consumers[0].(map[string]any), "num_pending")
				case "partial_consumers":
					stream["consumer_detail"] = consumers[:63]
				case "duplicate_consumer":
					consumers[1] = consumers[0]
				case "duplicate_server":
					root["server_id"] = "server-0"
				case "missing_server":
					delete(root, "server_id")
				case "wrong_sequence":
					state["last_seq"] = 1999999
				}
			})
			switch mode {
			case "wrong_node":
				a.Replicas[1].Node = 0
			case "partial_replicas":
				a.Replicas = a.Replicas[:2]
			case "missing_response":
				a.Replicas[1].Data = nil
			case "error":
				a.Error = "replica 2 monitoring: context deadline exceeded"
			case "unknown_version":
				a.Version++
			}
			if a.complete(3, 64) != (mode == "valid") {
				t.Fatalf("incorrect physical drain verdict: %s", mode)
			}
			data, err := json.Marshal(a)
			if err != nil {
				t.Fatal(err)
			}
			var restored physicalDrainAudit
			if err := json.Unmarshal(data, &restored); err != nil {
				t.Fatal(err)
			}
			if restored.complete(3, 64) != (mode == "valid") {
				t.Fatal("disk serialization changed verdict")
			}
		})
	}
}

func TestPhysicalDrainRetainsPartialResponseAtDeadline(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	valid := validPhysicalAudit(64)
	a := inspectPhysicalDrain(ctx, 3, func(c context.Context, node int) ([]byte, error) {
		if c != ctx {
			t.Fatal("changed caller deadline")
		}
		if node == 1 {
			cancel()
		}
		if err := c.Err(); err != nil {
			return nil, err
		}
		return valid.Replicas[node].Data, nil
	})
	if len(a.Replicas) != 1 || a.Error != "replica 1 monitoring: context canceled" || a.complete(3, 64) {
		t.Fatalf("lost partial evidence: %+v", a)
	}
}

func TestPhysicalDrainAgainstRetainedStoreSnapshots(t *testing.T) {
	root := os.Getenv("WF_PHYSICAL_DRAIN_SNAPSHOTS")
	if root == "" {
		t.Skip("set WF_PHYSICAL_DRAIN_SNAPSHOTS for retained native-store monitoring responses")
	}
	a := inspectPhysicalDrain(context.Background(), 3, func(_ context.Context, node int) ([]byte, error) {
		return os.ReadFile(filepath.Join(root, fmt.Sprintf("snapshot-14-node-%d.json", node)))
	})
	if a.Error != "" || len(a.Replicas) != 3 {
		t.Fatalf("missing retained evidence: %+v", a)
	}
	for node, expected := range []uint64{768, 141, 0} {
		_, messages, pending, _, err := physicalReplicaState(a.Replicas[node].Data, 64)
		if err != nil || messages != expected || pending != 0 {
			t.Fatalf("replica %d: messages=%d pending=%d err=%v", node, messages, pending, err)
		}
	}
	if a.complete(3, 64) {
		t.Fatal("retained physical sources certified empty")
	}
}
