//go:build linux

package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

func TestFiveReplicaReadinessDetailRetainsNestedCluster(t *testing.T) {
	info := &jetstream.StreamInfo{Cluster: &jetstream.ClusterInfo{Leader: "", Replicas: []*jetstream.PeerInfo{{Name: "lagging-node", Current: false, Offline: true, Lag: 17}}}}
	detail := fiveReplicaReadinessDetail("KV_WF_LEASE", info, errors.New("read timed out"))
	var decoded struct {
		Info  *jetstream.StreamInfo
		Error string
	}
	if err := json.Unmarshal([]byte(detail), &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.Info.Cluster.Replicas[0].Name != "lagging-node" || decoded.Info.Cluster.Replicas[0].Lag != 17 || decoded.Error != "read timed out" || strings.Contains(detail, "0x") {
		t.Fatal(detail)
	}
}

func TestFiveContainerFaultDiagnosticsRetainsPartialAndBoundedFailures(t *testing.T) {
	ctx, stop := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer stop()
	prefix := filepath.Join(t.TempDir(), "fault-1")
	started := time.Now()
	errs := saveFiveContainerFaultDiagnostics(ctx, func(ctx context.Context, node int, kind string) ([]byte, error) {
		if node == 0 {
			<-ctx.Done()
			return nil, ctx.Err()
		}
		if node == 1 && kind == "routes" {
			return []byte("truncated{"), nil
		}
		if node == 2 && kind == "connections" {
			return nil, errors.New("HTTP 503")
		}
		return []byte(fmt.Sprintf(`{"server":"node%d","kind":"%s","replicas":[{"current":false,"lag":17}]}`, node, kind)), nil
	}, prefix)
	if len(errs) > 0 {
		t.Fatal(errs)
	}
	if time.Since(started) > time.Second {
		t.Fatal("capture exceeded shared deadline")
	}
	files, err := filepath.Glob(prefix + "-failure-*.json")
	if err != nil || len(files) != 15 {
		t.Fatalf("files=%d err=%v", len(files), err)
	}
	for _, file := range files {
		data, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		var snapshot struct {
			Node          int
			Kind          string
			Before, After time.Time
			Data          json.RawMessage
			Error         string
		}
		if err := json.Unmarshal(data, &snapshot); err != nil {
			t.Fatal(err)
		}
		if snapshot.Before.Before(started) || snapshot.After.Before(snapshot.Before) {
			t.Fatal(string(data))
		}
		failed := snapshot.Node == 0 || (snapshot.Node == 1 && snapshot.Kind == "routes") || (snapshot.Node == 2 && snapshot.Kind == "connections")
		if failed {
			if snapshot.Error == "" || len(snapshot.Data) != 0 {
				t.Fatal(string(data))
			}
		} else {
			var payload struct {
				Server   string
				Kind     string
				Replicas []struct{ Lag int }
			}
			if err := json.Unmarshal(snapshot.Data, &payload); err != nil {
				t.Fatal(err)
			}
			if snapshot.Error != "" || payload.Server != fmt.Sprintf("node%d", snapshot.Node) || payload.Kind != snapshot.Kind || len(payload.Replicas) != 1 || payload.Replicas[0].Lag != 17 {
				t.Fatal(string(data))
			}
		}
	}
}
