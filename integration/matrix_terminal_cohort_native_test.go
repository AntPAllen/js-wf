//go:build linux

package integration_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"js-wf/integrity"
	"js-wf/internal/natsutil"
	"js-wf/testcluster"
)

type matrixStaleTerminalStream struct {
	jetstream.Stream
	state jetstream.StreamState
}

func (s matrixStaleTerminalStream) Info(ctx context.Context, opts ...jetstream.StreamInfoOpt) (*jetstream.StreamInfo, error) {
	info, err := s.Stream.Info(ctx, opts...)
	if err != nil {
		return nil, err
	}
	info.State = s.state
	return info, nil
}
func TestNativeMatrixTerminalCapturedPopulation(t *testing.T) {
	directory := t.TempDir()
	if base := os.Getenv("WF_MATRIX_TERMINAL_COHORT_ROOT"); base != "" {
		if !filepath.IsAbs(base) {
			t.Fatal("absolute retained fixture root required")
		}
		directory = filepath.Join(base, t.Name())
		if err := os.Mkdir(directory, 0700); err != nil {
			t.Fatal(err)
		}
	}
	cluster, err := testcluster.Start(directory, 3)
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	for {
		ready := false
		for _, node := range cluster.Servers {
			ready = ready || node.JetStreamIsLeader() && len(node.JetStreamClusterPeers()) == 3
		}
		if ready {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(20 * time.Millisecond):
		}
	}
	js, err := jetstream.New(cluster.Clients[0])
	if err != nil {
		t.Fatal(err)
	}
	inv, err := js.CreateStream(ctx, jetstream.StreamConfig{Name: "WF_INV", Subjects: []string{"wf.inv.>"}, Storage: jetstream.FileStorage, Replicas: 3})
	if err != nil {
		t.Fatal(err)
	}
	for i := 1; i <= 840; i++ {
		ack, err := js.Publish(ctx, fmt.Sprintf("wf.inv.audit.id%d", i), []byte(`{}`))
		if err != nil || ack.Sequence != uint64(i) {
			t.Fatal(i, ack, err)
		}
	}
	cutoff, err := integrity.CaptureInvocationCutoff(ctx, 822, func(call context.Context) (*jetstream.RawStreamMsg, error) {
		return natsutil.GetInvocationTailFromLeader(call, js)
	})
	if err != nil || cutoff != 840 {
		t.Fatal("capture", cutoff, err)
	}
	visits := map[string]int{}
	for name, state := range map[string]jetstream.StreamState{"stale": {FirstSeq: 1, LastSeq: 812}, "empty": {}} {
		count := 0
		err := matrixVisitCompletedInvocations(ctx, matrixStaleTerminalStream{Stream: inv, state: state}, cutoff, 840, func(raw *jetstream.RawStreamMsg) error {
			count++
			if raw.Sequence != uint64(count) {
				return fmt.Errorf("sequence=%d want=%d", raw.Sequence, count)
			}
			return nil
		})
		if err != nil || count != 840 {
			t.Fatal(name, count, err)
		}
		visits[name] = count
	}
	stale := matrixStaleTerminalStream{Stream: inv, state: jetstream.StreamState{FirstSeq: 1, LastSeq: 812}}
	weak := 0
	if err = matrixVisitCompletedInvocations(ctx, stale, 812, 812, func(*jetstream.RawStreamMsg) error { weak++; return nil }); err != nil || weak != 812 {
		t.Fatal("weak control", weak, err)
	}
	if err = inv.DeleteMsg(ctx, 840); err != nil {
		t.Fatal(err)
	}
	incomplete := 0
	missingErr := matrixVisitCompletedInvocations(ctx, stale, cutoff, 840, func(*jetstream.RawStreamMsg) error { incomplete++; return nil })
	if missingErr == nil || incomplete != 839 {
		t.Fatal("tail omission accepted", incomplete, missingErr)
	}
	peers := []map[string]any{}
	for _, node := range cluster.Servers {
		v, err := node.Varz(nil)
		if err != nil {
			t.Fatal(err)
		}
		peers = append(peers, map[string]any{"id": node.ID(), "name": node.Name(), "version": v.Version, "embedding_commit": v.GitCommit})
	}
	proof := map[string]any{"replicas": 3, "parent_budget_seconds": 30, "acknowledged_witness": 822, "captured_cutoff": cutoff, "visited": visits, "weak_visited": weak, "missing_tail_visited": incomplete, "missing_tail_error": missingErr.Error(), "peers": peers, "scope": "Real R3 invocation population scan with stale/empty metadata and omitted-tail rejection; not a workflow/fault/p99/full200/native matrix acceptance."}
	data, err := json.MarshalIndent(proof, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(directory, "terminal-cohort-proof.json"), append(data, '\n'), 0600); err != nil {
		t.Fatal(err)
	}
	t.Logf("terminal captured=%d stale/empty visits=%v weak=%d missing=%d rejected=%v", cutoff, visits, weak, incomplete, missingErr)
}
