//go:build linux

package integrity

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"js-wf/testcluster"
)

type stateSnapshotWatchAttempt struct {
	Records  int  `json:"records"`
	Complete bool `json:"complete"`
}
type stateSnapshotLeaderFault struct {
	cluster        *testcluster.DockerCluster
	stream         jetstream.Stream
	attempts       []*stateSnapshotWatchAttempt
	Triggered      bool                               `json:"triggered"`
	Consumer       *jetstream.ConsumerInfo            `json:"consumer"`
	Node           int                                `json:"node"`
	Kill           *testcluster.DockerKillObservation `json:"kill"`
	InjectionError string                             `json:"injection_error"`
}
type stateSnapshotFaultKV struct {
	jetstream.KeyValue
	fault *stateSnapshotLeaderFault
}

func (s stateSnapshotFaultKV) WatchAll(ctx context.Context, opts ...jetstream.WatchOpt) (jetstream.KeyWatcher, error) {
	prior := map[string]bool{}
	if !s.fault.Triggered {
		listed := s.fault.stream.ListConsumers(ctx)
		for info := range listed.Info() {
			prior[info.Name] = true
		}
		if err := listed.Err(); err != nil {
			return nil, err
		}
	}
	native, err := s.KeyValue.WatchAll(ctx, opts...)
	if err != nil {
		return nil, err
	}
	attempt := &stateSnapshotWatchAttempt{}
	s.fault.attempts = append(s.fault.attempts, attempt)
	relay := &faultRelayWatch{KeyWatcher: native, updates: make(chan jetstream.KeyValueEntry), done: make(chan struct{}), finished: make(chan struct{})}
	go func() {
		defer close(relay.finished)
		defer close(relay.updates)
		for {
			select {
			case <-ctx.Done():
				return
			case <-relay.done:
				return
			case entry, ok := <-native.Updates():
				if !ok {
					return
				}
				if entry != nil {
					attempt.Records++
				} else {
					attempt.Complete = true
				}
				if !s.fault.Triggered && attempt.Records == 128 {
					s.fault.Triggered = true
					listed := s.fault.stream.ListConsumers(ctx)
					var current []*jetstream.ConsumerInfo
					for info := range listed.Info() {
						if !prior[info.Name] {
							current = append(current, info)
						}
					}
					if err := listed.Err(); err != nil {
						s.fault.InjectionError = err.Error()
						return
					}
					if len(current) != 1 || current[0].Cluster == nil || current[0].NumPending == 0 {
						s.fault.InjectionError = fmt.Sprintf("expected one new pending watch consumer, got %+v", current)
						return
					}
					s.fault.Consumer = current[0]
					leader := current[0].Cluster.Leader
					if strings.LastIndex(leader, "-n") < 0 {
						s.fault.InjectionError = "unknown watch leader " + leader
						return
					}
					node, err := strconv.Atoi(leader[strings.LastIndex(leader, "-n")+2:])
					if err != nil || node < 0 || node >= 5 {
						s.fault.InjectionError = "unknown watch leader " + leader
						return
					}
					s.fault.Node = node
					kill, err := s.fault.cluster.KillNodeObserved(node)
					if err != nil {
						s.fault.InjectionError = err.Error()
						return
					}
					s.fault.Kill = &kill
				}
				select {
				case <-ctx.Done():
					return
				case <-relay.done:
					return
				case relay.updates <- entry:
				}
				if entry == nil {
					return
				}
			}
		}
	}()
	return relay, nil
}

func TestRetainedStateSnapshotCopiedStoreLeaderLoss(t *testing.T) {
	ctx, js, kv, cluster, root := stateSnapshotCopiedFixture(t)
	stream, err := js.Stream(ctx, "KV_WF_STATE")
	if err != nil {
		t.Fatal(err)
	}
	fault := &stateSnapshotLeaderFault{cluster: cluster, stream: stream, Node: -1}
	call, stop := context.WithTimeout(ctx, 20*time.Second)
	defer stop()
	began := time.Now()
	result, err := auditRead(call, func(attempt context.Context) (jetstream.KeyValue, error) {
		return initialAuditState(attempt, stateSnapshotFaultKV{KeyValue: kv, fault: fault}, func(string) bool { return true })
	})
	elapsed := time.Since(began)
	values := 0
	if snapshot, ok := result.(*auditStateSnapshot); ok {
		values = len(snapshot.values)
	}
	proof := struct {
		Fault     *stateSnapshotLeaderFault    `json:"fault"`
		Attempts  []*stateSnapshotWatchAttempt `json:"attempts"`
		ElapsedNS int64                        `json:"elapsed_ns"`
		Values    int                          `json:"values"`
		Error     string                       `json:"error"`
	}{fault, fault.attempts, elapsed.Nanoseconds(), values, fmt.Sprint(err)}
	data, writeErr := json.MarshalIndent(proof, "", "  ")
	if writeErr != nil {
		t.Fatal(writeErr)
	}
	if writeErr := os.WriteFile(filepath.Join(root, "state-watch-leader-loss.json"), append(data, '\n'), 0644); writeErr != nil {
		t.Fatal(writeErr)
	}
	t.Logf("state-watch leader-loss elapsed=%s values=%d attempts=%+v fault=%+v err=%v", elapsed, values, fault.attempts, fault, err)
	if !fault.Triggered || fault.Consumer == nil || fault.Kill == nil || fault.InjectionError != "" || err != nil || elapsed >= 20*time.Second || values != 29177 {
		t.Fatalf("original-budget state snapshot did not qualify: %+v", proof)
	}
}
