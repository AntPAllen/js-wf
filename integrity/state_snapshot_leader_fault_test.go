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

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"js-wf/testcluster"
)

type stateSnapshotWatchAttempt struct {
	Records       int       `json:"records"`
	Complete      bool      `json:"complete"`
	Started       time.Time `json:"started"`
	CreationError string    `json:"creation_error"`
	ReadError     string    `json:"read_error"`
}
type stateSnapshotLeaderFault struct {
	cluster        *testcluster.DockerCluster
	stream         jetstream.Stream
	attempts       []*stateSnapshotWatchAttempt
	Triggered      bool                               `json:"triggered"`
	Consumer       *jetstream.ConsumerInfo            `json:"consumer"`
	Node           int                                `json:"node"`
	Kill           *testcluster.DockerKillObservation `json:"kill"`
	Root           string                             `json:"-"`
	InjectionError string                             `json:"injection_error"`
}
type stateSnapshotFaultKV struct {
	jetstream.KeyValue
	fault *stateSnapshotLeaderFault
}

func (s stateSnapshotFaultKV) WatchAll(ctx context.Context, opts ...jetstream.WatchOpt) (jetstream.KeyWatcher, error) {
	attempt := &stateSnapshotWatchAttempt{Started: time.Now().UTC()}
	s.fault.attempts = append(s.fault.attempts, attempt)
	prior := map[string]bool{}
	if !s.fault.Triggered {
		listed := s.fault.stream.ListConsumers(ctx)
		for info := range listed.Info() {
			prior[info.Name] = true
		}
		if err := listed.Err(); err != nil {
			attempt.CreationError = err.Error()
			return nil, err
		}
	}
	native, err := s.KeyValue.WatchAll(ctx, opts...)
	if err != nil {
		attempt.CreationError = err.Error()
		return nil, err
	}
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
					logs, err := s.fault.cluster.Logs(node)
					if err != nil {
						s.fault.InjectionError = err.Error()
						return
					}
					if err := os.WriteFile(filepath.Join(s.fault.Root, fmt.Sprintf("server-%d-before-kill.log", node)), []byte(logs), 0644); err != nil {
						s.fault.InjectionError = err.Error()
						return
					}
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
	ctx, js, kv, cluster, nc, root := stateSnapshotCopiedFixture(t)
	stream, err := js.Stream(ctx, "KV_WF_STATE")
	if err != nil {
		t.Fatal(err)
	}
	fault := &stateSnapshotLeaderFault{cluster: cluster, stream: stream, Node: -1, Root: root}
	call, stop := context.WithTimeout(ctx, 20*time.Second)
	defer stop()
	began := time.Now()
	result, err := auditRead(call, func(attempt context.Context) (jetstream.KeyValue, error) {
		value, readErr := initialAuditState(attempt, stateSnapshotFaultKV{KeyValue: kv, fault: fault}, func(string) bool { return true })
		fault.attempts[len(fault.attempts)-1].ReadError = fmt.Sprint(readErr)
		return value, readErr
	})
	elapsed := time.Since(began)
	values := 0
	if snapshot, ok := result.(*auditStateSnapshot); ok {
		values = len(snapshot.values)
	}
	client := struct {
		Status       nats.Status `json:"status"`
		ConnectedURL string      `json:"connected_url"`
		Servers      []string    `json:"servers"`
		Discovered   []string    `json:"discovered"`
		Policy       string      `json:"policy"`
	}{nc.Status(), nc.ConnectedUrl(), nc.Servers(), nc.DiscoveredServers(), "IgnoreDiscoveredServers/dial1s/reconnect100ms"}
	clientData, _ := json.MarshalIndent(client, "", "  ")
	if err := os.WriteFile(filepath.Join(root, "client-after-snapshot.json"), append(clientData, '\n'), 0644); err != nil {
		t.Fatal(err)
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

// Inject the existing observed watch-consumer leader kill while the full
// checkpoint3140 cohort's journal and initial state set are read concurrently.
type concurrentSnapshotFaultJS struct {
	jetstream.JetStream
	state jetstream.KeyValue
}

func (j concurrentSnapshotFaultJS) KeyValue(ctx context.Context, name string) (jetstream.KeyValue, error) {
	if name == "WF_STATE" {
		return j.state, nil
	}
	return j.JetStream.KeyValue(ctx, name)
}

func TestConcurrentRetainedAuditCopiedStoreStateLeaderLoss(t *testing.T) {
	ctx, js, kv, cluster, _, root := stateSnapshotCopiedFixture(t)
	stream, err := js.Stream(ctx, "KV_WF_STATE")
	if err != nil {
		t.Fatal(err)
	}
	fault := &stateSnapshotLeaderFault{cluster: cluster, stream: stream, Node: -1, Root: root}
	call, stop := context.WithTimeout(ctx, 20*time.Second)
	began := time.Now()
	report, err := CheckThroughInvocationSequenceWithConcurrentStreamingStateReads(call, concurrentSnapshotFaultJS{JetStream: js, state: stateSnapshotFaultKV{KeyValue: kv, fault: fault}}, 87920)
	elapsed := time.Since(began)
	stop()
	proof := struct {
		Fault     *stateSnapshotLeaderFault    `json:"fault"`
		Attempts  []*stateSnapshotWatchAttempt `json:"attempts"`
		Report    Report                       `json:"report"`
		ElapsedNS int64                        `json:"elapsed_ns"`
		Error     string                       `json:"error"`
	}{fault, fault.attempts, report, int64(elapsed), fmt.Sprint(err)}
	data, writeErr := json.MarshalIndent(proof, "", "  ")
	if writeErr != nil {
		t.Fatal(writeErr)
	}
	if writeErr := os.WriteFile(filepath.Join(root, "state-watch-leader-loss.json"), append(data, '\n'), 0600); writeErr != nil {
		t.Fatal(writeErr)
	}
	t.Logf("concurrent copied cohort state-leader loss elapsed=%s report=%+v attempts=%d fault=%+v err=%v", elapsed, report, len(fault.attempts), fault, err)
	if !fault.Triggered || fault.Consumer == nil || fault.Kill == nil || fault.InjectionError != "" || err != nil || elapsed >= 20*time.Second || report != (Report{Invocations: 87920, Journals: 87920, Entries: 969925, Terminal: 87920}) {
		t.Fatalf("original-budget concurrent copied fault audit did not qualify: %+v", proof)
	}
}
