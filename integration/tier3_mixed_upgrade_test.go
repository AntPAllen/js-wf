//go:build linux

package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"js-wf/provision"
	"js-wf/testcluster"
)

func fiveUpgradeShutdownMode() (string, error) {
	mode := os.Getenv("WF_TIER3_UPGRADE_SHUTDOWN")
	if mode == "" {
		mode = "sigkill"
	}
	if mode != "sigkill" && mode != "ldm" {
		return "", fmt.Errorf("invalid WF_TIER3_UPGRADE_SHUTDOWN %q", mode)
	}
	return mode, nil
}

func TestFiveContainerMixedRollingServerUpgrade(t *testing.T) {
	runFiveContainerMixedLeader(t, "rolling_upgrade")
}

type fiveUpgradePeer struct {
	Node           int                     `json:"node"`
	Version        string                  `json:"version"`
	ServerID       string                  `json:"server_id"`
	NativeRejected string                  `json:"native_rejected"`
	NativeCheck    *fiveUpgradeNativeCheck `json:"native_check,omitempty"`
}
type fiveUpgradeNativeCheck struct {
	Started   time.Time                   `json:"started"`
	Ended     time.Time                   `json:"ended"`
	Deadline  time.Time                   `json:"deadline"`
	Rejection string                      `json:"rejection,omitempty"`
	Error     string                      `json:"error,omitempty"`
	API       []fiveUpgradeAPIObservation `json:"api,omitempty"`
}
type fiveUpgradeBackendCheck struct {
	Started  time.Time                   `json:"started"`
	Ended    time.Time                   `json:"ended"`
	Deadline time.Time                   `json:"deadline"`
	Backend  provision.TimerBackend      `json:"backend"`
	Error    string                      `json:"error,omitempty"`
	API      []fiveUpgradeAPIObservation `json:"api,omitempty"`
}

type fiveUpgradeHealthObservation struct {
	Node   int       `json:"node"`
	URL    string    `json:"url"`
	Before time.Time `json:"before"`
	After  time.Time `json:"after"`
	Status int       `json:"http_status"`
	Body   string    `json:"body"`
	Error  string    `json:"error,omitempty"`
}

type fiveUpgradeStreamObservation struct {
	Name  string                `json:"name"`
	At    time.Time             `json:"at"`
	Info  *jetstream.StreamInfo `json:"info"`
	Error string                `json:"error,omitempty"`
}

type fiveUpgradeProof struct {
	Health    []fiveUpgradeHealthObservation `json:"health"`
	Readiness []fiveUpgradeStreamObservation `json:"readiness,omitempty"`

	Version      int                      `json:"version"`
	Started      time.Time                `json:"started"`
	Complete     bool                     `json:"complete"`
	Phase        string                   `json:"phase"`
	Error        string                   `json:"error,omitempty"`
	BackendCheck *fiveUpgradeBackendCheck `json:"backend_check,omitempty"`
	At           time.Time                `json:"at"`
	Nodes        []fiveUpgradePeer        `json:"nodes"`
	Backend      provision.TimerBackend   `json:"backend"`
	Run          *jetstream.StreamInfo    `json:"run_info"`
	Timer        *jetstream.StreamInfo    `json:"timer_info"`
}

func fiveUpgradeVersions(upgraded []bool) []string {
	versions := make([]string, 5)
	for node := range versions {
		versions[node] = "2.11.17"
		if upgraded[node] {
			versions[node] = server.VERSION
		}
	}
	return versions
}

// Pin every direct endpoint; reconnect/discovery cannot substitute a peer.
// Retained fallback selection must survive each binary change, and explicit
// native provisioning must fail with its semantic version/configuration error.
func proveFiveUpgradeDeployment(ctx context.Context, js jetstream.JetStream, cluster *testcluster.DockerCluster, versions []string, path string) (observed []string, resultErr error) {
	bound, stop := context.WithTimeout(ctx, 60*time.Second)
	defer stop()
	proof := fiveUpgradeProof{Version: 4, Started: time.Now().UTC(), Phase: "all-peer-health"}
	// Preserve partial phase/timing evidence on failure as well as success.
	defer func() {
		proof.At = time.Now().UTC()
		if resultErr != nil {
			proof.Error = resultErr.Error()
		}
		data, err := json.MarshalIndent(proof, "", "  ")
		if err == nil {
			err = os.WriteFile(path, data, 0644)
		}
		if err != nil {
			resultErr = errors.Join(resultErr, fmt.Errorf("upgrade proof artifact: %w", err))
		}
	}()
	health, healthErr := observeFiveUpgradeHealth(bound, cluster)
	proof.Health = health
	if healthErr != nil {
		return nil, healthErr
	}
	proof.Phase = "peer-native-rejections"
	seen := map[string]bool{}
	observed = make([]string, 5)
	for node := 0; node < 5; node++ {
		nc, err := nats.Connect(cluster.ClientURL(node), nats.NoReconnect(), nats.IgnoreDiscoveredServers(), nats.Timeout(time.Second))
		if err != nil {
			return nil, err
		}
		peer := fiveUpgradePeer{Node: node, Version: nc.ConnectedServerVersion(), ServerID: nc.ConnectedServerId()}
		if peer.Version != versions[node] || peer.ServerID == "" || seen[peer.ServerID] {
			nc.Close()
			return nil, fmt.Errorf("upgrade node%d version=%s want=%s identity=%s", node, peer.Version, versions[node], peer.ServerID)
		}
		seen[peer.ServerID] = true
		observed[node] = peer.Version
		peerJS, err := jetstream.New(nc)
		if err != nil {
			nc.Close()
			return nil, err
		}
		check, nativeErr := checkFiveUpgradeNativeProvisioning(bound, peer.Version, peerJS)
		nc.Close()
		peer.NativeCheck, peer.NativeRejected = &check, check.Rejection
		proof.Nodes = append(proof.Nodes, peer)
		if nativeErr != nil {
			return nil, fmt.Errorf("upgrade node%d native proof: %w", node, nativeErr)
		}
	}
	proof.Phase = "fallback-provisioning"
	fallbackJS := js
	var snapshotAPI func() []fiveUpgradeAPIObservation
	if os.Getenv("WF_TIER3_UPGRADE_API_TRACE") == "1" {
		var err error
		fallbackJS, snapshotAPI, err = newFiveUpgradeProvisioningTrace(js.Conn())
		if err != nil {
			return nil, fmt.Errorf("upgrade provisioning trace: %w", err)
		}
	}
	check, err := checkFiveUpgradeFallback(bound, func(operation context.Context) (provision.TimerBackend, error) {
		return provision.EnsureAuto(operation, fallbackJS, 5)
	})
	if snapshotAPI != nil {
		check.API = snapshotAPI()
	}
	proof.BackendCheck, proof.Backend = &check, check.Backend
	if err != nil {
		return nil, err
	}
	proof.Phase = "replica-readiness"
	if err := waitFiveReplicaReadiness(bound, js, 0); err != nil {
		return nil, err
	}
	for _, name := range []string{"WF_RUN", "WF_TIMER"} {
		info, err := waitFiveUpgradeStreamReadiness(bound, name, func(attempt context.Context) (*jetstream.StreamInfo, error) {
			return matrixReadMetadata(attempt, func(operation context.Context) (*jetstream.StreamInfo, error) {
				s, err := js.Stream(operation, name)
				if err != nil {
					return nil, err
				}
				return s.Info(operation)
			})
		}, func(info *jetstream.StreamInfo, err error) {
			observation := fiveUpgradeStreamObservation{Name: name, At: time.Now().UTC(), Info: info}
			if err != nil {
				observation.Error = err.Error()
			}
			proof.Readiness = append(proof.Readiness, observation)
		})
		if name == "WF_RUN" {
			proof.Run = info
		} else {
			proof.Timer = info
		}
		if err != nil {
			return nil, err
		}
	}
	proof.Phase, proof.Complete = "complete", true
	return observed, nil
}

// Default /healthz checks the peer's retained JetStream assets, not just TCP
// readiness. Retain every response; advance only after one round passes all5.
func observeFiveUpgradeHealth(ctx context.Context, cluster *testcluster.DockerCluster) ([]fiveUpgradeHealthObservation, error) {
	var observed []fiveUpgradeHealthObservation
	client := &http.Client{Timeout: 2 * time.Second}
	for ctx.Err() == nil {
		ready := true
		for node := 0; node < 5; node++ {
			event := fiveUpgradeHealthObservation{Node: node, URL: cluster.MonitorURL(node) + "/healthz?details=true", Before: time.Now().UTC()}
			request, err := http.NewRequestWithContext(ctx, http.MethodGet, event.URL, nil)
			if err == nil {
				var response *http.Response
				response, err = client.Do(request)
				if err == nil {
					event.Status = response.StatusCode
					var body []byte
					body, err = io.ReadAll(io.LimitReader(response.Body, 1<<20))
					response.Body.Close()
					event.Body = string(body)
					var health struct {
						Status string            `json:"status"`
						Error  string            `json:"error"`
						Errors []json.RawMessage `json:"errors"`
					}
					if err == nil {
						err = json.Unmarshal(body, &health)
					}
					if err == nil && (event.Status != http.StatusOK || health.Status != "ok" || health.Error != "" || len(health.Errors) != 0) {
						err = fmt.Errorf("peer health status=%d body=%s", event.Status, body)
					}
				}
			}
			event.After = time.Now().UTC()
			if err != nil {
				ready = false
				event.Error = err.Error()
			}
			observed = append(observed, event)
		}
		if ready {
			return observed, nil
		}
		select {
		case <-ctx.Done():
			return observed, ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
	return observed, ctx.Err()
}

// Retain the same default API behavior and whole proof context while tracing
// each pinned peer's native check. Partial traces survive a failed lookup.
func checkFiveUpgradeNativeProvisioning(ctx context.Context, version string, js jetstream.JetStream) (fiveUpgradeNativeCheck, error) {
	var snapshot func() []fiveUpgradeAPIObservation
	if os.Getenv("WF_TIER3_UPGRADE_API_TRACE") == "1" {
		var err error
		js, snapshot, err = newFiveUpgradeProvisioningTrace(js.Conn())
		if err != nil {
			return fiveUpgradeNativeCheck{}, fmt.Errorf("upgrade native provisioning trace: %w", err)
		}
	}
	check, err := checkFiveUpgradeNative(ctx, version, func(operation context.Context) error {
		return provision.Ensure(operation, js, 5)
	})
	if snapshot != nil {
		check.API = snapshot()
	}
	return check, err
}

// A semantic native rejection requires multiple stream reads on a new peer.
// Use the remaining whole proof deadline once, after recorded asset health.
func checkFiveUpgradeNative(ctx context.Context, version string, lookup func(context.Context) error) (fiveUpgradeNativeCheck, error) {
	check := fiveUpgradeNativeCheck{Started: time.Now().UTC()}
	check.Deadline, _ = ctx.Deadline()
	nativeErr := lookup(ctx)
	check.Ended = time.Now().UTC()
	want := "stream WF_RUN configuration mismatch:"
	if version == "2.11.17" {
		want = "native timers require NATS 2.12+; connected server reports 2.11.17"
	}
	if nativeErr == nil {
		check.Error = "native provisioning unexpectedly succeeded"
		return check, fmt.Errorf("%s; want=%s", check.Error, want)
	}
	if !strings.HasPrefix(nativeErr.Error(), want) {
		check.Error = nativeErr.Error()
		return check, fmt.Errorf("upgrade native rejection=%w want=%s", nativeErr, want)
	}
	check.Rejection = nativeErr.Error()
	return check, nil
}

func TestFiveUpgradeNativeUsesWholeProofBudget(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	check, err := checkFiveUpgradeNative(ctx, "2.15.0", func(operation context.Context) error {
		for stage := 0; stage < 2; stage++ {
			select {
			case <-operation.Done():
				return operation.Err()
			case <-time.After(1100 * time.Millisecond):
			}
		}
		return fmt.Errorf("stream WF_RUN configuration mismatch: retained fallback")
	})
	deadline, _ := ctx.Deadline()
	if err != nil || check.Rejection != "stream WF_RUN configuration mismatch: retained fallback" || check.Error != "" || !check.Deadline.Equal(deadline) || check.Ended.Sub(check.Started) < 2200*time.Millisecond {
		t.Fatalf("native-operation proof=%+v err=%v", check, err)
	}
}

func TestFiveUpgradeNativeRejectsUnavailableOrSuccessfulProvisioning(t *testing.T) {
	for _, failure := range []error{nil, context.DeadlineExceeded, context.Canceled, fmt.Errorf("unrelated configuration mismatch")} {
		check, err := checkFiveUpgradeNative(context.Background(), "2.15.0", func(context.Context) error { return failure })
		if err == nil || check.Rejection != "" || check.Error == "" {
			t.Fatalf("invalid native proof=%+v err=%v", check, err)
		}
		if failure == context.DeadlineExceeded && !errors.Is(err, context.DeadlineExceeded) {
			t.Fatal("deadline cause erased")
		}
	}
	for _, version := range []string{"2.11.17", "2.15.0"} {
		rejection := "stream WF_RUN configuration mismatch: retained fallback"
		if version == "2.11.17" {
			rejection = "native timers require NATS 2.12+; connected server reports 2.11.17"
		}
		check, err := checkFiveUpgradeNative(context.Background(), version, func(context.Context) error { return fmt.Errorf("%s", rejection) })
		if err != nil || check.Rejection != rejection {
			t.Fatalf("semantic rejection proof=%+v err=%v", check, err)
		}
	}
}

// EnsureAuto checks several retained stores. Its total budget is the existing
// deployment-proof deadline, not the single metadata-read retry helper's 2s.
func checkFiveUpgradeFallback(ctx context.Context, lookup func(context.Context) (provision.TimerBackend, error)) (fiveUpgradeBackendCheck, error) {
	check := fiveUpgradeBackendCheck{Started: time.Now().UTC()}
	check.Deadline, _ = ctx.Deadline()
	backend, err := lookup(ctx)
	check.Ended, check.Backend = time.Now().UTC(), backend
	if err != nil {
		check.Error = err.Error()
		return check, fmt.Errorf("upgrade fallback provisioning proof: %w", err)
	}
	if backend != provision.FallbackTimers {
		err = fmt.Errorf("upgrade fallback changed backend=%s", backend)
		check.Error = err.Error()
		return check, err
	}
	return check, nil
}

func TestFiveUpgradeProvisioningUsesWholeProofBudget(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	check, err := checkFiveUpgradeFallback(ctx, func(operation context.Context) (provision.TimerBackend, error) {
		// Two sequential stages exceed one metadata-read budget. Restoring the
		// previous helper around this whole operation cancels before stage two.
		for stage := 0; stage < 2; stage++ {
			select {
			case <-operation.Done():
				return "", operation.Err()
			case <-time.After(1100 * time.Millisecond):
			}
		}
		return provision.FallbackTimers, nil
	})
	deadline, _ := ctx.Deadline()
	if err != nil || check.Backend != provision.FallbackTimers || check.Error != "" || !check.Deadline.Equal(deadline) || check.Ended.Sub(check.Started) < 2200*time.Millisecond {
		t.Fatalf("whole-operation proof=%+v err=%v", check, err)
	}
}

func TestFiveUpgradeProvisioningPreservesCancellationAndBackendFailure(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	check, err := checkFiveUpgradeFallback(ctx, func(operation context.Context) (provision.TimerBackend, error) { return "", operation.Err() })
	if !errors.Is(err, context.Canceled) || check.Backend != "" || check.Error != context.Canceled.Error() {
		t.Fatalf("cancellation proof=%+v err=%v", check, err)
	}
	check, err = checkFiveUpgradeFallback(context.Background(), func(context.Context) (provision.TimerBackend, error) { return provision.NativeTimers, nil })
	if err == nil || check.Backend != provision.NativeTimers || check.Error == "" {
		t.Fatalf("changed backend proof=%+v err=%v", check, err)
	}
}

func upgradeFiveMixedServer(ctx context.Context, js jetstream.JetStream, cluster *testcluster.DockerCluster, node int, scheduled time.Time, prefix string, upgraded []bool) (matrixLeaderFault, error) {
	event := matrixLeaderFault{Scheduled: scheduled, Node: node}
	var err error
	event.VersionsBefore, err = proveFiveUpgradeDeployment(ctx, js, cluster, fiveUpgradeVersions(upgraded), prefix+"-upgrade-before.json")
	if err != nil {
		return event, err
	}
	event.Killed = time.Now().UTC()
	if upgraded[node] {
		return event, fmt.Errorf("upgrade node%d selected twice", node)
	}
	mode, err := fiveUpgradeShutdownMode()
	if err != nil {
		return event, err
	}
	event.UpgradeShutdownMode = mode
	if mode == "ldm" {
		proof, err := cluster.UpgradeNodeGracefully(ctx, node)
		event.GracefulUpgrade = &proof
		if err != nil {
			return event, err
		}
	} else if err := cluster.UpgradeNode(node); err != nil {
		return event, err
	}
	upgraded[node] = true
	event.VersionsAfter, err = proveFiveUpgradeDeployment(ctx, js, cluster, fiveUpgradeVersions(upgraded), prefix+"-upgrade-after.json")
	if err != nil {
		return event, err
	}
	event.Healed = time.Now().UTC()
	return event, nil
}

// Recheck dynamic replica state under the original whole-proof context. A prior
// readiness observation cannot guarantee that a later metadata read is current.
// Configuration errors remain immediate; replica catch-up gets no extra budget.
func waitFiveUpgradeStreamReadiness(ctx context.Context, name string,
	read func(context.Context) (*jetstream.StreamInfo, error), observe func(*jetstream.StreamInfo, error)) (*jetstream.StreamInfo, error) {
	var last *jetstream.StreamInfo
	for {
		if err := ctx.Err(); err != nil {
			return last, fmt.Errorf("upgrade %s replica readiness: %w", name, err)
		}
		info, err := read(ctx)
		observe(info, err)
		last = info
		if err != nil {
			return info, fmt.Errorf("upgrade %s metadata: %w", name, err)
		}
		if info == nil {
			return nil, fmt.Errorf("upgrade %s missing stream metadata", name)
		}
		if info.Config.Replicas != 5 || info.Config.Storage != jetstream.FileStorage {
			return info, fmt.Errorf("upgrade %s configuration mismatch: want five file replicas", name)
		}
		if name == "WF_RUN" && info.Config.AllowMsgSchedules {
			return info, fmt.Errorf("upgrade unexpectedly enabled native scheduling")
		}
		ready := info.Cluster != nil && info.Cluster.Leader != "" && len(info.Cluster.Replicas) == 4
		if ready {
			for _, replica := range info.Cluster.Replicas {
				if replica == nil || !replica.Current || replica.Offline {
					ready = false
					break
				}
			}
		}
		if ready {
			return info, nil
		}
		timer := time.NewTimer(200 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return info, fmt.Errorf("upgrade %s replica readiness: %w", name, ctx.Err())
		case <-timer.C:
		}
	}
}
