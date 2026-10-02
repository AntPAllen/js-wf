//go:build linux

package testcluster

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
)

// DockerCluster runs NATS nodes with separate network namespaces and file
// stores. It is opt-in: callers must have a working Docker daemon.
type DockerCluster struct {
	root          string
	image         string
	network       string
	clientNetwork string
	names         []string
	routeNames    []string
	urls          []string
	monitorURLs   []string
	stores        []string
	serverTags    map[int][]string
	syncInterval  string
	skewNode      int
	skewSeconds   int64
	oldNode       int
	clockAdvance  time.Duration
	clockAdvanced bool
}

func dockerCommand(ctx context.Context, args ...string) (string, error) {
	command := exec.CommandContext(ctx, "docker", args...)
	output, err := command.CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("docker %s: %w: %s", strings.Join(args, " "), err, output)
	}
	return strings.TrimSpace(string(output)), nil
}

// StartDockerCluster builds this module's pinned nats-server in a scratch
// image. Each node routes to node zero; discovered routes form the cluster.
func StartDockerCluster(root string, count int) (_ *DockerCluster, err error) {
	return startDockerCluster(root, count, "", 0, nil, nil)
}

// StartDockerClusterWithStores binds existing caller-owned directories to the
// specified nodes. Other nodes keep their ordinary root/node-N stores. The
// caller must keep these filesystems mounted until after Close; restarts reuse
// the same bindings. This supports private device-mapper fault fixtures.
func StartDockerClusterWithStores(root string, count int, stores map[int]string) (*DockerCluster, error) {
	return startDockerCluster(root, count, "", 0, stores, nil)
}

// StartMixedVersionDockerCluster starts node zero on oldBinary while the other
// nodes use the module-pinned server. UpgradeNode replaces node zero in place.
func StartMixedVersionDockerCluster(root string, count int, oldBinary string) (*DockerCluster, error) {
	if oldBinary == "" {
		return nil, fmt.Errorf("old server binary is required")
	}
	return startDockerCluster(root, count, oldBinary, 0, nil, nil)
}

// StartAdvancingClockDockerCluster prepares an initially unshifted cluster
// and a separately built future-clock binary for a later full stopped restart.
func StartAdvancingClockDockerCluster(root string, count int, advance time.Duration) (*DockerCluster, error) {
	if advance < 24*time.Hour || advance > 31*24*time.Hour || advance%time.Second != 0 {
		return nil, fmt.Errorf("advance must be whole seconds within 1..31 days")
	}
	if os.Getenv("WF_TIER3_SERVER_SKEW") != "" {
		return nil, fmt.Errorf("clock advance cannot be combined with a skewed peer")
	}
	return startDockerCluster(root, count, "", advance, nil, nil)
}

// StartDockerClusterWithStoresAndTags copies documented server placement tags.
// Restarts keep the same per-node configuration; callers select unique tags for
// independent probes. Existing constructors retain their untagged configuration.
func StartDockerClusterWithStoresAndTags(root string, count int, stores map[int]string, tags map[int][]string) (*DockerCluster, error) {
	return startDockerCluster(root, count, "", 0, stores, tags)
}

func copyDockerServerTags(count int, tags map[int][]string) (map[int][]string, error) {
	out := make(map[int][]string, len(tags))
	for node, values := range tags {
		if node < 0 || node >= count {
			return nil, fmt.Errorf("server tag node %d out of range", node)
		}
		seen := map[string]bool{}
		for _, value := range values {
			if value == "" || strings.TrimSpace(value) != value || strings.ContainsAny(value, "\r\n") || seen[value] {
				return nil, fmt.Errorf("invalid or duplicate server tag")
			}
			seen[value] = true
		}
		out[node] = append([]string(nil), values...)
	}
	return out, nil
}

func (c *DockerCluster) serverConfig(i int) (string, error) {
	path := filepath.Join(c.root, "nats.conf")
	if len(c.serverTags[i]) == 0 {
		return path, nil
	}
	base, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	tags, err := json.Marshal(c.serverTags[i])
	if err != nil {
		return "", err
	}
	path = filepath.Join(c.root, fmt.Sprintf("node-%d.conf", i))
	data := append(append(base, '\n'), []byte("server_tags: "+string(tags)+"\n")...)
	if err := os.WriteFile(path, data, 0644); err != nil {
		return "", err
	}
	return path, nil
}

func startDockerCluster(root string, count int, oldBinary string, advance time.Duration, overrides map[int]string, tags map[int][]string) (_ *DockerCluster, err error) {
	if count < 3 || count > 5 {
		return nil, fmt.Errorf("docker cluster count must be 3..5")
	}
	serverTags, err := copyDockerServerTags(count, tags)
	if err != nil {
		return nil, err
	}
	stores, err := dockerStorePaths(root, count, overrides)
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(root, 0755); err != nil {
		return nil, err
	}
	c := &DockerCluster{root: root, stores: stores, serverTags: serverTags, names: make([]string, count), routeNames: make([]string, count), urls: make([]string, count), monitorURLs: make([]string, count), skewNode: -1, oldNode: -1, clockAdvance: advance}
	if oldBinary != "" {
		c.oldNode = 0
		data, readErr := os.ReadFile(oldBinary)
		if readErr != nil {
			return nil, fmt.Errorf("read old server binary: %w", readErr)
		}
		if writeErr := os.WriteFile(filepath.Join(root, "nats-server-old"), data, 0755); writeErr != nil {
			return nil, fmt.Errorf("copy old server binary: %w", writeErr)
		}
	}
	if specification := os.Getenv("WF_TIER3_SERVER_SKEW"); specification != "" {
		node, offset, found := strings.Cut(specification, ":")
		if !found {
			return nil, fmt.Errorf("invalid WF_TIER3_SERVER_SKEW %q: want node:duration", specification)
		}
		c.skewNode, err = strconv.Atoi(node)
		if err != nil || c.skewNode < 0 || c.skewNode >= count {
			return nil, fmt.Errorf("invalid WF_TIER3_SERVER_SKEW node %q", node)
		}
		duration, parseErr := time.ParseDuration(offset)
		if parseErr != nil || duration == 0 || duration%time.Second != 0 || duration < -60*time.Second || duration > 60*time.Second {
			return nil, fmt.Errorf("invalid WF_TIER3_SERVER_SKEW duration %q: want whole seconds within ±60s", offset)
		}
		c.skewSeconds = int64(duration / time.Second)
		if c.skewNode == c.oldNode {
			return nil, fmt.Errorf("old server node %d cannot also use the skewed binary", c.oldNode)
		}
	}
	c.syncInterval = os.Getenv("WF_TIER3_SYNC_INTERVAL")
	if c.syncInterval == "" {
		c.syncInterval = "2m" // pinned nats-server v2.15.0 file-store default
	}
	if c.syncInterval != "always" {
		duration, parseErr := time.ParseDuration(c.syncInterval)
		if parseErr != nil || duration <= 0 {
			return nil, fmt.Errorf("invalid WF_TIER3_SYNC_INTERVAL %q", c.syncInterval)
		}
	}
	defer func() {
		if err != nil {
			c.Close()
		}
	}()
	suffix := fmt.Sprintf("%d-%d", os.Getpid(), time.Now().UnixNano())
	c.image, c.network, c.clientNetwork = "js-wf-nats-"+suffix, "js-wf-route-"+suffix, "js-wf-client-"+suffix
	binary := filepath.Join(root, "nats-server")
	build := exec.Command("go", "build", "-o", binary, "github.com/nats-io/nats-server/v2")
	build.Env = append(os.Environ(), "CGO_ENABLED=0")
	if output, buildErr := build.CombinedOutput(); buildErr != nil {
		return nil, fmt.Errorf("build Docker nats-server: %w: %s", buildErr, output)
	}
	dockerfile := "FROM scratch\nCOPY nats-server /nats-server\n"
	if c.oldNode >= 0 {
		dockerfile += "COPY nats-server-old /nats-server-old\n"
	}
	if c.skewNode >= 0 {
		overlayPath, overlayErr := WriteClockOverlay(root, time.Duration(c.skewSeconds)*time.Second)
		if overlayErr != nil {
			return nil, overlayErr
		}
		skewBuild := exec.Command("go", "build", "-overlay="+overlayPath, "-o", filepath.Join(root, "nats-server-skewed"), "github.com/nats-io/nats-server/v2")
		skewBuild.Env = append(os.Environ(), "CGO_ENABLED=0")
		if output, buildErr := skewBuild.CombinedOutput(); buildErr != nil {
			return nil, fmt.Errorf("build skewed Docker nats-server: %w: %s", buildErr, output)
		}
		dockerfile += "COPY nats-server-skewed /nats-server-skewed\n"
	}
	if c.clockAdvance != 0 {
		overlayPath, err := WriteAdvancedClockOverlay(filepath.Join(root, "advanced-clock"), c.clockAdvance)
		if err != nil {
			return nil, err
		}
		advanceBuild := exec.Command("go", "build", "-overlay="+overlayPath, "-o", filepath.Join(root, "nats-server-advanced"), "github.com/nats-io/nats-server/v2")
		advanceBuild.Env = append(os.Environ(), "CGO_ENABLED=0")
		if output, err := advanceBuild.CombinedOutput(); err != nil {
			return nil, fmt.Errorf("build advanced-clock server: %w: %s", err, output)
		}
		dockerfile += "COPY nats-server-advanced /nats-server-advanced\n"
	}
	dockerfile += "ENTRYPOINT [\"/nats-server\"]\n"
	if err := os.WriteFile(filepath.Join(root, "Dockerfile"), []byte(dockerfile), 0644); err != nil {
		return nil, err
	}
	config := fmt.Sprintf("jetstream: { sync_interval: %q }\n", c.syncInterval)
	if value := os.Getenv("WF_TIER3_ROUTE_PING_INTERVAL"); value != "" {
		interval, err := time.ParseDuration(value)
		if err != nil || interval <= 0 || interval > 30*time.Second {
			return nil, fmt.Errorf("invalid WF_TIER3_ROUTE_PING_INTERVAL %q", value)
		}
		config += fmt.Sprintf("cluster: { ping_interval: %q }\n", value)
	}
	if err := os.WriteFile(filepath.Join(root, "nats.conf"), []byte(config), 0644); err != nil {
		return nil, err
	}
	commandCtx, stop := context.WithTimeout(context.Background(), 2*time.Minute)
	defer stop()
	if _, err := dockerCommand(commandCtx, "build", "-q", "-t", c.image, root); err != nil {
		return nil, err
	}
	if _, err := dockerCommand(commandCtx, "network", "create", c.clientNetwork); err != nil {
		return nil, err
	}
	if _, err := dockerCommand(commandCtx, "network", "create", c.network); err != nil {
		return nil, err
	}
	for i := range c.names {
		c.names[i] = fmt.Sprintf("%s-n%d", c.network, i)
		c.routeNames[i] = fmt.Sprintf("%s-route-n%d", c.network, i)
	}
	for i := range c.names {
		if err := c.RestartNode(i); err != nil {
			return nil, err
		}
	}
	return c, nil
}

func (c *DockerCluster) ClientURL(i int) string { return c.urls[i] }

func (c *DockerCluster) MonitorURL(i int) string { return c.monitorURLs[i] }

func (c *DockerCluster) SyncInterval() string { return c.syncInterval }

func (c *DockerCluster) ClockSkew() (node int, offset time.Duration) {
	return c.skewNode, time.Duration(c.skewSeconds) * time.Second
}

func (c *DockerCluster) NodeName(i int) string {
	if i < 0 || i >= len(c.names) {
		return ""
	}
	return c.names[i]
}

// AdvanceStoppedClusterClock selects the future-clock binary for all peers.
// Every container must already be removed so no mixed clock cluster is exposed.
func (c *DockerCluster) AdvanceStoppedClusterClock() error {
	if c.clockAdvance == 0 || c.clockAdvanced {
		return fmt.Errorf("cluster clock advance is unavailable or already applied")
	}
	ctx, done := context.WithTimeout(context.Background(), 15*time.Second)
	defer done()
	for _, name := range c.names {
		existing, err := dockerCommand(ctx, "ps", "-a", "--filter", "name=^/"+name+"$", "--format", "{{.Names}}")
		if err != nil {
			return err
		}
		if existing != "" {
			return fmt.Errorf("clock advance requires stopped container %s", name)
		}
	}
	c.clockAdvanced = true
	return nil
}

func (c *DockerCluster) RestartNode(i int) error {
	if i < 0 || i >= len(c.names) || c.names[i] == "" {
		return fmt.Errorf("invalid Docker node %d", i)
	}
	store := c.stores[i]
	if err := os.MkdirAll(store, 0755); err != nil {
		return err
	}
	configPath, err := c.serverConfig(i)
	if err != nil {
		return err
	}
	serverArgs := []string{"-c", "/etc/nats.conf", "-a", "0.0.0.0", "-p", "4222", "-m", "8222", "-n", c.names[i], "-js", "-sd", "/data", "-cluster_name", c.network, "-cluster", "nats://0.0.0.0:6222"}
	peer := 0
	if i == 0 {
		peer = 1
	}
	// Explicitly advertise the route-only alias. With two container networks,
	// default interface discovery can advertise the client bridge or loopback,
	// allowing routes to survive DisconnectNode's route-network cut.
	serverArgs = append(serverArgs, "-cluster_advertise", c.routeNames[i]+":6222")
	serverArgs = append(serverArgs, "-routes", "nats://"+c.routeNames[peer]+":6222")
	// Retain published ports across replacement containers. Long-lived clients
	// must be able to reconnect after every original peer has been restarted.
	clientBinding, monitorBinding := "127.0.0.1::4222", "127.0.0.1::8222"
	if c.urls[i] != "" {
		clientBinding = strings.TrimPrefix(c.urls[i], "nats://") + ":4222"
	}
	if c.monitorURLs[i] != "" {
		monitorBinding = strings.TrimPrefix(c.monitorURLs[i], "http://") + ":8222"
	}
	args := []string{"run", "-d", "--rm", "--name", c.names[i], "--network", c.clientNetwork, "--user", fmt.Sprintf("%d:%d", os.Getuid(), os.Getgid()), "-p", clientBinding, "-p", monitorBinding, "-v", store + ":/data", "-v", configPath + ":/etc/nats.conf:ro"}
	if i == c.oldNode {
		args = append(args, "--entrypoint", "/nats-server-old")
	} else if c.clockAdvanced {
		args = append(args, "--entrypoint", "/nats-server-advanced")
	} else if i == c.skewNode {
		args = append(args, "--entrypoint", "/nats-server-skewed")
	}
	args = append(args, c.image)
	args = append(args, serverArgs...)
	ctx, stop := context.WithTimeout(context.Background(), 30*time.Second)
	defer stop()
	if _, err := dockerCommand(ctx, args...); err != nil {
		return err
	}
	if _, err := dockerCommand(ctx, "network", "connect", "--alias", c.routeNames[i], c.network, c.names[i]); err != nil {
		return err
	}
	port, err := dockerCommand(ctx, "port", c.names[i], "4222/tcp")
	if err != nil {
		return err
	}
	address := strings.TrimSpace(strings.Split(port, "\n")[0])
	if _, err := strconv.Atoi(strings.TrimPrefix(address, "127.0.0.1:")); err != nil || !strings.HasPrefix(address, "127.0.0.1:") {
		return fmt.Errorf("invalid Docker client port %q", address)
	}
	c.urls[i] = "nats://" + address
	monitorPort, err := dockerCommand(ctx, "port", c.names[i], "8222/tcp")
	if err != nil {
		return err
	}
	c.monitorURLs[i] = "http://" + strings.TrimSpace(strings.Split(monitorPort, "\n")[0])
	readyUntil := time.Now().Add(15 * time.Second)
	for time.Now().Before(readyUntil) {
		nc, dialErr := nats.Connect(c.urls[i], nats.Timeout(time.Second), nats.NoReconnect())
		if dialErr == nil {
			nc.Close()
			return nil
		}
		time.Sleep(100 * time.Millisecond)
	}
	logs, _ := c.Logs(i)
	return fmt.Errorf("Docker node %d did not accept clients: %s", i, logs)
}

// UpgradeNode restarts the old node with the current image binary and its
// existing file store. It is valid only for a mixed-version cluster.
func (c *DockerCluster) UpgradeNode(i int) error {
	if i != c.oldNode || i < 0 {
		return fmt.Errorf("Docker node %d is not the old-version node", i)
	}
	if err := c.KillNode(i); err != nil {
		return err
	}
	c.oldNode = -1
	return c.RestartNode(i)
}

// ServerNow reads the clock of one actual NATS process from its monitoring
// endpoint. Clock-skew tests must check this before claiming a skewed run.
func (c *DockerCluster) ServerNow(ctx context.Context, i int) (time.Time, error) {
	if i < 0 || i >= len(c.names) || c.monitorURLs[i] == "" {
		return time.Time{}, fmt.Errorf("invalid Docker node %d", i)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.monitorURLs[i]+"/varz", nil)
	if err != nil {
		return time.Time{}, err
	}
	response, err := http.DefaultClient.Do(req)
	if err != nil {
		return time.Time{}, err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return time.Time{}, fmt.Errorf("node %d varz status %d", i, response.StatusCode)
	}
	var data struct {
		Now time.Time `json:"now"`
	}
	if err := json.NewDecoder(response.Body).Decode(&data); err != nil {
		return time.Time{}, err
	}
	if data.Now.IsZero() {
		return time.Time{}, fmt.Errorf("node %d varz omitted server time", i)
	}
	return data.Now, nil
}

// Diagnostic reads public server monitoring independently of the NATS client.
// Callers supply a bounded context; large responses cannot exhaust memory.
func (c *DockerCluster) Diagnostic(ctx context.Context, node int, kind string) ([]byte, error) {
	if node < 0 || node >= len(c.monitorURLs) {
		return nil, fmt.Errorf("invalid diagnostic node %d", node)
	}
	var path string
	switch kind {
	case "connections":
		path = "/connz?subs=detail"
	case "jetstream":
		path = "/jsz?accounts=true&streams=true&consumers=true&raft=true"
	case "routes":
		path = "/routez"
	default:
		return nil, fmt.Errorf("unsupported Docker diagnostic %q", kind)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.monitorURLs[node]+path, nil)
	if err != nil {
		return nil, err
	}
	response, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("server %s diagnostic: HTTP %d", kind, response.StatusCode)
	}
	const limit = 16 << 20
	data, err := io.ReadAll(io.LimitReader(response.Body, limit+1))
	if len(data) > limit {
		return nil, fmt.Errorf("server %s diagnostic exceeds %d bytes", kind, limit)
	}
	return data, err
}

// RouteCount reads the server's monitoring endpoint through its pinned host port.
func (c *DockerCluster) RouteCount(ctx context.Context, i int) (int, error) {
	if i < 0 || i >= len(c.names) || c.monitorURLs[i] == "" {
		return 0, fmt.Errorf("invalid Docker node %d", i)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.monitorURLs[i]+"/routez", nil)
	if err != nil {
		return 0, err
	}
	response, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("node %d routez status %d", i, response.StatusCode)
	}
	var data struct {
		NumRoutes int `json:"num_routes"`
	}
	if err := json.NewDecoder(response.Body).Decode(&data); err != nil {
		return 0, err
	}
	return data.NumRoutes, nil
}

func (c *DockerCluster) DisconnectNode(i int) error {
	if i < 0 || i >= len(c.names) {
		return fmt.Errorf("invalid Docker node %d", i)
	}
	ctx, stop := context.WithTimeout(context.Background(), 15*time.Second)
	defer stop()
	_, err := dockerCommand(ctx, "network", "disconnect", "-f", c.network, c.names[i])
	return err
}

func (c *DockerCluster) ConnectNode(i int) error {
	if i < 0 || i >= len(c.names) {
		return fmt.Errorf("invalid Docker node %d", i)
	}
	ctx, stop := context.WithTimeout(context.Background(), 15*time.Second)
	defer stop()
	_, err := dockerCommand(ctx, "network", "connect", "--alias", c.routeNames[i], c.network, c.names[i])
	return err
}

// DockerKillObservation records controller-clock upper bounds, never daemon time.
// SourceStopped confirms exit or absence; CleanupComplete permits name reuse.
type DockerKillObservation struct {
	Node            int       `json:"node"`
	Container       string    `json:"container"`
	KillStarted     time.Time `json:"kill_started"`
	KillReturned    time.Time `json:"kill_returned"`
	SourceStopped   time.Time `json:"source_stopped"`
	CleanupComplete time.Time `json:"cleanup_complete"`
	State           string    `json:"state"`
}

func (c *DockerCluster) KillNode(i int) error {
	_, err := c.KillNodeObserved(i)
	return err
}

func (c *DockerCluster) KillNodeObserved(i int) (DockerKillObservation, error) {
	if i < 0 || i >= len(c.names) {
		return DockerKillObservation{}, fmt.Errorf("invalid Docker node %d", i)
	}
	ctx, stop := context.WithTimeout(context.Background(), 15*time.Second)
	defer stop()
	return observeDockerKill(ctx, i, c.names[i], dockerCommand)
}

func observeDockerKill(ctx context.Context, node int, name string, command func(context.Context, ...string) (string, error)) (DockerKillObservation, error) {
	receipt := DockerKillObservation{Node: node, Container: name, KillStarted: time.Now().UTC()}
	if _, err := command(ctx, "kill", "--signal=SIGKILL", name); err != nil {
		return receipt, err
	}
	receipt.KillReturned = time.Now().UTC()
	// A successful exact-name listing proves either stopped state or absence.
	// Name cleanup may finish later. Unknown/removing/running states cannot
	// establish source exit, and no server-clock FinishedAt is used.
	for ctx.Err() == nil {
		state, err := command(ctx, "ps", "-a", "--filter", "name=^/"+name+"$", "--format", "{{.State}}")
		if err != nil {
			return receipt, err
		}
		observed := time.Now().UTC()
		if receipt.SourceStopped.IsZero() && (state == "" || state == "exited" || state == "dead") {
			receipt.SourceStopped, receipt.State = observed, state
			if state == "" {
				receipt.State = "absent"
			}
		}
		if state == "" {
			receipt.CleanupComplete = observed
			return receipt, nil
		}
		select {
		case <-ctx.Done():
		case <-time.After(100 * time.Millisecond):
		}
	}
	return receipt, fmt.Errorf("Docker node %d was not removed after kill: %w", node, ctx.Err())
}

func (c *DockerCluster) PauseNode(i int) error {
	return c.nodeCommand(i, "pause")
}

func (c *DockerCluster) UnpauseNode(i int) error {
	return c.nodeCommand(i, "unpause")
}

func (c *DockerCluster) nodeCommand(i int, action string) error {
	if i < 0 || i >= len(c.names) || c.names[i] == "" {
		return fmt.Errorf("invalid Docker node %d", i)
	}
	ctx, stop := context.WithTimeout(context.Background(), 15*time.Second)
	defer stop()
	_, err := dockerCommand(ctx, action, c.names[i])
	return err
}

func (c *DockerCluster) Logs(i int) (string, error) {
	if i < 0 || i >= len(c.names) {
		return "", fmt.Errorf("invalid Docker node %d", i)
	}
	ctx, stop := context.WithTimeout(context.Background(), 10*time.Second)
	defer stop()
	return dockerCommand(ctx, "logs", c.names[i])
}

func (c *DockerCluster) Close() {
	ctx, stop := context.WithTimeout(context.Background(), 30*time.Second)
	defer stop()
	for _, name := range c.names {
		if name != "" {
			_, _ = dockerCommand(ctx, "rm", "-f", name)
		}
	}
	if c.network != "" {
		_, _ = dockerCommand(ctx, "network", "rm", c.network)
	}
	if c.clientNetwork != "" {
		_, _ = dockerCommand(ctx, "network", "rm", c.clientNetwork)
	}
	if c.image != "" {
		_, _ = dockerCommand(ctx, "image", "rm", "-f", c.image)
	}
}
