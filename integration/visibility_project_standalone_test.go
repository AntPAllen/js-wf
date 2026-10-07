package integration_test

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"debug/buildinfo"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"js-wf/testcluster"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

// The full case uses the real command binary; the SDK only drives the workload
// and reads the final projection. Each daemon owns a separate SQL application name.
type standaloneProjection struct {
	command   *exec.Cmd
	done      chan error
	joined    chan struct{}
	err       error
	output    *os.File
	record    map[string]any
	proxy     *testcluster.ClientProxy
	wireRoot  string
	wireSaved bool
}

func buildStandaloneProjection(t *testing.T, ctx context.Context, root string, proof map[string]any) string {
	t.Helper()
	parent, err := buildinfo.ReadFile(os.Args[0])
	if err != nil {
		t.Fatal(err)
	}
	revision := ""
	for _, setting := range parent.Settings {
		if setting.Key == "vcs.revision" {
			revision = setting.Value
		}
		if setting.Key == "vcs.modified" && setting.Value != "false" {
			t.Fatal("parent SDK requires clean VCS")
		}
		if setting.Key == "-race" && setting.Value == "true" {
			t.Fatal("full standalone projection proof requires original nonrace profile")
		}
	}
	if revision == "" {
		t.Fatal("parent SDK VCS revision missing")
	}
	module, err := exec.CommandContext(ctx, "go", "env", "GOMOD").Output()
	if err != nil {
		t.Fatal(err)
	}
	repo := filepath.Dir(strings.TrimSpace(string(module)))
	binary := filepath.Join(root, "wf-project")
	command := exec.CommandContext(ctx, "go", "build", "-p=1", "-buildvcs=true", "-o", binary, "./cmd/wf")
	command.Dir = repo
	output, err := command.CombinedOutput()
	if e := os.WriteFile(filepath.Join(root, "standalone-project-build.log"), output, 0600); e != nil {
		t.Fatal(e)
	}
	if err != nil {
		t.Fatalf("standalone project build: %v; %s", err, output)
	}
	info, err := buildinfo.ReadFile(binary)
	if err != nil {
		t.Fatal(err)
	}
	if info.Path != "js-wf/cmd/wf" {
		t.Fatalf("unexpected standalone program %q", info.Path)
	}
	actualRevision, modified := "", ""
	for _, setting := range info.Settings {
		if setting.Key == "vcs.revision" {
			actualRevision = setting.Value
		}
		if setting.Key == "vcs.modified" {
			modified = setting.Value
		}
		if setting.Key == "-race" && setting.Value == "true" {
			t.Fatal("standalone binary unexpectedly race-enabled")
		}
	}
	if actualRevision != revision || modified != "false" {
		t.Fatalf("standalone VCS %s/%s, parent %s", actualRevision, modified, revision)
	}
	data, err := os.ReadFile(binary)
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(data)
	proof["standalone_binary"] = map[string]any{"path": binary, "sha256": hex.EncodeToString(digest[:]), "source": revision, "package": info.Path, "build_info": info.String(), "build_command": command.Args, "working_directory": repo}
	return binary
}

func startStandaloneProjection(t *testing.T, ctx context.Context, db *sql.DB, binary, root, phase, natsURL, domain string, leafTransport ...bool) *standaloneProjection {
	t.Helper()
	leaf := len(leafTransport) > 0 && leafTransport[0]
	return startStandaloneProjectionReady(t, ctx, db, binary, root, phase, natsURL, domain, leaf, 0)
}

func startStandaloneProjectionReady(t *testing.T, ctx context.Context, db *sql.DB, binary, root, phase, natsURL, domain string, leaf bool, blocker int) *standaloneProjection {
	t.Helper()
	app := "wf-project-" + phase
	parsed, err := url.Parse(os.Getenv("WF_TEST_POSTGRES_DSN"))
	if err != nil {
		t.Fatal(err)
	}
	query := parsed.Query()
	query.Set("application_name", app)
	parsed.RawQuery = query.Encode()
	dsn := parsed.String()
	originalURL := natsURL
	var proxy *testcluster.ClientProxy
	wireRoot := ""
	if leaf {
		wireRoot = filepath.Join(root, "projector-"+phase+"-leaf-wire")
		if err = os.Mkdir(wireRoot, 0700); err != nil {
			t.Fatal(err)
		}
		proxy, err = testcluster.NewClientProxy(natsURL)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(proxy.Close)
		// Full50000 workload and rebuild traffic: retain complete frames on
		// disk, with an explicit512MiB encoded budget, not an in-memory prefix.
		if err = proxy.EnableTrafficFileTrace(filepath.Join(wireRoot, "traffic.frames.jsonl"), 512<<20); err != nil {
			t.Fatal(err)
		}
		natsURL = proxy.URL()
	}
	command := exec.CommandContext(ctx, binary, "-url", natsURL, "-domain", domain, "project")
	for _, entry := range os.Environ() {
		if !strings.HasPrefix(entry, "WF_POSTGRES_DSN=") {
			command.Env = append(command.Env, entry)
		}
	}
	command.Env = append(command.Env, "WF_POSTGRES_DSN="+dsn)
	output, err := os.Create(filepath.Join(root, "standalone-project-"+phase+".log"))
	if err != nil {
		t.Fatal(err)
	}
	command.Stdout, command.Stderr = output, output
	if err := command.Start(); err != nil {
		output.Close()
		t.Fatal(err)
	}
	process := &standaloneProjection{command: command, done: make(chan error, 1), joined: make(chan struct{}), output: output, proxy: proxy, wireRoot: wireRoot}
	go func() { process.err = command.Wait(); close(process.joined); process.done <- process.err }()
	t.Cleanup(func() { process.abort() })
	deadline := time.Now().Add(10 * time.Second)
	var backend int
	for {
		select {
		case <-process.joined:
			t.Fatalf("standalone %s exited before writer admission: %v", phase, process.err)
		default:
		}
		if blocker > 0 {
			err = db.QueryRowContext(ctx, `SELECT a.pid FROM pg_stat_activity a WHERE a.application_name=$1 AND a.wait_event_type='Lock' AND $2=ANY(pg_blocking_pids(a.pid)) AND a.query LIKE 'CREATE %'`, app, blocker).Scan(&backend)
		} else {
			err = db.QueryRowContext(ctx, `SELECT l.pid FROM pg_locks l JOIN pg_stat_activity a ON a.pid=l.pid WHERE l.locktype='advisory' AND l.mode='ExclusiveLock' AND l.granted AND a.application_name=$1 AND l.database=(SELECT oid FROM pg_database WHERE datname=current_database()) LIMIT 1`, app).Scan(&backend)
		}
		if err == nil {
			break
		}
		if !errors.Is(err, sql.ErrNoRows) {
			t.Fatal(err)
		}
		if time.Now().After(deadline) || ctx.Err() != nil {
			t.Fatalf("standalone %s writer readiness: %v", phase, err)
		}
		time.Sleep(20 * time.Millisecond)
	}
	proc := filepath.Join("/proc", fmt.Sprint(command.Process.Pid))
	executable, err := os.ReadFile(filepath.Join(proc, "exe"))
	if err != nil {
		t.Fatal(err)
	}
	expected, err := os.ReadFile(binary)
	if err != nil {
		t.Fatal(err)
	}
	actualHash, expectedHash := sha256.Sum256(executable), sha256.Sum256(expected)
	if actualHash != expectedHash {
		t.Fatal("standalone actual executable differs from built operator")
	}
	argv, err := os.ReadFile(filepath.Join(proc, "cmdline"))
	if err != nil {
		t.Fatal(err)
	}
	actualArgs := strings.Split(strings.TrimRight(string(argv), "\x00"), "\x00")
	if strings.Join(actualArgs, "\x00") != strings.Join(command.Args, "\x00") {
		t.Fatal("standalone actual argv mismatch")
	}
	stat, err := os.ReadFile(filepath.Join(proc, "stat"))
	if err != nil {
		t.Fatal(err)
	}
	environment, err := os.ReadFile(filepath.Join(proc, "environ"))
	if err != nil {
		t.Fatal(err)
	}
	actualDSN := ""
	for _, entry := range strings.Split(string(environment), "\x00") {
		if strings.HasPrefix(entry, "WF_POSTGRES_DSN=") {
			actualDSN = strings.TrimPrefix(entry, "WF_POSTGRES_DSN=")
		}
	}
	if actualDSN != dsn {
		t.Fatal("standalone SQL selection differs from admitted fixture")
	}
	actualEnvironment := map[string]string{}
	for _, entry := range strings.Split(string(environment), "\x00") {
		key, value, ok := strings.Cut(entry, "=")
		if ok && (key == "GOMAXPROCS" || key == "GOMEMLIMIT" || key == "GOWORK" || key == "GOFLAGS") {
			actualEnvironment[key] = value
		}
	}
	if actualEnvironment["GOMAXPROCS"] != "2" || actualEnvironment["GOMEMLIMIT"] != "2GiB" || actualEnvironment["GOWORK"] != "off" || actualEnvironment["GOFLAGS"] != "" {
		t.Fatalf("standalone actual profile: %v", actualEnvironment)
	}
	cwd, err := os.Readlink(filepath.Join(proc, "cwd"))
	if err != nil {
		t.Fatal(err)
	}
	dsnHash := sha256.Sum256([]byte(dsn))
	info, err := exec.Command("go", "version", "-m", filepath.Join(proc, "exe")).Output()
	if err != nil {
		t.Fatal(err)
	}
	process.record = map[string]any{"environment": actualEnvironment, "working_directory": cwd, "phase": phase, "pid": command.Process.Pid, "stat": string(stat), "argv": actualArgs, "sha256": hex.EncodeToString(actualHash[:]), "build_info": string(info), "postgres_dsn_sha256": hex.EncodeToString(dsnHash[:]), "application_name": app, "writer_backend_pid": backend, "admitted_at": time.Now().UTC(), "nats_target": originalURL, "nats_url": natsURL, "leaf_transport": proxy != nil}
	process.record["startup_blocker_backend_pid"] = blocker
	return process
}

func (p *standaloneProjection) abort() {
	select {
	case <-p.joined:
	default:
		_ = p.command.Process.Kill()
		<-p.joined
	}
	if p.proxy != nil {
		p.proxy.Close()
	}
	_ = p.output.Close()
}

func (p *standaloneProjection) recordExit(t *testing.T) map[string]any {
	t.Helper()
	<-p.joined
	p.record["exit_code"] = p.command.ProcessState.ExitCode()
	if status, ok := p.command.ProcessState.Sys().(syscall.WaitStatus); ok && status.Signaled() {
		p.record["exit_signal"] = status.Signal().String()
	}
	if p.err != nil {
		p.record["wait_error"] = p.err.Error()
	}
	p.record["reaped_at"] = time.Now().UTC()
	if p.proxy != nil && !p.wireSaved {
		p.proxy.Close()
		stats, trace := p.proxy.Stats(), p.proxy.TrafficTrace()
		for name, value := range map[string]any{"traffic.json": trace, "proxy-final.json": stats} {
			data, err := json.MarshalIndent(value, "", "  ")
			if err != nil {
				t.Fatal(err)
			}
			if err = os.WriteFile(filepath.Join(p.wireRoot, name), append(data, '\n'), 0600); err != nil {
				t.Fatal(err)
			}
		}
		p.wireSaved = true
		p.record["wire_root"] = p.wireRoot
		if trace.Truncated || trace.FrameFileError != "" || trace.FrameRecords == 0 || len(trace.Connections) != 1 || stats.AcceptedConnections != 1 || stats.UpstreamDialFailures != 0 || stats.Active != 0 || stats.BufferedBytes != 0 || stats.BufferOverflows != 0 {
			t.Fatalf("incomplete SQL leaf trace: stats=%+v trace=%+v", stats, trace)
		}
		t.Logf("SQL projector leaf wire: phase=%s pid=%d records=%d client_bytes=%d server_bytes=%d", p.record["phase"], p.command.Process.Pid, trace.FrameRecords, stats.ClientToServer, stats.ServerToClient)
	}

	if err := p.output.Close(); err != nil {
		t.Fatal(err)
	}
	return p.record
}

func (p *standaloneProjection) kill(t *testing.T) map[string]any {
	t.Helper()
	if err := p.command.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	<-p.joined
	status, ok := p.command.ProcessState.Sys().(syscall.WaitStatus)
	if !ok || !status.Signaled() || status.Signal() != syscall.SIGKILL {
		t.Fatalf("standalone kill not reaped SIGKILL: %v", p.command.ProcessState)
	}
	return p.recordExit(t)
}

func (p *standaloneProjection) stop(t *testing.T) {
	t.Helper()
	if err := p.command.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	select {
	case <-p.joined:
	case <-time.After(10 * time.Second):
		t.Fatal("standalone project did not stop after SIGTERM")
	}
	if p.err != nil || p.command.ProcessState.ExitCode() != 0 {
		t.Fatalf("standalone SIGTERM exit: %v/%v", p.err, p.command.ProcessState)
	}
}
