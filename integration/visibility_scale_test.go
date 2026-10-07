package integration_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"js-wf/testcluster"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"js-wf/client"
	"js-wf/identity"
	"js-wf/provision"
	"js-wf/visibility"
	"js-wf/wf"
	"js-wf/worker"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func TestProjectionProcessHelper(t *testing.T) {
	if os.Getenv("WF_PROJECTION_HELPER") == "" {
		t.Skip("projection process helper")
	}
	connection, err := nats.Connect(os.Getenv("WF_PROJECTION_URL"))
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	js, err := newTestJetStreamDomain(connection, os.Getenv("WF_PROJECTION_DOMAIN"))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()
	if domain := os.Getenv("WF_PROJECTION_DOMAIN"); domain != "" {
		info, err := js.AccountInfo(ctx)
		if err != nil || info.Domain != domain || connection.ConnectedDomain() != domain {
			t.Fatalf("projection helper domain: info=%+v err=%v", info, err)
		}
		fmt.Printf("projection helper real domain=%s server_id=%s\n", domain, connection.ConnectedServerId())
	}
	var options []visibility.Option
	if os.Getenv("WF_PROJECTION_BACKEND") == "postgres" {
		db, err := sql.Open("pgx", os.Getenv("WF_TEST_POSTGRES_DSN"))
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		options = append(options, visibility.WithPostgres(&visibility.PostgresStore{DB: db}))
	}
	projection, err := visibility.New(ctx, js, options...)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- projection.Run(ctx) }()
	select {
	case err := <-done:
		t.Fatalf("projection stopped before ready: %v", err)
	case <-time.After(200 * time.Millisecond):
	}
	if err := os.WriteFile(os.Getenv("WF_PROJECTION_READY_FILE"), []byte("ready"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

// WF_PROJECTION_SCALE=1 runs the Phase 7 consumer-restart proof. The count
// defaults to 50,000; WF_PROJECTION_COUNT allows a smaller diagnostic run.
func TestProjectionRecoversFiftyThousandInvocations(t *testing.T) {
	if os.Getenv("WF_PROJECTION_SCALE") == "" {
		t.Skip("set WF_PROJECTION_SCALE=1 for the 50,000-invocation projection proof")
	}
	runProjectionRecoversFiftyThousandInvocations(t, false)
}

func TestPostgresProjectionCrashAndSessionLossFiftyThousandInvocations(t *testing.T) {
	if os.Getenv("WF_PROJECTION_POSTGRES_FAULT_ROOT") == "" || os.Getenv("WF_TEST_POSTGRES_DSN") == "" {
		t.Skip("opt-in retained PostgreSQL full projection fault proof")
	}
	runProjectionRecoversFiftyThousandInvocations(t, true)
}

func TestPostgresProjectionCrashAndSessionLossFiftyThousandInvocationsInJetStreamDomain(t *testing.T) {
	if os.Getenv("WF_PROJECTION_POSTGRES_FAULT_ROOT") == "" || os.Getenv("WF_TEST_POSTGRES_DSN") == "" {
		t.Skip("opt-in retained PostgreSQL full domain projection fault proof")
	}
	if os.Getenv("WF_PROJECTION_COUNT") != "" {
		t.Fatal("full domain proof requires default50000 count")
	}
	runProjectionRecoversFiftyThousandInvocations(t, true, "WFVIEW")
}

func TestStandalonePostgresProjectionCrashAndSessionLossFiftyThousandInvocationsInJetStreamDomain(t *testing.T) {
	if os.Getenv("WF_PROJECTION_POSTGRES_FAULT_ROOT") == "" || os.Getenv("WF_TEST_POSTGRES_DSN") == "" {
		t.Skip("opt-in retained full packaged PostgreSQL domain projection fault proof")
	}
	if os.Getenv("WF_PROJECTION_COUNT") != "" {
		t.Fatal("full standalone proof requires default50000 count")
	}
	runProjectionRecovery(t, true, true, "WFVIEW")
}

func TestStandalonePostgresProjectionCrashAndSessionLossFiftyThousandInvocationsThroughLeaf(t *testing.T) {
	if os.Getenv("WF_PROJECTION_POSTGRES_FAULT_ROOT") == "" || os.Getenv("WF_TEST_POSTGRES_DSN") == "" {
		t.Skip("opt-in retained full packaged SQL leaf proof")
	}
	if os.Getenv("WF_PROJECTION_COUNT") != "" {
		t.Fatal("full SQL leaf proof requires default50000 count")
	}
	runProjectionRecoveryWithTransport(t, true, true, true, "WFVIEW")
}

// The original full case follows a real SQL startup cancellation boundary.
func TestStandalonePostgresProjectionCrashAndSessionLossFiftyThousandInvocationsThroughLeafWithSQLStartupCancellation(t *testing.T) {
	if os.Getenv("WF_PROJECTION_POSTGRES_FAULT_ROOT") == "" || os.Getenv("WF_TEST_POSTGRES_DSN") == "" {
		t.Skip("opt-in retained full packaged SQL leaf startup cancellation proof")
	}
	if os.Getenv("WF_PROJECTION_COUNT") != "" {
		t.Fatal("full SQL startup proof requires default50000 count")
	}
	runProjectionRecoveryStartup(t, true, true, true, true, "WFVIEW")
}

func TestStandalonePostgresProjectionCrashAndSessionLossFiftyThousandInvocationsThroughLeafWithSIGKILL(t *testing.T) {
	if os.Getenv("WF_PROJECTION_POSTGRES_FAULT_ROOT") == "" || os.Getenv("WF_TEST_POSTGRES_DSN") == "" {
		t.Skip("opt-in full SQL leaf SIGKILL proof")
	}
	if os.Getenv("WF_PROJECTION_COUNT") != "" {
		t.Fatal("full SQL leaf SIGKILL requires default50000")
	}
	runProjectionRecoveryLeafFault(t, true, true, true, true, true, "WFVIEW")
}

func runProjectionRecoversFiftyThousandInvocations(t *testing.T, postgresFault bool, domains ...string) {
	runProjectionRecovery(t, postgresFault, false, domains...)
}

func runProjectionRecovery(t *testing.T, postgresFault, standalone bool, domains ...string) {
	runProjectionRecoveryWithTransport(t, postgresFault, standalone, false, domains...)
}

func runProjectionRecoveryWithTransport(t *testing.T, postgresFault, standalone, leaf bool, domains ...string) {
	runProjectionRecoveryStartup(t, postgresFault, standalone, leaf, false, domains...)
}

func runProjectionRecoveryStartup(t *testing.T, postgresFault, standalone, leaf, sqlStartup bool, domains ...string) {
	runProjectionRecoveryLeafFault(t, postgresFault, standalone, leaf, sqlStartup, false, domains...)
}

func runProjectionRecoveryLeafFault(t *testing.T, postgresFault, standalone, leaf, sqlStartup, leafSIGKILL bool, domains ...string) {
	t.Helper()
	count := 50000
	if value := os.Getenv("WF_PROJECTION_COUNT"); value != "" {
		parsed, err := strconv.Atoi(value)
		if err != nil || parsed < 1 || parsed > 50000 {
			t.Fatalf("invalid WF_PROJECTION_COUNT %q", value)
		}
		count = parsed
	}
	var all []jetstream.JetStream
	var cluster *testcluster.Cluster
	root := ""
	domain := ""
	if len(domains) > 0 {
		domain = domains[0]
	}
	proof := map[string]any{"count": count, "postgres_fault": postgresFault, "projection_domain": domain}
	var observed, wrong atomic.Int64
	var jsOptions []jetstream.JetStreamOpt
	if domain != "" {
		jsOptions = append(jsOptions, jetstream.WithClientTrace(&jetstream.ClientTrace{RequestSent: func(subject string, _ []byte) {
			if strings.HasPrefix(subject, "$JS."+domain+".API.") {
				observed.Add(1)
			} else {
				wrong.Add(1)
			}
		}}))

	}
	if postgresFault {
		base := os.Getenv("WF_PROJECTION_POSTGRES_FAULT_ROOT")
		if !filepath.IsAbs(base) {
			t.Fatal("absolute retained root required")
		}
		root = filepath.Join(base, t.Name())
		if err := os.Mkdir(root, 0700); err != nil {
			t.Fatal(err)
		}
		defer func() {
			if domain != "" {
				proof["domain_api_requests"] = observed.Load()
				proof["wrong_domain_api_prefix_requests"] = wrong.Load()
				if observed.Load() == 0 || wrong.Load() != 0 {
					t.Errorf("projection domain API routing: observed=%d wrong=%d", observed.Load(), wrong.Load())
				}
				t.Logf("projection real domain=%s domain_api_requests=%d wrong_prefix_requests=%d", domain, observed.Load(), wrong.Load())
			}
			proof["native_test_failed"] = t.Failed()
			data, err := json.MarshalIndent(proof, "", "  ")
			if err == nil {
				err = os.WriteFile(filepath.Join(root, "projection-fault-proof.json"), append(data, '\n'), 0600)
			}
			if err != nil {
				t.Error(err)
			}
		}()
		var err error
		if domain == "" {
			cluster, err = testcluster.Start(filepath.Join(root, "cluster"), 3)
		} else if leaf {
			cluster, err = testcluster.StartWithLeafDomain(filepath.Join(root, "cluster"), 3, domain)
		} else {
			cluster, err = testcluster.StartWithDomain(filepath.Join(root, "cluster"), 3, domain)
		}
		if err != nil {
			t.Fatal(err)
		}
		all, cluster = setupClusterDomain(t, cluster, domain)
		if domain != "" {
			for i, nc := range cluster.Clients {
				all[i], err = newTestJetStreamDomain(nc, domain, jsOptions...)
				if err != nil {
					t.Fatal(err)
				}
			}
		}
	} else {
		all, cluster = setup(t)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	defer cancel()
	var db *sql.DB
	var options []visibility.Option
	if postgresFault {
		var err error
		db, err = sql.Open("pgx", os.Getenv("WF_TEST_POSTGRES_DSN"))
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		db.SetMaxOpenConns(40)
		options = append(options, visibility.WithPostgres(&visibility.PostgresStore{DB: db}))
	}
	const typ = "view-scale"
	var leafFault projectionLeafFault
	projectorURL := cluster.Servers[0].ClientURL()
	if leaf {
		if !postgresFault || !standalone || domain == "" {
			t.Fatal("SQL leaf requires full packaged domain fault profile")
		}
		var hooks []*projectionLeafFault
		if leafSIGKILL {
			hooks = append(hooks, &leafFault)
		}
		endpoint, finishLeaf := projectionLeafEndpoint(t, ctx, cluster, domain, root, hooks...)
		defer finishLeaf()
		projectorURL = endpoint
		proof["projector_transport"] = "leaf"
		proof["projector_leaf_url"] = endpoint
	}
	var standaloneBinary string
	var standaloneRecords []map[string]any
	if standalone {
		proof["projector_profile"] = "standalone"
		standaloneBinary = buildStandaloneProjection(t, ctx, root, proof)
		if sqlStartup {
			if !leaf || !postgresFault || domain != "WFVIEW" {
				t.Fatal("SQL startup requires full packaged SQL leaf profile")
			}
			proof["sql_startup_cancellation"] = cancelStandaloneSQLStartup(t, ctx, db, all[0], standaloneBinary, root, projectorURL, domain)
		}
		initial := startStandaloneProjection(t, ctx, db, standaloneBinary, root, "initial", projectorURL, domain, leaf)
		var rows int
		if err := db.QueryRowContext(ctx, `SELECT count(*) FROM wf_visibility`).Scan(&rows); err != nil || rows != 0 {
			t.Fatalf("initial rows=%d err=%v", rows, err)
		}
		proof["projection_process_observed_before_kill"] = initial.record["admitted_at"]
		standaloneRecords = append(standaloneRecords, initial.kill(t))
		proof["standalone_projectors"] = standaloneRecords
		proof["projection_process_reaped_sigkill"] = true
		proof["projection_process_stopped"] = time.Now().UTC()
	} else {
		readyFile := filepath.Join(t.TempDir(), "projection-ready")
		if postgresFault {
			readyFile = filepath.Join(root, "projection-ready")
		}
		process := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestProjectionProcessHelper$")
		process.Env = append(os.Environ(), "WF_PROJECTION_HELPER=1", "WF_PROJECTION_DOMAIN="+domain, "WF_PROJECTION_URL="+cluster.Servers[0].ClientURL(), "WF_PROJECTION_READY_FILE="+readyFile)
		backend := "kv"
		if postgresFault {
			backend = "postgres"
		}
		process.Env = append(process.Env, "WF_PROJECTION_BACKEND="+backend)
		var processOutput bytes.Buffer
		process.Stdout, process.Stderr = &processOutput, &processOutput
		if err := process.Start(); err != nil {
			t.Fatal(err)
		}
		processReaped := false
		defer func() {
			if !processReaped {
				_ = process.Process.Kill()
				_ = process.Wait()
			}
		}()
		readyDeadline := time.Now().Add(10 * time.Second)
		for time.Now().Before(readyDeadline) && ctx.Err() == nil {
			if _, err := os.Stat(readyFile); err == nil {
				break
			}
			time.Sleep(20 * time.Millisecond)
		}
		if _, err := os.Stat(readyFile); err != nil {
			t.Fatalf("projection process did not become ready: %v; output=%s", err, processOutput.String())
		}
		if postgresFault {
			captureProjectionProcess(t, process, root, proof)
		}
		if err := process.Process.Kill(); err != nil {
			t.Fatal(err)
		}
		if err := process.Wait(); err == nil {
			t.Fatal("projection process exited without SIGKILL")
		}
		processReaped = true
		if postgresFault {
			assertProjectionSIGKILL(t, process, proof)
			proof["projection_process_stopped"] = time.Now().UTC()
			if err := os.WriteFile(filepath.Join(root, "killed-projection.log"), processOutput.Bytes(), 0600); err != nil {
				t.Fatal(err)
			}
		}
	}
	projection, err := visibility.New(ctx, all[0], options...)
	if err != nil {
		t.Fatal(err)
	}
	workerCtx, stopWorkers := context.WithCancel(ctx)
	workerDone := make(chan error, 6)
	startedWorkers := 0
	joinWorkers := func() {
		stopWorkers()
		for startedWorkers > 0 {
			if err := <-workerDone; err != nil && !errors.Is(err, context.Canceled) {
				t.Errorf("worker exit: %v", err)
			}
			startedWorkers--
		}
	}
	defer joinWorkers()
	for index := 0; index < 6; index++ {
		w, err := worker.New(ctx, all[index%len(all)], fmt.Sprintf("view-scale-%d", index), map[string]worker.Handler{typ: func(_ *wf.Context, input json.RawMessage) (json.RawMessage, error) {
			return input, nil
		}}, worker.WithPartitionConcurrency(16))
		if err != nil {
			t.Fatal(err)
		}
		go func(index int) { workerDone <- w.RunAssigned(workerCtx, index, 6) }(index)
		startedWorkers++
	}
	run, err := all[0].Stream(ctx, "WF_RUN")
	if err != nil {
		t.Fatal(err)
	}
	for ctx.Err() == nil {
		info, err := run.Info(ctx)
		if err == nil && info.State.Consumers == int(provision.Partitions) {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if ctx.Err() != nil {
		t.Fatal("dispatch consumers were not ready")
	}
	clients := [3]*client.Client{client.New(all[0]), client.New(all[1]), client.New(all[2])}
	work := func(label string, action func(*client.Client, int) error) {
		t.Helper()
		workCtx, stopWork := context.WithCancel(ctx)
		defer stopWork()
		jobs := make(chan int, 256)
		failures := make(chan error, 1)
		var group sync.WaitGroup
		for caller := 0; caller < 64; caller++ {
			group.Add(1)
			go func(caller int) {
				defer group.Done()
				for index := range jobs {
					if err := action(clients[caller%len(clients)], index); err != nil {
						select {
						case failures <- err:
						default:
						}
						stopWork()
						return
					}
				}
			}(caller)
		}
	produce:
		for index := 0; index < count; index++ {
			select {
			case jobs <- index:
			case <-workCtx.Done():
				break produce
			}
		}
		close(jobs)
		group.Wait()
		select {
		case err := <-failures:
			t.Fatalf("%s: %v", label, err)
		default:
		}
		if ctx.Err() != nil {
			t.Fatalf("%s: %v", label, ctx.Err())
		}
	}
	started := time.Now()
	work("start", func(c *client.Client, index int) error {
		id := fmt.Sprintf("job-%05d", index)
		_, err := c.Start(ctx, typ, id, []byte(strconv.Itoa(index)))
		return err
	})
	t.Logf("started %d invocations in %s while projection stopped", count, time.Since(started))
	work("await", func(c *client.Client, index int) error {
		id := fmt.Sprintf("job-%05d", index)
		result, err := c.Await(ctx, typ, id)
		if err != nil {
			return fmt.Errorf("%s: %w", id, err)
		}
		if string(result) != strconv.Itoa(index) {
			return fmt.Errorf("%s: result=%s", id, result)
		}
		return nil
	})
	t.Logf("completed and checked %d results in %s", count, time.Since(started))
	proof["all_results_checked_while_projection_stopped"] = true
	journalStream, err := all[1].Stream(ctx, "WF_JRN")
	if err != nil {
		t.Fatal(err)
	}
	info, err := journalStream.Info(ctx)
	if err != nil || info.State.Msgs != uint64(2*count) || info.State.NumSubjects != uint64(count) {
		t.Fatalf("journal count: info=%+v err=%v", info, err)
	}
	if lag, err := projection.Lag(ctx); err != nil || lag != uint64(2*count) {
		t.Fatalf("stopped projection lag=%d want=%d err=%v", lag, 2*count, err)
	}
	proof["stopped_projection_lag"] = 2 * count
	if postgresFault {
		// Await observes the terminal before its delivery is necessarily acked.
		// Require physical queue drain before stopping completed workers.
		for ctx.Err() == nil {
			call, stop := context.WithTimeout(ctx, 2*time.Second)
			info, err := run.Info(call)
			stop()
			if err == nil && info.State.Msgs == 0 {
				proof["run_queue_drained_before_fault"] = time.Now().UTC()
				break
			}
			time.Sleep(20 * time.Millisecond)
		}
		if ctx.Err() != nil {
			t.Fatal("workflow queue did not drain before projection fault", ctx.Err())
		}
		joinWorkers()
		if t.Failed() {
			t.Fatal("workflow workers failed before projection fault")
		}
		proof["all_workflow_workers_joined_before_fault"] = time.Now().UTC()
	}
	var projectionJS jetstream.JetStream = all[2]
	var dependencyTrace *retainedAuditTrace
	if postgresFault {
		dependencyTrace = &retainedAuditTrace{}
		projectionJS = tracedAuditJS{JetStream: all[2], trace: dependencyTrace}
		defer func() {
			data, err := json.MarshalIndent(dependencyTrace.snapshot(), "", "  ")
			if err == nil {
				err = os.WriteFile(filepath.Join(root, "projection-dependency-trace.json"), append(data, '\n'), 0600)
			}
			if err != nil {
				t.Error(err)
			}
		}()
	}
	restarted, err := visibility.New(ctx, projectionJS, options...)
	if err != nil {
		t.Fatal(err)
	}
	projectionCtx, stopProjection := context.WithCancel(ctx)
	defer stopProjection()
	projectionDone := make(chan error, 1)
	var standaloneWriter *standaloneProjection
	if standalone {
		standaloneWriter = startStandaloneProjection(t, projectionCtx, db, standaloneBinary, root, "catchup", projectorURL, domain, leaf)
		projectionDone = standaloneWriter.done
	} else {
		go func() { projectionDone <- restarted.Run(projectionCtx) }()
	}
	if postgresFault {
		applyPostgresProjectionCatchupFault(t, ctx, db, cluster, all, journalStream, projectionDone, proof, jsOptions...)
		if standalone {
			if proof["terminated_writer_backend_pid"] != standaloneWriter.record["writer_backend_pid"] {
				t.Fatal("fault did not terminate admitted CLI writer")
			}
			record := standaloneWriter.recordExit(t)
			if record["exit_code"] != 1 {
				t.Fatalf("faulted CLI exit=%v want1", record["exit_code"])
			}
			standaloneRecords = append(standaloneRecords, record)
			proof["standalone_projectors"] = standaloneRecords
		}
		if leafSIGKILL {
			proof["leaf_transport_fault"] = faultStandaloneSQLLeaf(t, ctx, db, standaloneBinary, root, projectorURL, domain, &leafFault)
		}
		stopProjection()
		projectionJS = tracedAuditJS{JetStream: all[2], trace: dependencyTrace}
		restarted, err = visibility.New(ctx, projectionJS, options...)
		if err != nil {
			t.Fatal(err)
		}
		projectionCtx, stopProjection = context.WithCancel(ctx)
		defer stopProjection()
		projectionDone = make(chan error, 1)
		if standalone {
			standaloneWriter = startStandaloneProjection(t, projectionCtx, db, standaloneBinary, root, "replacement", projectorURL, domain, leaf)
			projectionDone = standaloneWriter.done
		} else {
			go func() { projectionDone <- restarted.Run(projectionCtx) }()
		}
	}
	for ctx.Err() == nil {
		select {
		case err := <-projectionDone:
			t.Fatalf("restarted projection exited before lag drained: %v", err)
		default:
		}
		lag, err := restarted.Lag(ctx)
		if err == nil && lag == 0 {
			break
		}
		time.Sleep(250 * time.Millisecond)
	}
	if ctx.Err() != nil {
		stopProjection()
		<-projectionDone
		t.Fatalf("projection lag did not drain: %v", ctx.Err())
	}
	if standalone {
		standaloneWriter.stop(t)
	}
	stopProjection()
	if err := <-projectionDone; err != nil && !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if standalone {
		standaloneRecords = append(standaloneRecords, standaloneWriter.recordExit(t))
		proof["standalone_projectors"] = standaloneRecords
		proof["standalone_replacement_clean_sigterm"] = true
	}
	t.Logf("projection lag drained in %s", time.Since(started))
	if postgresFault {
		proof["final_lag"] = 0
		before := postgresProjectionState(t, ctx, db, count, filepath.Join(root, "before-rebuild.jsonl"))
		if err := restarted.Rebuild(ctx); err != nil {
			t.Fatal(err)
		}
		after := postgresProjectionState(t, ctx, db, count, filepath.Join(root, "after-rebuild.jsonl"))
		if before != after {
			t.Fatalf("PostgreSQL projection changed across full rebuild: %x/%x", before, after)
		}
		proof["row_and_indexed_column_sha256"] = fmt.Sprintf("%x", before)
		proof["elapsed_ns"] = time.Since(started).Nanoseconds()
		return
	}
	view, err := all[0].KeyValue(ctx, "WF_VIEW")
	if err != nil {
		t.Fatal(err)
	}
	before, err := projectionStateDigest(ctx, view, count)
	if err != nil {
		t.Fatal(err)
	}
	if err := restarted.Rebuild(ctx); err != nil {
		t.Fatalf("full rebuild: %v", err)
	}
	after, err := projectionStateDigest(ctx, view, count)
	if err != nil {
		t.Fatal(err)
	}
	if before != after {
		t.Fatalf("full rebuild changed projection bytes or revisions: before=%x after=%x", before, after)
	}
	t.Logf("projection rebuild preserved %d row/index pairs in %s", count, time.Since(started))
}

func projectionStateDigest(ctx context.Context, view jetstream.KeyValue, count int) ([32]byte, error) {
	readCtx, stop := context.WithCancel(ctx)
	defer stop()
	keys, err := view.Keys(ctx)
	if err != nil {
		return [32]byte{}, err
	}
	if len(keys) != 2*count {
		return [32]byte{}, fmt.Errorf("projection has %d keys, want %d", len(keys), 2*count)
	}
	sort.Strings(keys)
	rows, indexes := 0, 0
	for _, key := range keys {
		switch {
		case strings.HasPrefix(key, "row."):
			rows++
			matching := "idx.completed." + strings.TrimPrefix(key, "row.")
			if position := sort.SearchStrings(keys, matching); position >= len(keys) || keys[position] != matching {
				return [32]byte{}, fmt.Errorf("row %q has no completed index", key)
			}
		case strings.HasPrefix(key, "idx.completed."):
			indexes++
			matching := "row." + strings.TrimPrefix(key, "idx.completed.")
			if position := sort.SearchStrings(keys, matching); position >= len(keys) || keys[position] != matching {
				return [32]byte{}, fmt.Errorf("index %q has no row", key)
			}
		default:
			return [32]byte{}, fmt.Errorf("unexpected projection key %q", key)
		}
	}
	if rows != count || indexes != count {
		return [32]byte{}, fmt.Errorf("projection has %d rows and %d indexes, want %d each", rows, indexes, count)
	}
	digests := make([][32]byte, len(keys))
	jobs := make(chan int, 128)
	var group sync.WaitGroup
	var firstErr error
	var failOnce sync.Once
	fail := func(err error) {
		failOnce.Do(func() { firstErr = err; stop() })
	}
	for i := 0; i < 32; i++ {
		group.Add(1)
		go func() {
			defer group.Done()
			for index := range jobs {
				key := keys[index]
				entry, err := view.Get(readCtx, key)
				if err != nil {
					fail(err)
					return
				}
				if strings.HasPrefix(key, "row.") {
					var row visibility.Row
					if err := json.Unmarshal(entry.Value(), &row); err != nil || row.Type != "view-scale" || row.Status != "completed" || key != "row."+identity.Key(row.Type, row.ID) || row.InvSeq == 0 || row.JournalSeq == 0 || row.LastIndex != 1 || row.Started.IsZero() || row.Updated.IsZero() {
						fail(fmt.Errorf("invalid completed row %q: %+v: %v", key, row, err))
						return
					}
				}
				var revision [8]byte
				binary.BigEndian.PutUint64(revision[:], entry.Revision())
				h := sha256.New()
				writeProjectionHash(h, []byte(key), revision[:], entry.Value())
				digests[index] = sha256.Sum256(h.Sum(nil))
			}
		}()
	}
	for index := range keys {
		select {
		case jobs <- index:
		case <-readCtx.Done():
			close(jobs)
			group.Wait()
			if firstErr != nil {
				return [32]byte{}, firstErr
			}
			return [32]byte{}, ctx.Err()
		}
	}
	close(jobs)
	group.Wait()
	if firstErr != nil {
		return [32]byte{}, firstErr
	}
	h := sha256.New()
	for _, digest := range digests {
		_, _ = h.Write(digest[:])
	}
	var output [32]byte
	copy(output[:], h.Sum(nil))
	return output, nil
}

func writeProjectionHash(h hash.Hash, parts ...[]byte) {
	var length [8]byte
	for _, part := range parts {
		binary.BigEndian.PutUint64(length[:], uint64(len(part)))
		_, _ = h.Write(length[:])
		_, _ = h.Write(part)
	}
}
