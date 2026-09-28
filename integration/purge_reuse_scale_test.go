package integration_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"js-wf/client"
	"js-wf/integrity"
	"js-wf/journal"
	"js-wf/provision"
	"js-wf/retention"
	"js-wf/visibility"
	"js-wf/wf"
	"js-wf/worker"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// WF_PURGE_REUSE_SCALE=1 runs the Phase 7 10k/10k/1k retention proof.
// WF_PURGE_REUSE_COUNT can lower both 10k groups for diagnostics.
// WF_PURGE_REUSE_ACTIVE=1 keeps all live handlers executing during the purge.
// WF_PURGE_REUSE_POSTGRES=1 also verifies the live PostgreSQL projection.
func TestTenThousandConcurrentPurgesAndThousandReusedIDs(t *testing.T) {
	if os.Getenv("WF_PURGE_REUSE_SCALE") == "" {
		t.Skip("set WF_PURGE_REUSE_SCALE=1 for the 10k purge/reuse proof")
	}
	count := 10000
	if value := os.Getenv("WF_PURGE_REUSE_COUNT"); value != "" {
		parsed, err := strconv.Atoi(value)
		if err != nil || parsed < 10 || parsed > 10000 {
			t.Fatalf("invalid WF_PURGE_REUSE_COUNT %q", value)
		}
		count = parsed
	}
	reuseCount := count / 10
	activeMode := os.Getenv("WF_PURGE_REUSE_ACTIVE") == "1"
	all, _ := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	defer cancel()
	const typ = "purge-scale"
	var postgres *visibility.Projection
	var postgresDB *sql.DB
	if os.Getenv("WF_PURGE_REUSE_POSTGRES") == "1" {
		dsn := os.Getenv("WF_TEST_POSTGRES_DSN")
		if dsn == "" {
			t.Fatal("WF_PURGE_REUSE_POSTGRES requires WF_TEST_POSTGRES_DSN for a disposable database")
		}
		var err error
		postgresDB, err = sql.Open("pgx", dsn)
		if err != nil {
			t.Fatal(err)
		}
		defer postgresDB.Close()
		postgres, err = visibility.New(ctx, all[0], visibility.WithPostgres(&visibility.PostgresStore{DB: postgresDB}))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := postgresDB.ExecContext(ctx, `TRUNCATE wf_visibility`); err != nil {
			t.Fatal(err)
		}
		projectionCtx, stopProjection := context.WithCancel(ctx)
		projectionDone := make(chan error, 1)
		go func() { projectionDone <- postgres.Run(projectionCtx) }()
		defer func() {
			stopProjection()
			if err := <-projectionDone; err != nil && !errors.Is(err, context.Canceled) {
				t.Errorf("PostgreSQL projection exit: %v", err)
			}
		}()
	}
	type input struct {
		Kind  string `json:"kind"`
		Index int    `json:"index"`
	}
	activeRelease := make(chan struct{})
	var activeHandlers atomic.Int64
	handler := func(c *wf.Context, raw json.RawMessage) (json.RawMessage, error) {
		var job input
		if err := json.Unmarshal(raw, &job); err != nil {
			return nil, err
		}
		switch job.Kind {
		case "old":
			return json.RawMessage(strconv.Itoa(job.Index)), nil
		case "live":
			if activeMode {
				activeHandlers.Add(1)
				defer activeHandlers.Add(-1)
				select {
				case <-activeRelease:
				case <-c.Context().Done():
					return nil, c.Context().Err()
				}
			}
			return wf.AwaitSignal(c, "release")
		case "reused":
			return wf.AwaitSignal(c, "go")
		default:
			return nil, fmt.Errorf("unknown job kind %q", job.Kind)
		}
	}
	workerCtx, stopWorkers := context.WithCancel(ctx)
	workerDone := make(chan error, 6)
	startedWorkers := 0
	defer func() {
		stopWorkers()
		for i := 0; i < startedWorkers; i++ {
			if err := <-workerDone; err != nil && !errors.Is(err, context.Canceled) {
				t.Errorf("worker exit: %v", err)
			}
		}
	}()
	for index := 0; index < 6; index++ {
		concurrency := 32
		if activeMode {
			concurrency = 256
		}
		w, err := worker.New(ctx, all[index%len(all)], fmt.Sprintf("purge-scale-%d", index), map[string]worker.Handler{typ: handler}, worker.WithPartitionConcurrency(concurrency))
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
	oldSeq := make([]uint64, reuseCount)
	oldEpoch := make([]uint64, reuseCount)
	newSeq := make([]uint64, reuseCount)
	liveSeq := make([]uint64, count)
	staleSignalSeq := make([]uint64, reuseCount)
	freshSignalSeq := make([]uint64, reuseCount)
	var purgesDone, reuseOverlaps atomic.Int64
	oldID := func(index int) string { return fmt.Sprintf("old-%05d", index) }
	liveID := func(index int) string { return fmt.Sprintf("live-%05d", index) }
	start := time.Now()
	runPurgeScaleJobs(t, ctx, count, func(caller, index int) error {
		data, _ := json.Marshal(input{Kind: "old", Index: index})
		handle, err := clients[caller%3].Start(ctx, typ, oldID(index), data)
		if err == nil && index < reuseCount {
			oldSeq[index] = handle.InvSeq
		}
		return err
	})
	runPurgeScaleJobs(t, ctx, count, func(caller, index int) error {
		value, err := clients[caller%3].Await(ctx, typ, oldID(index))
		if err != nil || string(value) != strconv.Itoa(index) {
			return fmt.Errorf("old %d: value=%s err=%v", index, value, err)
		}
		return nil
	})
	t.Logf("completed %d old invocations in %s", count, time.Since(start))
	runPurgeScaleJobs(t, ctx, reuseCount, func(caller, index int) error {
		records, _, err := journal.New(all[caller%3]).Read(ctx, typ, oldID(index))
		if err != nil || len(records) != 2 || records[0].Kind != journal.Started || records[1].Kind != journal.Completed {
			return fmt.Errorf("old journal %d: entries=%d err=%v", index, len(records), err)
		}
		oldEpoch[index] = records[0].Epoch
		return nil
	})
	if postgres != nil {
		waitPostgresPurgeScale(t, ctx, all[0], postgres, postgresDB, count, nil, nil)
		t.Logf("PostgreSQL indexed %d completed old invocations before purge in %s", count, time.Since(start))
	}
	runPurgeScaleJobs(t, ctx, count, func(caller, index int) error {
		data, _ := json.Marshal(input{Kind: "live", Index: index})
		handle, err := clients[caller%3].Start(ctx, typ, liveID(index), data)
		if err == nil {
			liveSeq[index] = handle.InvSeq
		}
		return err
	})
	waitSuspended := func(id string, js jetstream.JetStream) ([]journal.Record, error) {
		for ctx.Err() == nil {
			records, _, err := journal.New(js).Read(ctx, typ, id)
			if err != nil {
				return nil, err
			}
			if len(records) > 0 && records[len(records)-1].Kind == journal.Suspended {
				return records, nil
			}
			if len(records) > 0 && (records[len(records)-1].Kind == journal.Completed || records[len(records)-1].Kind == journal.Failed) {
				return nil, fmt.Errorf("%s became terminal before fresh signal", id)
			}
			time.Sleep(20 * time.Millisecond)
		}
		return nil, ctx.Err()
	}
	if activeMode {
		for ctx.Err() == nil && activeHandlers.Load() != int64(count) {
			time.Sleep(20 * time.Millisecond)
		}
		if ctx.Err() != nil {
			t.Fatalf("only %d/%d handlers active: %v", activeHandlers.Load(), count, ctx.Err())
		}
		t.Logf("kept %d other handlers executing before purge at %s", count, time.Since(start))
	} else {
		runPurgeScaleJobs(t, ctx, count, func(caller, index int) error {
			_, err := waitSuspended(liveID(index), all[caller%3])
			return err
		})
		t.Logf("kept %d other invocations suspended before purge at %s", count, time.Since(start))
	}
	runPurgeScaleJobs(t, ctx, count, func(caller, index int) error {
		id := oldID(index)
		for ctx.Err() == nil {
			err := retention.Purge(ctx, all[caller%3], typ, id, time.Hour)
			if err == nil {
				break
			}
			if !errors.Is(err, retention.ErrActive) {
				return fmt.Errorf("purge %s: %w", id, err)
			}
			time.Sleep(20 * time.Millisecond)
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		purgesDone.Add(1)
		if index >= reuseCount {
			return nil
		}
		stale := &nats.Msg{Subject: "wf.sig." + typ + "." + id + ".go", Data: []byte("stale"), Header: nats.Header{}}
		stale.Header.Set("Wf-Inv-Seq", strconv.FormatUint(oldSeq[index], 10))
		ack, err := all[caller%3].PublishMsg(ctx, stale)
		if err != nil {
			return fmt.Errorf("publish delayed old signal %s: %w", id, err)
		}
		staleSignalSeq[index] = ack.Sequence
		if purgesDone.Load() < int64(count) {
			reuseOverlaps.Add(1)
		}
		data, _ := json.Marshal(input{Kind: "reused", Index: index})
		handle, err := clients[caller%3].Start(ctx, typ, id, data)
		if err != nil {
			return fmt.Errorf("reuse %s: %w", id, err)
		}
		if handle.InvSeq <= oldSeq[index] {
			return fmt.Errorf("reuse %s did not advance invocation sequence", id)
		}
		newSeq[index] = handle.InvSeq
		return nil
	})
	if purgesDone.Load() != int64(count) || reuseOverlaps.Load() == 0 {
		t.Fatalf("purge/reuse overlap: purged=%d/%d reused_during_purge=%d", purgesDone.Load(), count, reuseOverlaps.Load())
	}
	if activeMode && activeHandlers.Load() != int64(count) {
		t.Fatalf("handlers lost during purge: active=%d want=%d", activeHandlers.Load(), count)
	}
	t.Logf("purged %d while %d remained live; reused %d ids in %s", count, count, reuseCount, time.Since(start))
	if activeMode {
		close(activeRelease)
	}
	runPurgeScaleJobs(t, ctx, count, func(caller, index int) error {
		_, err := waitSuspended(liveID(index), all[caller%3])
		return err
	})
	runPurgeScaleJobs(t, ctx, reuseCount, func(caller, index int) error {
		records, err := waitSuspended(oldID(index), all[caller%3])
		if err != nil {
			return err
		}
		for _, record := range records {
			if record.Kind == journal.SignalConsumed {
				return fmt.Errorf("reused %s consumed a signal before fresh publish", oldID(index))
			}
		}
		return nil
	})
	runPurgeScaleJobs(t, ctx, reuseCount, func(caller, index int) error {
		seq, err := clients[caller%3].Signal(ctx, typ, oldID(index), "go", []byte(strconv.Itoa(100000+index)), fmt.Sprintf("fresh-%d", index))
		if err == nil {
			freshSignalSeq[index] = seq
		}
		return err
	})
	runPurgeScaleJobs(t, ctx, reuseCount, func(caller, index int) error {
		value, err := clients[caller%3].Await(ctx, typ, oldID(index))
		if err != nil || string(value) != strconv.Itoa(100000+index) {
			return fmt.Errorf("reused %d: value=%s err=%v", index, value, err)
		}
		return nil
	})
	runPurgeScaleJobs(t, ctx, count, func(caller, index int) error {
		_, err := clients[caller%3].Signal(ctx, typ, liveID(index), "release", []byte(strconv.Itoa(index)), fmt.Sprintf("release-%d", index))
		return err
	})
	runPurgeScaleJobs(t, ctx, count, func(caller, index int) error {
		value, err := clients[caller%3].Await(ctx, typ, liveID(index))
		if err != nil || string(value) != strconv.Itoa(index) {
			return fmt.Errorf("live %d: value=%s err=%v", index, value, err)
		}
		return nil
	})
	t.Logf("reused and released all invocations in %s", time.Since(start))
	runPurgeScaleJobs(t, ctx, reuseCount, func(caller, index int) error {
		records, _, err := journal.New(all[caller%3]).Read(ctx, typ, oldID(index))
		if err != nil || len(records) < 2 || records[0].Kind != journal.Started || records[0].Index != 0 || records[0].Epoch <= oldEpoch[index] || records[len(records)-1].Kind != journal.Completed {
			return fmt.Errorf("reused %s journal: entries=%d old_epoch=%d err=%v", oldID(index), len(records), oldEpoch[index], err)
		}
		consumed := uint64(0)
		for _, record := range records {
			if record.Epoch <= oldEpoch[index] {
				return fmt.Errorf("reused %s includes prior epoch %d", oldID(index), record.Epoch)
			}
			if record.Kind == journal.SignalConsumed {
				var signal struct {
					Sequence uint64 `json:"sig_seq"`
				}
				if err := json.Unmarshal(record.Payload, &signal); err != nil {
					return err
				}
				if signal.Sequence == staleSignalSeq[index] || signal.Sequence != freshSignalSeq[index] {
					return fmt.Errorf("reused %s consumed signal %d, stale=%d fresh=%d", oldID(index), signal.Sequence, staleSignalSeq[index], freshSignalSeq[index])
				}
				consumed++
			}
		}
		if consumed != 1 {
			return fmt.Errorf("reused %s consumed %d signals", oldID(index), consumed)
		}
		var outcome wf.Outcome
		if err := json.Unmarshal(records[len(records)-1].Payload, &outcome); err != nil || outcome.InvSeq != newSeq[index] {
			return fmt.Errorf("reused %s terminal generation: %+v err=%v", oldID(index), outcome, err)
		}
		return nil
	})
	for ctx.Err() == nil {
		info, err := run.Info(ctx)
		if err == nil && info.State.Msgs == 0 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if ctx.Err() != nil {
		t.Fatal("run queue did not drain")
	}
	inv, err := all[0].Stream(ctx, "WF_INV")
	if err != nil {
		t.Fatal(err)
	}
	info, err := inv.Info(ctx)
	if err != nil || info.State.NumSubjects != uint64(count+reuseCount) {
		t.Fatalf("retained invocation subjects=%d want=%d err=%v", info.State.NumSubjects, count+reuseCount, err)
	}
	if _, err := integrity.Check(ctx, all[0]); err != nil {
		t.Fatalf("retained integrity audit: %v", err)
	}
	if postgres != nil {
		waitPostgresPurgeScale(t, ctx, all[0], postgres, postgresDB, count, liveSeq, newSeq)
		if count == 10000 && postgres.BacklogRebuilds() == 0 {
			t.Fatal("10,000-case PostgreSQL proof did not exercise backlog rebuild")
		}
		if count == 10000 && postgres.JournalResets() == 0 {
			t.Fatal("10,000-case PostgreSQL proof did not advance rebuilt journal cursor")
		}
		t.Logf("PostgreSQL caught up after all purges and reuse in %s; backlog_rebuilds=%d journal_resets=%d", time.Since(start), postgres.BacklogRebuilds(), postgres.JournalResets())
	}
	t.Logf("retained integrity audit passed in %s", time.Since(start))
}

// A nil generation slice checks the pre-purge population. Otherwise every
// retained row must belong to a live ID or to one of the reused generations.
func waitPostgresPurgeScale(t *testing.T, ctx context.Context, js jetstream.JetStream, projection *visibility.Projection, db *sql.DB, count int, liveSeq, newSeq []uint64) {
	t.Helper()
	want := count
	if liveSeq != nil {
		want += len(newSeq)
	}
	lastProgress := time.Now()
	for ctx.Err() == nil {
		lag, err := projection.Lag(ctx)
		if err == nil && lag == 0 {
			var rows int
			if err := db.QueryRowContext(ctx, `SELECT count(*) FROM wf_visibility WHERE type='purge-scale'`).Scan(&rows); err == nil && rows == want {
				break
			}
		}
		if time.Since(lastProgress) >= 10*time.Second {
			var rows int
			_ = db.QueryRowContext(ctx, `SELECT count(*) FROM wf_visibility WHERE type='purge-scale'`).Scan(&rows)
			t.Logf("PostgreSQL catch-up rows=%d/%d lag=%d lag_err=%v backlog_rebuilds=%d journal_resets=%d", rows, want, lag, err, projection.BacklogRebuilds(), projection.JournalResets())
			lastProgress = time.Now()
		}
		time.Sleep(100 * time.Millisecond)
	}
	if ctx.Err() != nil {
		inspectCtx, stop := context.WithTimeout(context.Background(), 3*time.Second)
		defer stop()
		lag, lagErr := projection.Lag(inspectCtx)
		var rows int
		countErr := db.QueryRowContext(inspectCtx, `SELECT count(*) FROM wf_visibility WHERE type='purge-scale'`).Scan(&rows)
		t.Fatalf("PostgreSQL projection did not settle: rows=%d/%d lag=%d lag_err=%v count_err=%v context=%v", rows, want, lag, lagErr, countErr, ctx.Err())
	}
	rows, err := db.QueryContext(ctx, `SELECT id, status, row_data->>'inv_seq' FROM wf_visibility WHERE type='purge-scale' ORDER BY id`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	seen := 0
	for rows.Next() {
		var id, status, generation string
		if err := rows.Scan(&id, &status, &generation); err != nil {
			t.Fatal(err)
		}
		if status != "completed" {
			t.Fatalf("PostgreSQL row %s status=%s, want completed", id, status)
		}
		var index int
		var wantSeq uint64
		switch {
		case strings.HasPrefix(id, "old-"):
			if _, err := fmt.Sscanf(id, "old-%05d", &index); err != nil || index < 0 || index >= count {
				t.Fatalf("invalid PostgreSQL old row %s: %v", id, err)
			}
			if liveSeq == nil {
				// Before purge, only the completed status is required; its
				// generation is checked by the source-of-truth journal.
				seen++
				continue
			}
			if index >= len(newSeq) {
				t.Fatalf("purged PostgreSQL row %s remains", id)
			}
			wantSeq = newSeq[index]
		case strings.HasPrefix(id, "live-") && liveSeq != nil:
			if _, err := fmt.Sscanf(id, "live-%05d", &index); err != nil || index < 0 || index >= count {
				t.Fatalf("invalid PostgreSQL live row %s: %v", id, err)
			}
			wantSeq = liveSeq[index]
		default:
			t.Fatalf("unexpected PostgreSQL row %s", id)
		}
		if wantSeq == 0 || generation != strconv.FormatUint(wantSeq, 10) {
			t.Fatalf("PostgreSQL row %s generation=%s, want %d", id, generation, wantSeq)
		}
		seen++
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if seen != want {
		t.Fatalf("verified PostgreSQL rows=%d, want %d", seen, want)
	}
	if liveSeq != nil {
		purges, err := js.Stream(ctx, "WF_PURGE")
		if err != nil {
			t.Fatal(err)
		}
		info, err := purges.Info(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if info.State.Msgs != 0 {
			t.Fatalf("PostgreSQL purge work queue messages=%d, want zero", info.State.Msgs)
		}
	}
}

func runPurgeScaleJobs(t *testing.T, ctx context.Context, count int, action func(caller, index int) error) {
	t.Helper()
	workCtx, stop := context.WithCancel(ctx)
	defer stop()
	jobs := make(chan int, 256)
	failures := make(chan error, 1)
	var group sync.WaitGroup
	for caller := 0; caller < 64; caller++ {
		group.Add(1)
		go func(caller int) {
			defer group.Done()
			for index := range jobs {
				if err := action(caller, index); err != nil {
					select {
					case failures <- err:
					default:
					}
					stop()
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
		t.Fatal(err)
	default:
	}
	if ctx.Err() != nil {
		t.Fatal(ctx.Err())
	}
}
