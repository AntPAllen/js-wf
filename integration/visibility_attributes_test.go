package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"js-wf/client"
	"js-wf/identity"
	"js-wf/journal"
	"js-wf/provision"
	"js-wf/retention"
	"js-wf/visibility"
	"js-wf/wf"
	"js-wf/worker"
)

func TestVisibilitySearchAttributesRebuildPurgeAndReuse(t *testing.T) {
	all, _ := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	const typ, id = "test", "search-attributes"
	p, err := visibility.New(ctx, all[0])
	if err != nil {
		t.Fatal(err)
	}
	w, err := worker.New(ctx, all[1], "attribute-worker", map[string]worker.Handler{typ: func(c *wf.Context, input json.RawMessage) (json.RawMessage, error) {
		if string(input) == `"second"` {
			if err := c.SetSearchAttributes(map[string]string{"team": "gamma"}); err != nil {
				return nil, err
			}
			return json.RawMessage(`2`), nil
		}
		if err := c.SetSearchAttributes(map[string]string{"team": "alpha", "region": "east"}); err != nil {
			return nil, err
		}
		if _, err := wf.AwaitSignal(c, "go"); err != nil {
			return nil, err
		}
		if err := c.SetSearchAttributes(map[string]string{"team": "beta"}); err != nil {
			return nil, err
		}
		return json.RawMessage(`1`), nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	client := client.New(all[0])
	startWorker := func() (context.CancelFunc, <-chan error) {
		workerCtx, stop := context.WithCancel(ctx)
		done := make(chan error, 1)
		go func() { done <- w.RunPartition(workerCtx, identity.Partition(typ, id, provision.Partitions)) }()
		return stop, done
	}
	if _, err := client.Start(ctx, typ, id, []byte(`"first"`)); err != nil {
		t.Fatal(err)
	}
	stop, done := startWorker()
	for ctx.Err() == nil {
		records, _, err := journal.New(all[0]).Read(ctx, typ, id)
		if err != nil {
			t.Fatal(err)
		}
		if len(records) > 0 && records[len(records)-1].Kind == journal.Suspended {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if ctx.Err() != nil {
		t.Fatal("workflow did not suspend")
	}
	if err := p.SyncOne(ctx, typ, id); err != nil {
		t.Fatal(err)
	}
	check := func(key, value, status string, want int) {
		t.Helper()
		rows, err := p.ListByAttribute(ctx, key, value, status)
		if err != nil || len(rows) != want {
			t.Fatalf("search %s=%s status=%s: rows=%+v err=%v", key, value, status, rows, err)
		}
		if want == 1 && rows[0].ID != id {
			t.Fatalf("unexpected row: %+v", rows[0])
		}
	}
	check("team", "alpha", "suspended", 1)
	check("region", "east", "", 1)
	if _, err := client.Signal(ctx, typ, id, "go", []byte(`true`), "release"); err != nil {
		t.Fatal(err)
	}
	if result, err := client.Await(ctx, typ, id); err != nil || string(result) != "1" {
		t.Fatalf("first result=%s err=%v", result, err)
	}
	stop()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if err := p.SyncOne(ctx, typ, id); err != nil {
		t.Fatal(err)
	}
	check("team", "alpha", "", 0)
	check("region", "east", "", 0)
	check("team", "beta", "completed", 1)
	if _, err := journal.New(all[0]).SnapshotPrefix(ctx, typ, id, 3); err != nil {
		t.Fatal(err)
	}
	if err := p.Rebuild(ctx); err != nil {
		t.Fatal(err)
	}
	check("team", "beta", "completed", 1)
	if err := retention.Purge(ctx, all[0], typ, id, time.Minute); err != nil {
		t.Fatal(err)
	}
	if err := p.Rebuild(ctx); err != nil {
		t.Fatal(err)
	}
	check("team", "beta", "", 0)
	if _, err := p.Get(ctx, typ, id); !errors.Is(err, visibility.ErrNotFound) {
		t.Fatalf("purged row: %v", err)
	}
	if _, err := client.Start(ctx, typ, id, []byte(`"second"`)); err != nil {
		t.Fatal(err)
	}
	stop, done = startWorker()
	if result, err := client.Await(ctx, typ, id); err != nil || string(result) != "2" {
		t.Fatalf("second result=%s err=%v", result, err)
	}
	stop()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if err := p.Rebuild(ctx); err != nil {
		t.Fatal(err)
	}
	check("team", "gamma", "completed", 1)
	check("team", "beta", "", 0)
	migrated, err := visibility.New(ctx, all[2], visibility.WithAttributeMapper(2, func(attributes map[string]string) (map[string]string, error) {
		if team, ok := attributes["team"]; ok {
			delete(attributes, "team")
			attributes["group"] = team
		}
		return attributes, nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	if err := migrated.Rebuild(ctx); err != nil {
		t.Fatal(err)
	}
	rows, err := migrated.ListByAttribute(ctx, "group", "gamma", "completed")
	if err != nil || len(rows) != 1 || rows[0].SchemaVersion != 2 {
		t.Fatalf("migrated rows=%+v err=%v", rows, err)
	}
	rows, err = migrated.ListByAttribute(ctx, "team", "gamma", "")
	if err != nil || len(rows) != 0 {
		t.Fatalf("old attribute index=%+v err=%v", rows, err)
	}
}
