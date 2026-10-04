package integrity

import (
	"context"
	"errors"
	"testing"

	"github.com/nats-io/nats.go/jetstream"
)

type auditWatchEntry struct {
	jetstream.KeyValueEntry
	key string
	rev uint64
	op  jetstream.KeyValueOp
}

func (e auditWatchEntry) Bucket() string                  { return "WF_STATE" }
func (e auditWatchEntry) Key() string                     { return e.key }
func (e auditWatchEntry) Revision() uint64                { return e.rev }
func (e auditWatchEntry) Operation() jetstream.KeyValueOp { return e.op }

type auditWatch struct {
	updates chan jetstream.KeyValueEntry
	stopped bool
}

func (w *auditWatch) Updates() <-chan jetstream.KeyValueEntry { return w.updates }
func (w *auditWatch) Stop() error                             { w.stopped = true; return nil }

type auditWatchState struct {
	jetstream.KeyValue
	watch *auditWatch
}

func (s auditWatchState) Bucket() string { return "WF_STATE" }
func (s auditWatchState) WatchAll(context.Context, ...jetstream.WatchOpt) (jetstream.KeyWatcher, error) {
	return s.watch, nil
}

func TestInitialAuditStateRequiresCompleteInitialSet(t *testing.T) {
	for _, tc := range []struct {
		name    string
		entries []jetstream.KeyValueEntry
		valid   bool
	}{
		{"empty complete", []jetstream.KeyValueEntry{nil}, true},
		{"partial closed", []jetstream.KeyValueEntry{auditWatchEntry{key: "a", rev: 1}}, false},
		{"empty closed", nil, false},
		{"zero revision", []jetstream.KeyValueEntry{auditWatchEntry{key: "a"}, nil}, false},
		{"unknown operation", []jetstream.KeyValueEntry{auditWatchEntry{key: "a", rev: 1, op: 99}, nil}, false},
		{"duplicate revision", []jetstream.KeyValueEntry{auditWatchEntry{key: "a", rev: 1}, auditWatchEntry{key: "a", rev: 1}, nil}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			watch := &auditWatch{updates: make(chan jetstream.KeyValueEntry, len(tc.entries))}
			for _, e := range tc.entries {
				watch.updates <- e
			}
			close(watch.updates)
			state, err := initialAuditState(context.Background(), auditWatchState{watch: watch}, func(string) bool { return true })
			if (err == nil) != tc.valid || (state != nil) != tc.valid || !watch.stopped {
				t.Fatalf("state=%v err=%v stopped=%v", state, err, watch.stopped)
			}
		})
	}
	ctx, stop := context.WithCancel(context.Background())
	stop()
	watch := &auditWatch{updates: make(chan jetstream.KeyValueEntry)}
	state, err := initialAuditState(ctx, auditWatchState{watch: watch}, func(string) bool { return true })
	if state != nil || !errors.Is(err, context.Canceled) || !watch.stopped {
		t.Fatalf("state=%v err=%v stopped=%v", state, err, watch.stopped)
	}
}

func TestInitialAuditStateTombstonesAndCohortFilter(t *testing.T) {
	watch := &auditWatch{updates: make(chan jetstream.KeyValueEntry, 7)}
	for _, e := range []jetstream.KeyValueEntry{
		auditWatchEntry{key: "old", rev: 1},
		auditWatchEntry{key: "old", rev: 2, op: jetstream.KeyValueDelete},
		auditWatchEntry{key: "purged", rev: 3, op: jetstream.KeyValuePurge},
		auditWatchEntry{key: "later", rev: 4},
		auditWatchEntry{key: "snap.old", rev: 5}, nil,
		auditWatchEntry{key: "old", rev: 6},
	} {
		watch.updates <- e
	}
	state, err := initialAuditState(context.Background(), auditWatchState{watch: watch}, func(key string) bool { return key != "later" })
	if err != nil {
		t.Fatal(err)
	}
	keys, err := state.Keys(context.Background())
	if err != nil || len(keys) != 1 || keys[0] != "snap.old" {
		t.Fatalf("keys=%v err=%v", keys, err)
	}
	for _, key := range []string{"old", "purged", "later", "missing"} {
		if _, err := state.Get(context.Background(), key); !errors.Is(err, jetstream.ErrKeyNotFound) {
			t.Fatalf("key=%s err=%v", key, err)
		}
	}
}
