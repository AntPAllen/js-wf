package sim

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"js-wf/internal/graphpublication"
	"js-wf/journal"
)

var graphJournalModes = []string{"json", "protobuf", "lost_append", "dropped_append", "unknown_readback", "collector_wins", "retirement_lost", "old_reader", "reader_expiry"}

func runGraphJournal(seed int64, replay *Trace) (trace Trace, runErr error) {
	s := NewScheduler(seed)
	if replay != nil {
		var err error
		s, err = ReplayScheduler(*replay)
		if err != nil {
			return Trace{}, err
		}
	}
	if err := s.SetWorkload("graph_journal_generation"); err != nil {
		return Trace{}, err
	}
	defer func() { trace = s.Trace() }()
	mode, err := s.Choose(graphJournalModes)
	if err != nil {
		return trace, err
	}
	m := NewGraphPublicationTransport(s)
	ctx := context.Background()
	now := func() time.Time { return time.UnixMilli(s.NowMillis()).UTC() }
	encoding := journal.JSON
	if mode == "protobuf" {
		encoding = journal.ProtobufV1
	}
	store, err := journal.NewGraphStore(journal.GraphConfig{Protocol: m.Protocol(), Now: now, PinTTL: 3 * time.Second, IntentTTL: time.Second, Encoding: encoding})
	if err != nil {
		return trace, err
	}
	tail, err := store.Begin(ctx, "flow", "model", 10)
	if err != nil {
		return trace, err
	}
	tail, err = store.Append(ctx, "flow", "model", 10, journal.Entry{Kind: journal.Started, Epoch: 1}, tail, [][]byte{[]byte("input")}, nil)
	if err != nil {
		return trace, err
	}
	view, err := store.Open(ctx, "flow", "model", 10)
	if err != nil {
		return trace, err
	}
	first, err := view.Read(ctx, 0)
	if err != nil || first.Sequence != 1 || len(first.Blobs) != 1 {
		return trace, fmt.Errorf("invalid initial journal edge: %v", err)
	}
	completed := journal.Entry{Kind: journal.Completed, Index: 1, Epoch: 2, Payload: []byte(`"result"`)}
	switch mode {
	case "lost_append", "unknown_readback":
		if err = m.QueueFault("cas_root", LoseAckAfterCommit); err != nil {
			return trace, err
		}
		if mode == "unknown_readback" {
			m.PauseBefore("cas_root", func() error { return m.QueueFault("read_root", DropBeforeCommit) })
		}
	case "dropped_append":
		if err = m.QueueFault("cas_root", DropBeforeCommit); err != nil {
			return trace, err
		}
	case "collector_wins":
		m.PauseBefore("cas_root", func() error {
			if e := s.AdvanceMillis(1000); e != nil {
				return e
			}
			_, e := m.Protocol().SweepWithReaders(ctx, now())
			return e
		})
	}
	committed, err := store.Append(ctx, "flow", "model", 10, completed, tail, nil, []graphpublication.OwnedPayload{{Index: 0, Link: first.Blobs[0]}})
	switch mode {
	case "dropped_append", "unknown_readback":
		if !errors.Is(err, journal.ErrUnknown) {
			return trace, fmt.Errorf("unknown append misclassified: %v", err)
		}
	case "collector_wins":
		if !errors.Is(err, journal.ErrStale) {
			return trace, fmt.Errorf("collector did not fence original head: %v", err)
		}
	default:
		if err != nil || committed != 2 {
			return trace, fmt.Errorf("terminal append failed: %v", err)
		}
	}
	records, tail, err := store.Read(ctx, "flow", "model", 10)
	if err != nil {
		return trace, err
	}
	if mode == "dropped_append" || mode == "collector_wins" {
		if len(records) != 1 || tail != 1 {
			return trace, fmt.Errorf("failed append changed logical journal")
		}
		tail, err = store.Append(ctx, "flow", "model", 10, completed, tail, nil, []graphpublication.OwnedPayload{{Index: 0, Link: first.Blobs[0]}})
		if err != nil {
			return trace, err
		}
	} else if len(records) != 2 || tail != 2 || !bytes.Equal(records[1].Payload, completed.Payload) {
		return trace, fmt.Errorf("complete logical replay differs")
	}
	if mode == "retirement_lost" {
		if err = m.QueueFault("cas_root", LoseAckAfterCommit); err != nil {
			return trace, err
		}
	}
	if err = store.Retire(ctx, "flow", "model", 10, tail); err != nil {
		return trace, err
	}
	tail, err = store.Begin(ctx, "flow", "model", 11)
	if err != nil || tail != 2 {
		return trace, fmt.Errorf("logical high-water reset: %v", err)
	}
	if _, err = store.Append(ctx, "flow", "model", 10, journal.Entry{Kind: journal.Started}, 0, nil, nil); !errors.Is(err, journal.ErrStale) {
		return trace, fmt.Errorf("old generation revived")
	}
	if err = s.AdvanceMillis(1000); err != nil {
		return trace, err
	}
	if _, err = m.Protocol().SweepWithReaders(ctx, now()); err != nil {
		return trace, err
	}
	input, err := view.Payload(ctx, 0, first.Blobs[0], 100)
	if err != nil || string(input) != "input" {
		return trace, fmt.Errorf("old retained input lost: %v", err)
	}
	if mode == "old_reader" {
		if err = view.Renew(ctx); err != nil {
			return trace, err
		}
	}
	if mode == "reader_expiry" {
		if err = s.AdvanceMillis(3000); err != nil {
			return trace, err
		}
		if _, err = m.Protocol().SweepWithReaders(ctx, now()); err != nil {
			return trace, err
		}
		if _, err = view.Read(ctx, 0); !errors.Is(err, graphpublication.ErrRevoked) {
			return trace, fmt.Errorf("expired journal view succeeded: %v", err)
		}
	} else if err = view.Close(ctx); err != nil {
		return trace, err
	}
	tail, err = store.Append(ctx, "flow", "model", 11, journal.Entry{Kind: journal.Started}, tail, nil, nil)
	if err != nil || tail != 3 {
		return trace, fmt.Errorf("new generation first entry failed: %v", err)
	}
	tail, err = store.Append(ctx, "flow", "model", 11, journal.Entry{Kind: journal.Failed, Index: 1, Epoch: 1}, tail, nil, nil)
	if err != nil || tail != 4 {
		return trace, fmt.Errorf("new generation second entry failed: %v", err)
	}
	if err = store.Retire(ctx, "flow", "model", 11, tail); err != nil {
		return trace, err
	}
	if err = s.AdvanceMillis(10000); err != nil {
		return trace, err
	}
	if _, err = m.Protocol().SweepWithReaders(ctx, now()); err != nil {
		return trace, err
	}
	objects, err := m.Objects(ctx)
	if err != nil || len(objects) != 0 {
		return trace, fmt.Errorf("journal objects not drained: %v", err)
	}
	if err = m.CheckReferences(); err != nil {
		return trace, err
	}
	s.RecordTransport(TransportEvent{Operation: "check_graph_journal_generation", Outcome: mode})
	if err = s.Finish(); err != nil {
		return trace, err
	}
	return trace, nil
}

func TestSeededGraphJournalReplay(t *testing.T) {
	observed := map[string]int{}
	fail := func(seed int64, trace Trace, cause error) {
		path := os.Getenv("FAULT_TRACE_OUT")
		if path == "" {
			dir, err := os.MkdirTemp("", "js-wf-graph-journal-failure-")
			if err != nil {
				t.Fatal(err)
			}
			path = filepath.Join(dir, "trace.json")
		}
		if err := trace.Save(path); err != nil {
			t.Fatal(err)
		}
		t.Fatalf("FAULT_SEED=%d FAULT_TRACE=%s: %v", seed, path, cause)
	}
	for seed := range seededSchedules(t) {
		generated, err := runGraphJournal(seed, nil)
		if err != nil {
			fail(seed, generated, err)
		}
		mode := generated.Decisions[0].Chosen
		observed[mode]++
		replayed, err := runGraphJournal(seed, &generated)
		if err != nil || !reflect.DeepEqual(generated, replayed) {
			fail(seed, generated, fmt.Errorf("graph journal replay differs: %v", err))
		}
		if dir := os.Getenv("SIM_GRAPH_JOURNAL_ROOT"); dir != "" && observed[mode] == 1 {
			if err = generated.Save(filepath.Join(dir, mode+".json")); err != nil {
				t.Fatal(err)
			}
		}
	}
	if len(observed) != len(graphJournalModes) {
		t.Fatalf("journal coverage=%v", observed)
	}
	t.Logf("graph journal: modes=%v; exact replay, logical sequence, generations, retained input, unknown ACKs and complete drain", observed)
}
