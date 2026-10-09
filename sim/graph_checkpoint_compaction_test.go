package sim

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"js-wf/client"
	"js-wf/internal/checkpoint"
	"js-wf/internal/graphpublication"
	"js-wf/journal"
)

var graphCompactionModes = []string{"normal", "protobuf", "cas-drop", "cas-lost", "cas-unconfirmed", "append-race", "reader-race", "collector-race", "retire-race", "put-drop", "put-lost", "release-drop", "release-lost", "release-unconfirmed"}

type compactionFaultPort struct {
	*GraphPublicationTransport
	mode  string
	armed bool
	race  func() error
}

func (p *compactionFaultPort) CASRoot(ctx context.Context, key string, head uint64, root graphpublication.Root) (graphpublication.Root, error) {
	var app struct {
		RetainedFrom uint64 `json:"retained_from"`
	}
	if err := json.Unmarshal(root.Application, &app); err != nil {
		return graphpublication.Root{}, err
	}
	release := p.mode == "release-drop" || p.mode == "release-lost" || p.mode == "release-unconfirmed"
	match := app.RetainedFrom > 0
	if release {
		old, err := p.GraphPublicationTransport.ReadRoot(ctx, key)
		if err != nil {
			return graphpublication.Root{}, err
		}
		match = len(old.Readers) > 0 && len(root.Readers) == 0
	}
	if !p.armed || !match {
		return p.GraphPublicationTransport.CASRoot(ctx, key, head, root)
	}
	p.armed = false
	if p.race != nil {
		if err := p.race(); err != nil {
			return graphpublication.Root{}, err
		}
	}
	if p.mode == "cas-drop" || p.mode == "release-drop" {
		if err := p.QueueFault("cas_root", DropBeforeCommit); err != nil {
			return graphpublication.Root{}, err
		}
	}
	if p.mode == "cas-lost" || p.mode == "cas-unconfirmed" || p.mode == "release-lost" || p.mode == "release-unconfirmed" {
		if err := p.QueueFault("cas_root", LoseAckAfterCommit); err != nil {
			return graphpublication.Root{}, err
		}
	}
	ack, err := p.GraphPublicationTransport.CASRoot(ctx, key, head, root)
	if err != nil && (p.mode == "cas-unconfirmed" || p.mode == "release-unconfirmed") {
		if queued := p.QueueFault("read_root", DropBeforeCommit); queued != nil {
			return graphpublication.Root{}, queued
		}
	}
	return ack, err
}

func (p *compactionFaultPort) Put(ctx context.Context, name string, data []byte) error {
	if p.armed && (p.mode == "put-drop" || p.mode == "put-lost") {
		p.armed = false
		fault := DropBeforeCommit
		if p.mode == "put-lost" {
			fault = LoseAckAfterCommit
		}
		if err := p.QueueFault("put", fault); err != nil {
			return err
		}
	}
	return p.GraphPublicationTransport.Put(ctx, name, data)
}

func runGraphCompaction(seed int64, replay *Trace) (trace Trace, runErr error) {
	s := NewScheduler(seed)
	if replay != nil {
		var err error
		s, err = ReplayScheduler(*replay)
		if err != nil {
			return trace, err
		}
	}
	if err := s.SetWorkload("graph_checkpoint_compaction"); err != nil {
		return trace, err
	}
	defer func() { trace = s.Trace() }()
	mode, err := s.Choose(graphCompactionModes)
	if err != nil {
		return trace, err
	}
	ctx := context.Background()
	m := NewGraphPublicationTransport(s)
	port := &compactionFaultPort{GraphPublicationTransport: m, mode: mode}
	protocol := m.Protocol()
	protocol.Port = port
	now := func() time.Time { return time.UnixMilli(s.NowMillis()).UTC() }
	encoding := journal.JSON
	if mode == "protobuf" {
		encoding = journal.ProtobufV1
	}
	store, err := journal.NewGraphStore(journal.GraphConfig{Protocol: protocol, Now: now, PinTTL: 3 * time.Second, IntentTTL: time.Second, Encoding: encoding, CanonicalStarts: true, CanonicalSignals: true, CheckpointIndex: true, ArchiveCheckpoints: true})
	if err != nil {
		return trace, err
	}
	source := NewSignalTransport(s)
	c, err := client.NewWithSignalPorts(source, source).WithGraphJournal(store)
	if err != nil {
		return trace, err
	}
	h, err := c.Start(ctx, "flow", "compact", []byte(`7`))
	if err != nil {
		return trace, err
	}
	status, err := store.InspectStart(ctx, h.Type, h.ID)
	if err != nil {
		return trace, err
	}
	tail, err := store.Begin(ctx, h.Type, h.ID, h.InvSeq)
	if err != nil {
		return trace, err
	}
	index := uint64(0)
	appendRecord := func(kind journal.Kind, payload []byte, objects ...[]byte) error {
		var err error
		tail, err = store.Append(ctx, h.Type, h.ID, h.InvSeq, journal.Entry{Kind: kind, Index: index, Epoch: 3, Payload: payload}, tail, objects, nil)
		if err == nil {
			index++
		}
		return err
	}
	started, _ := json.Marshal(map[string]string{"input_sha256": status.State.Start.InputSHA256})
	if err = appendRecord(journal.Started, started, []byte(`7`)); err != nil {
		return trace, err
	}
	for i := 0; i < 2; i++ {
		if err = appendRecord(journal.StepRequested, []byte(fmt.Sprintf(`{"kind":"run","name":"padding%d"}`, i))); err != nil {
			return trace, err
		}
		if err = appendRecord(journal.StepCompleted, []byte(`{"result":7}`)); err != nil {
			return trace, err
		}
	}
	locals := json.RawMessage(`{"value":42}`)
	digest := sha256.Sum256(locals)
	frame := checkpoint.Frame{Version: checkpoint.Version, Identity: checkpoint.Identity{Type: h.Type, ID: h.ID, InvSeq: h.InvSeq}, Stage: "next", Data: locals, Anchor: checkpoint.Anchor{Index: index + 1, Epoch: 3}, StepPosition: index + 1}
	body, hash, err := checkpoint.Encode(frame)
	if err != nil {
		return trace, err
	}
	request, _ := json.Marshal(map[string]string{"kind": "checkpoint", "name": "next", "input_hash": hex.EncodeToString(digest[:])})
	completion, _ := json.Marshal(map[string]string{"result_ref": "step-result-" + hash, "result_hash": hash})
	if err = appendRecord(journal.StepRequested, request); err != nil {
		return trace, err
	}
	if err = appendRecord(journal.StepCompleted, completion, body); err != nil {
		return trace, err
	}
	if err = appendRecord(journal.Suspended, []byte(`{"waiting_on":"continuation:next"}`)); err != nil {
		return trace, err
	}
	view, err := store.Open(ctx, h.Type, h.ID, h.InvSeq)
	if err != nil {
		return trace, err
	}
	found, err := view.ReadCheckpoint(ctx, h.Type, h.ID)
	if err != nil || found == nil {
		return trace, fmt.Errorf("checkpoint fixture: %v", err)
	}
	if err = view.Close(ctx); err != nil {
		return trace, err
	}
	if err = store.PublishCheckpoint(ctx, h.Type, h.ID, found.Runtime, tail); err != nil {
		return trace, err
	}
	original, originalTail, err := store.Read(ctx, h.Type, h.ID, h.InvSeq)
	if err != nil {
		return trace, err
	}
	var held *journal.GraphView
	if mode == "append-race" {
		port.race = func() error { return appendRecord(journal.Suspended, []byte(`{"waiting_on":"continuation:next"}`)) }
	}
	if mode == "reader-race" {
		port.race = func() error { var err error; held, err = store.Open(ctx, h.Type, h.ID, h.InvSeq); return err }
	}
	if mode == "collector-race" {
		port.race = func() error {
			if err := s.AdvanceMillis(4000); err != nil {
				return err
			}
			_, err := protocol.SweepWithReaders(ctx, now())
			return err
		}
	}
	if mode == "retire-race" {
		held, err = store.Open(ctx, h.Type, h.ID, h.InvSeq)
		if err != nil {
			return trace, err
		}
		port.race = func() error {
			if err := appendRecord(journal.Completed, []byte(`{"result":42}`)); err != nil {
				return err
			}
			return store.Retire(ctx, h.Type, h.ID, h.InvSeq, tail)
		}
	}
	port.armed = true
	err = store.CompactCheckpoint(ctx, h.Type, h.ID, found.Runtime, tail)
	success := mode == "normal" || mode == "protobuf" || mode == "cas-lost" || mode == "release-lost"
	if success && err != nil {
		return trace, fmt.Errorf("confirmed %s: %w", mode, err)
	}
	if !success && err == nil {
		return trace, fmt.Errorf("uncertain/racing %s certified success", mode)
	}
	if mode == "append-race" || mode == "reader-race" || mode == "collector-race" || mode == "retire-race" {
		if !errors.Is(err, journal.ErrStale) {
			return trace, fmt.Errorf("race not stale: %w", err)
		}
	} else if !success && !errors.Is(err, journal.ErrUnknown) {
		return trace, fmt.Errorf("uncertainty not classified: %w", err)
	}
	port.armed = false
	keys, readErr := m.RootKeys(ctx)
	if readErr != nil || len(keys) != 1 {
		return trace, fmt.Errorf("compaction root census: %v %v", keys, readErr)
	}
	actual, readErr := m.ReadRoot(ctx, keys[0])
	if readErr != nil {
		return trace, readErr
	}
	var cursor struct {
		RetainedFrom uint64 `json:"retained_from"`
	}
	if readErr = json.Unmarshal(actual.Application, &cursor); readErr != nil {
		return trace, readErr
	}
	committed := success || mode == "cas-unconfirmed"
	if committed && cursor.RetainedFrom != found.Request.Index || !committed && cursor.RetainedFrom != 0 {
		return trace, fmt.Errorf("unexpected actual compaction after %s: %d", mode, cursor.RetainedFrom)
	}
	if mode == "release-drop" && len(actual.Readers) != 1 {
		return trace, fmt.Errorf("uncertain release lost its durable pin")
	}
	if mode == "retire-race" {
		for i, expected := range original {
			got, err := held.Read(ctx, uint64(i))
			if err != nil || !reflect.DeepEqual(got.Record, expected) {
				return trace, fmt.Errorf("retired pin %d: %v", i, err)
			}
		}
		if _, err = store.Open(ctx, h.Type, h.ID, h.InvSeq); !errors.Is(err, journal.ErrStale) {
			return trace, fmt.Errorf("retired generation reopened: %v", err)
		}
		if err = held.Close(ctx); err != nil {
			return trace, err
		}
	} else {
		if held != nil {
			if err = held.Close(ctx); err != nil {
				return trace, err
			}
		}
		if err = store.CompactCheckpoint(ctx, h.Type, h.ID, found.Runtime, tail); err != nil {
			return trace, fmt.Errorf("fresh compaction retry %s: %w", mode, err)
		}
		got, gotTail, err := store.Read(ctx, h.Type, h.ID, h.InvSeq)
		if err != nil || gotTail != tail || len(got) < len(original) || !reflect.DeepEqual(got[:len(original)], original) {
			return trace, fmt.Errorf("logical audit changed: %v", err)
		}
		if mode != "append-race" && gotTail != originalTail {
			return trace, fmt.Errorf("logical tail changed")
		}
		if err = s.AdvanceMillis(4000); err != nil {
			return trace, err
		}
		if _, err = protocol.SweepWithReaders(ctx, now()); err != nil {
			return trace, err
		}
		fresh, err := store.Open(ctx, h.Type, h.ID, h.InvSeq)
		if err != nil {
			return trace, err
		}
		cp, err := fresh.ReadCheckpoint(ctx, h.Type, h.ID)
		if err != nil || cp == nil || cp.Runtime != found.Runtime {
			return trace, fmt.Errorf("checkpoint after collection: %v", err)
		}
		if err = fresh.Close(ctx); err != nil {
			return trace, err
		}
		if err = appendRecord(journal.Completed, []byte(`{"result":42}`)); err != nil {
			return trace, err
		}
		if err = store.Retire(ctx, h.Type, h.ID, h.InvSeq, tail); err != nil {
			return trace, err
		}
	}
	if err = s.AdvanceMillis(10000); err != nil {
		return trace, err
	}
	if _, err = protocol.SweepWithReaders(ctx, now()); err != nil {
		return trace, err
	}
	objects, err := m.Objects(ctx)
	if err != nil || len(objects) != 0 {
		return trace, fmt.Errorf("physical compaction drain %d: %v", len(objects), err)
	}
	if err = m.CheckReferences(); err != nil {
		return trace, err
	}
	s.RecordTransport(TransportEvent{Operation: "check_graph_checkpoint_compaction", Outcome: mode})
	return trace, s.Finish()
}

func TestSeededGraphCheckpointCompactionReplay(t *testing.T) {
	observed := map[string]int{}
	fail := func(seed int64, trace Trace, err error) {
		path := os.Getenv("FAULT_TRACE_OUT")
		if path == "" {
			dir, e := os.MkdirTemp("", "js-wf-compaction-failure-")
			if e != nil {
				t.Fatal(e)
			}
			path = filepath.Join(dir, "trace.json")
		}
		if e := trace.Save(path); e != nil {
			t.Fatal(e)
		}
		t.Fatalf("FAULT_SEED=%d FAULT_TRACE=%s: %v", seed, path, err)
	}
	for seed := range seededSchedules(t) {
		generated, err := runGraphCompaction(seed, nil)
		if err != nil {
			fail(seed, generated, err)
		}
		mode := generated.Decisions[0].Chosen
		observed[mode]++
		replayed, err := runGraphCompaction(seed, &generated)
		if err != nil || !reflect.DeepEqual(generated, replayed) {
			fail(seed, generated, fmt.Errorf("compaction replay differs: %v", err))
		}
		if dir := os.Getenv("SIM_GRAPH_COMPACTION_ROOT"); dir != "" && observed[mode] == 1 {
			if err = generated.Save(filepath.Join(dir, mode+".json")); err != nil {
				t.Fatal(err)
			}
		}
	}
	if len(observed) != len(graphCompactionModes) {
		t.Fatalf("compaction coverage=%v", observed)
	}
	t.Logf("graph compaction: modes=%v; exact replay, confirmed/unknown CAS, reader release, racing head, orphan drain and checkpoint recovery", observed)
}
