package sim

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"js-wf/reconcile"
	"js-wf/retention"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"testing"
	"time"
)

type partialTombstonePort struct {
	partialStartPort
	TombstoneSweepTransport
	replace, replaced bool
}

func (p *partialTombstonePort) DeleteStateMarker(context.Context, string, uint64) error {
	return fmt.Errorf("partial-cursor model exposes delete markers as holes")
}

func (p *partialTombstonePort) Open(context.Context) (retention.TombstoneScanSession, error) {
	p.deadline = p.schedule.NowMillis() + 5000
	for _, op := range []string{"partial_tombstone_open_state", "partial_tombstone_open_stream", "partial_tombstone_open_invocation"} {
		if err := p.request(op, 0); err != nil {
			return nil, err
		}
	}
	return p, nil
}
func (p *partialTombstonePort) LastStateSequence(context.Context) (uint64, error) {
	if err := p.request("partial_tombstone_last", 0); err != nil {
		return 0, err
	}
	p.State.mu.Lock()
	defer p.State.mu.Unlock()
	return p.State.revision, nil
}
func (p *partialTombstonePort) GetStateMessage(_ context.Context, seq uint64) (*jetstream.RawStreamMsg, error) {
	if err := p.request("partial_tombstone_stream_read", seq); err != nil {
		return nil, err
	}
	// The History=1 model exposes the current live revision; superseded values
	// and delete markers are holes here, both safe cursor-advance decisions.
	p.State.mu.Lock()
	defer p.State.mu.Unlock()
	for key, item := range p.State.items {
		if item.revision == seq {
			return &jetstream.RawStreamMsg{Subject: "$KV.WF_STATE." + key, Sequence: seq, Data: append([]byte(nil), item.value...)}, nil
		}
	}
	return nil, jetstream.ErrMsgNotFound
}
func (p *partialTombstonePort) StateValue(ctx context.Context, key string) ([]byte, uint64, error) {
	if err := p.request("partial_tombstone_value", 0); err != nil {
		return nil, 0, err
	}
	return p.TombstoneSweepTransport.StateValue(ctx, key)
}
func (p *partialTombstonePort) CurrentInvocation(ctx context.Context, subject string) (uint64, error) {
	if err := p.request("partial_tombstone_invocation", 0); err != nil {
		return 0, err
	}
	return p.TombstoneSweepTransport.CurrentInvocation(ctx, subject)
}
func (p *partialTombstonePort) DeleteState(ctx context.Context, key string, revision uint64) error {
	if key != "test.prefix-35" {
		return fmt.Errorf("deleted protected key:%s", key)
	}
	if err := p.request("partial_tombstone_delete", revision); err != nil {
		return err
	}
	if p.replace && !p.replaced {
		if _, err := p.Invocations.PublishInvocation(ctx, &nats.Msg{Subject: "wf.inv.test.prefix-35", Data: []byte("null")}); err != nil {
			return err
		}
		if _, err := p.State.Update(ctx, key, []byte(`{"inv_seq":2,"result":true}`), revision); err != nil {
			return err
		}
		p.replaced = true
	}
	err := p.TombstoneSweepTransport.DeleteState(ctx, key, revision)
	if errors.Is(err, ErrTransportLost) {
		return fmt.Errorf("%w: %v", nats.ErrTimeout, err)
	}
	return err
}

type tombstoneProgressLoop struct {
	*LoopTransport
	port   *partialTombstonePort
	stop   context.CancelFunc
	policy string
}

func (p *tombstoneProgressLoop) Wait(ctx context.Context, delay time.Duration) error {
	if p.policy == "checkpoint_prefix" {
		entry, err := p.port.State.Get(context.Background(), "test.prefix-35")
		if errors.Is(err, jetstream.ErrKeyNotFound) || (err == nil && p.port.replace && string(entry.Value) == `{"inv_seq":2,"result":true}`) {
			p.stop()
		}
	}
	return p.LoopTransport.Wait(ctx, delay)
}
func runTombstonePartialCursor(seed int64, replay *Trace) (trace Trace, runErr error) {
	s := NewScheduler(seed)
	if replay != nil {
		var err error
		s, err = ReplayScheduler(*replay)
		if err != nil {
			return Trace{}, err
		}
	}
	if err := s.SetWorkload("tombstone_partial_cursor"); err != nil {
		return Trace{}, err
	}
	defer func() { trace = s.Trace() }()
	policy, err := s.Choose([]string{"discard_prefix", "checkpoint_prefix"})
	if err != nil {
		return trace, err
	}
	cost, err := s.Choose([]string{"100ms", "150ms"})
	if err != nil {
		return trace, err
	}
	deletion, err := s.Choose([]string{"ack", "drop", "lost_ack", "revision_changed"})
	if err != nil {
		return trace, err
	}
	save, err := s.Choose([]string{"ack", "drop", "lost_ack"})
	if err != nil {
		return trace, err
	}
	state := NewKVTransport(s, 0)
	inv := NewStartTransport(s)
	port := &partialTombstonePort{partialStartPort: partialStartPort{schedule: s, cost: 100}, TombstoneSweepTransport: TombstoneSweepTransport{Invocations: inv, State: state}, replace: deletion == "revision_changed"}
	if cost == "150ms" {
		port.cost = 150
	}
	base := time.Unix(1700000000, 0).UTC()
	protected := map[string][]byte{}
	for seq := 1; seq <= 35; seq++ {
		key := fmt.Sprintf("test.prefix-%d", seq)
		value := []byte(`{"inv_seq":1,"result":true}`)
		if seq == 3 {
			key = "scan.fixture.cursor"
			value = []byte("1")
		}
		if seq == 2 || seq == 4 || seq == 35 {
			expires := base.Add(-time.Minute)
			if seq == 2 {
				expires = base.Add(time.Hour)
			}
			value, _ = json.Marshal(retention.Tombstone{Tombstone: true, InvSeq: 1, PurgedAt: base.Add(-time.Hour), ExpiresAt: expires})
		}
		if _, err := state.Create(context.Background(), key, value); err != nil {
			return trace, err
		}
		if seq < 35 {
			protected[key] = value
		}
	}
	if _, err := inv.PublishInvocation(context.Background(), &nats.Msg{Subject: "wf.inv.test.prefix-4", Data: []byte("null")}); err != nil {
		return trace, err
	}
	if err := state.Delete(context.Background(), "test.prefix-7", 7); err != nil {
		return trace, err
	}
	delete(protected, "test.prefix-7")
	protected["test.prefix-11"] = []byte(`{"inv_seq":99,"result":true}`)
	if _, err := state.Update(context.Background(), "test.prefix-11", protected["test.prefix-11"], 11); err != nil {
		return trace, err
	}
	if deletion == "drop" || deletion == "lost_ack" {
		kind := KVDropBeforeCommit
		if deletion == "lost_ack" {
			kind = KVLoseAckAfterCommit
		}
		if err := state.QueueFault(KVFault{Operation: "delete", Kind: kind}); err != nil {
			return trace, err
		}
	}
	loop := NewLoopTransport(s)
	loop.cursors = state // Same underlying WF_STATE revision space as the scanner.
	if save != "ack" {
		kind := KVDropBeforeCommit
		if save == "lost_ack" {
			kind = KVLoseAckAfterCommit
		}
		if err := loop.RejectCursorSaveAt(1, kind); err != nil {
			return trace, err
		}
	}
	ctx, stop := context.WithCancel(context.Background())
	defer stop()
	loop.StopAfterWaits(8, stop)
	runner := &tombstoneProgressLoop{LoopTransport: loop, port: port, stop: stop, policy: policy}
	now := func() time.Time { return base.Add(time.Duration(s.NowMillis()) * time.Millisecond) }
	if policy == "checkpoint_prefix" {
		err = reconcile.RunTombstoneLoopWithPorts(ctx, runner, port, "partial-retention", time.Second, 500, now)
	} else {
		scanner := retention.NewTombstoneScanWithPort(port)
		err = reconcile.RunLoopWithPort(ctx, runner, "partial-retention", "tombstone", time.Second, 500, func(ctx context.Context, next uint64, budget int, dry bool) (reconcile.ScanResult, error) {
			page, err := scanner.Scan(ctx, next, budget, now(), dry)
			// The earlier production bridge discarded completed-prefix metadata.
			return reconcile.ScanResult{NextSequence: page.NextSequence, Inspected: page.Inspected, Removed: page.Deleted}, err
		})
	}
	if err != nil {
		return trace, err
	}
	cursor, _, err := loop.LoadCursor(context.Background(), "tombstone")
	if err != nil {
		return trace, err
	}
	current, getErr := state.Get(context.Background(), "test.prefix-35")
	resolved := errors.Is(getErr, jetstream.ErrKeyNotFound)
	if port.replace {
		resolved = getErr == nil && string(current.Value) == `{"inv_seq":2,"result":true}`
	}
	if policy == "discard_prefix" {
		if resolved || cursor != 1 {
			return trace, fmt.Errorf("legacy control advanced:resolved=%t cursor=%d", resolved, cursor)
		}
	} else if !resolved || s.NowMillis() >= 30000 {
		return trace, fmt.Errorf("prefix checkpoint fails progress:resolved=%t cursor=%d at=%d", resolved, cursor, s.NowMillis())
	}
	keys := make([]string, 0, len(protected))
	for key := range protected {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		value := protected[key]
		entry, err := state.Get(context.Background(), key)
		if err != nil || !bytes.Equal(entry.Value, value) {
			return trace, fmt.Errorf("protected state changed:%s %+v %v", key, entry, err)
		}
	}
	s.RecordTransport(TransportEvent{Operation: "characterize_partial_cursor", Outcome: policy + "_" + save + "_" + deletion, AtMillis: s.NowMillis()})
	if err := s.Finish(); err != nil {
		return trace, err
	}
	return s.Trace(), nil
}
func TestTombstonePartialCursorReplay(t *testing.T) {
	covered := map[string]bool{}
	for seed := int64(1); seed <= 256; seed++ {
		generated, err := runTombstonePartialCursor(seed, nil)
		if err != nil {
			path := os.Getenv("FAULT_TRACE_OUT")
			if path == "" {
				path = filepath.Join(t.TempDir(), "partial-cursor.json")
			}
			_ = generated.Save(path)
			t.Fatalf("seed%d trace=%s:%v", seed, path, err)
		}
		replayed, err := runTombstonePartialCursor(seed, &generated)
		if err != nil || !reflect.DeepEqual(generated, replayed) {
			t.Fatalf("seed%d replay:%v", seed, err)
		}
		for _, event := range generated.Transport {
			if event.Operation == "characterize_partial_cursor" {
				if os.Getenv("SIM_WRITE_TOMBSTONE_PARTIAL_CURSOR_PINS") == "1" && !covered[event.Outcome] {
					if err := generated.Save(filepath.Join("testdata", "regressions", "tombstone-partial-"+event.Outcome+".json")); err != nil {
						t.Fatal(err)
					}
				}
				covered[event.Outcome] = true
			}
		}
	}
	if len(covered) != 24 {
		t.Fatalf("missing cursor/ack policy cells:%v", covered)
	}
}
