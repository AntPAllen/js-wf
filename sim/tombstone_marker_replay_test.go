package sim

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"js-wf/retention"
)

// Retain raw History=1 revisions, including DEL/PURGE markers. A successful
// subject purge removes only revisions below its supplied sequence boundary.
type tombstoneMarkerPort struct {
	s        *Scheduler
	rows     map[uint64]*jetstream.RawStreamMsg
	last     uint64
	fault    string
	reuse    bool
	attempts int
}

func (p *tombstoneMarkerPort) Open(context.Context) (retention.TombstoneScanSession, error) {
	return p, nil
}
func (p *tombstoneMarkerPort) LastStateSequence(context.Context) (uint64, error) { return p.last, nil }
func (p *tombstoneMarkerPort) GetStateMessage(_ context.Context, seq uint64) (*jetstream.RawStreamMsg, error) {
	if m := p.rows[seq]; m != nil {
		return m, nil
	}
	return nil, jetstream.ErrMsgNotFound
}
func (p *tombstoneMarkerPort) StateKeys(context.Context) ([]string, error) {
	panic("paged scan enumerated keys")
}
func (p *tombstoneMarkerPort) StateValue(_ context.Context, key string) ([]byte, uint64, error) {
	for seq, m := range p.rows {
		if m.Subject == "$KV.WF_STATE."+key {
			if m.Header.Get("KV-Operation") != "" {
				return nil, 0, jetstream.ErrKeyNotFound
			}
			return m.Data, seq, nil
		}
	}
	return nil, 0, jetstream.ErrKeyNotFound
}
func (p *tombstoneMarkerPort) CurrentInvocation(context.Context, string) (uint64, error) {
	return 0, jetstream.ErrMsgNotFound
}
func (p *tombstoneMarkerPort) DeleteState(context.Context, string, uint64) error {
	return fmt.Errorf("fixture has only markers and live results")
}
func (p *tombstoneMarkerPort) DeleteStateMarker(_ context.Context, key string, seq uint64) error {
	if key != "test.retired" || seq != 2 {
		return fmt.Errorf("unsafe marker purge:%s/%d", key, seq)
	}
	p.attempts++
	if p.reuse && p.attempts == 1 {
		delete(p.rows, 2)
		p.last++
		p.rows[p.last] = &jetstream.RawStreamMsg{Subject: "$KV.WF_STATE.test.retired", Sequence: p.last, Data: []byte(`{"inv_seq":2,"result":true}`)}
		p.s.RecordTransport(TransportEvent{Operation: "marker_reuse", Sequence: p.last, Outcome: "new_generation", AtMillis: p.s.NowMillis()})
	}
	if p.attempts == 1 && p.fault == "drop" {
		p.s.RecordTransport(TransportEvent{Operation: "marker_purge", Sequence: seq, Outcome: "dropped_before_commit", AtMillis: p.s.NowMillis()})
		return nats.ErrTimeout
	}
	for revision, m := range p.rows {
		if m.Subject == "$KV.WF_STATE."+key && revision <= seq {
			delete(p.rows, revision)
		}
	}
	outcome := "acknowledged"
	if p.attempts == 1 && p.fault == "lost_ack" {
		outcome = "committed_ack_lost"
	}
	p.s.RecordTransport(TransportEvent{Operation: "marker_purge", Sequence: seq, Outcome: outcome, AtMillis: p.s.NowMillis()})
	if outcome == "committed_ack_lost" {
		return nats.ErrTimeout
	}
	return nil
}
func runTombstoneMarkerDrain(seed int64, replay *Trace) (trace Trace, runErr error) {
	s := NewScheduler(seed)
	if replay != nil {
		var err error
		s, err = ReplayScheduler(*replay)
		if err != nil {
			return trace, err
		}
	}
	defer func() { trace = s.Trace() }()
	if err := s.SetWorkload("tombstone_marker_drain"); err != nil {
		return trace, err
	}
	op, err := s.Choose([]string{"DEL", "PURGE"})
	if err != nil {
		return trace, err
	}
	fault, err := s.Choose([]string{"ack", "drop", "lost_ack"})
	if err != nil {
		return trace, err
	}
	reuse, err := s.Choose([]string{"absent", "reuse"})
	if err != nil {
		return trace, err
	}
	p := &tombstoneMarkerPort{s: s, last: 3, fault: fault, reuse: reuse == "reuse", rows: map[uint64]*jetstream.RawStreamMsg{
		1: {Subject: "$KV.WF_STATE.scan.tombstone.cursor", Sequence: 1, Header: nats.Header{"KV-Operation": []string{"DEL"}}},
		2: {Subject: "$KV.WF_STATE.test.retired", Sequence: 2, Header: nats.Header{"KV-Operation": []string{op}}},
		3: {Subject: "$KV.WF_STATE.test.protected", Sequence: 3, Data: []byte(`{"inv_seq":99,"result":true}`)},
	}}
	scan := retention.NewTombstoneScanWithPort(p)
	now := time.Unix(1700000000, 0).UTC()
	if _, err := scan.Scan(context.Background(), 1, 10, now, true); err != nil {
		return trace, err
	}
	if len(p.rows) != 3 || p.attempts != 0 {
		return trace, fmt.Errorf("dry run mutated retained state")
	}
	page, err := scan.Scan(context.Background(), 1, 10, now, false)
	if fault != "ack" {
		if err != nats.ErrTimeout || page.RetrySequence != 2 {
			return trace, fmt.Errorf("uncertain purge skipped marker:%+v %v", page, err)
		}
		_, err = scan.Scan(context.Background(), page.RetrySequence, 10, now, false)
	}
	if err != nil {
		return trace, err
	}
	if p.attempts == 0 {
		return trace, fmt.Errorf("marker cleanup never attempted")
	}
	want := 2
	if p.reuse {
		want++
	}
	if len(p.rows) != want || p.rows[1] == nil || p.rows[3] == nil || string(p.rows[3].Data) != `{"inv_seq":99,"result":true}` {
		return trace, fmt.Errorf("incorrect physical drain/protected state:%+v", p.rows)
	}
	if p.reuse && (p.rows[4] == nil || string(p.rows[4].Data) != `{"inv_seq":2,"result":true}`) {
		return trace, fmt.Errorf("new generation purged")
	}
	s.RecordTransport(TransportEvent{Operation: "characterize_marker_drain", Outcome: op + "_" + fault + "_" + reuse, AtMillis: s.NowMillis()})
	if err := s.Finish(); err != nil {
		return trace, err
	}
	return s.Trace(), nil
}
func TestTombstoneMarkerDrainReplay(t *testing.T) {
	covered := map[string]bool{}
	for seed := int64(1); seed <= 128; seed++ {
		generated, err := runTombstoneMarkerDrain(seed, nil)
		if err != nil {
			t.Fatalf("seed%d:%v", seed, err)
		}
		replayed, err := runTombstoneMarkerDrain(seed, &generated)
		if err != nil || !reflect.DeepEqual(generated, replayed) {
			t.Fatalf("seed%d replay:%v", seed, err)
		}
		for _, event := range generated.Transport {
			if event.Operation == "characterize_marker_drain" {
				if os.Getenv("SIM_WRITE_TOMBSTONE_MARKER_PINS") == "1" && !covered[event.Outcome] {
					if err := generated.Save(filepath.Join("testdata", "regressions", "tombstone-marker-"+event.Outcome+".json")); err != nil {
						t.Fatal(err)
					}
				}
				covered[event.Outcome] = true
			}
		}
	}
	if len(covered) != 12 {
		t.Fatalf("missing marker/reuse/transport cells:%v", covered)
	}
}
