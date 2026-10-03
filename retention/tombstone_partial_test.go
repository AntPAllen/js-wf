package retention

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

type tombstonePrefixSession struct {
	stage   string
	deletes int
}

func (p *tombstonePrefixSession) DeleteStateMarker(context.Context, string, uint64) error {
	panic("fixture has no raw delete markers")
}

func (p *tombstonePrefixSession) Open(context.Context) (TombstoneScanSession, error) {
	if p.stage == "open" {
		return nil, nats.ErrTimeout
	}
	return p, nil
}
func (p *tombstonePrefixSession) LastStateSequence(context.Context) (uint64, error) {
	if p.stage == "info" {
		return 0, nats.ErrTimeout
	}
	return 3, nil
}
func (p *tombstonePrefixSession) GetStateMessage(_ context.Context, seq uint64) (*jetstream.RawStreamMsg, error) {
	if p.stage == "first_read" || (seq == 3 && (p.stage == "read" || p.stage == "hole_prefix" || p.stage == "absent_prefix" || p.stage == "changed_prefix" || p.stage == "dry_prefix" || p.stage == "ack_delete_prefix")) {
		return nil, nats.ErrTimeout
	}
	if seq == 1 && p.stage == "hole_prefix" {
		return nil, jetstream.ErrMsgNotFound
	}
	subject := fmt.Sprintf("$KV.WF_STATE.test.prefix-%d", seq)
	if seq == 1 {
		subject = "$KV.WF_STATE.scan.fixture.cursor"
	}
	return &jetstream.RawStreamMsg{Subject: subject, Sequence: seq}, nil
}
func (p *tombstonePrefixSession) StateKeys(context.Context) ([]string, error) {
	panic("paged scanner must not enumerate keys")
}
func (p *tombstonePrefixSession) StateValue(_ context.Context, key string) ([]byte, uint64, error) {
	seq := uint64(3)
	if key == "test.prefix-2" {
		seq = 2
	}
	if seq == 2 && p.stage == "absent_prefix" {
		return nil, 0, jetstream.ErrKeyNotFound
	}
	if seq == 2 && p.stage == "changed_prefix" {
		return []byte(`{"inv_seq":2,"result":true}`), 4, nil
	}
	if seq == 2 && p.stage != "dry_prefix" && p.stage != "ack_delete_prefix" {
		return []byte(`{"inv_seq":1,"result":true}`), 2, nil
	}
	if seq == 3 && p.stage == "value" {
		return nil, 0, nats.ErrTimeout
	}
	if seq == 3 && p.stage == "decode" {
		return []byte(`{bad`), seq, nil
	}
	now := time.Now().UTC()
	value, _ := json.Marshal(Tombstone{Tombstone: true, InvSeq: 1, PurgedAt: now.Add(-time.Hour), ExpiresAt: now.Add(-time.Minute)})
	return value, seq, nil
}
func (p *tombstonePrefixSession) CurrentInvocation(context.Context, string) (uint64, error) {
	if p.stage == "generation" {
		return 0, nats.ErrTimeout
	}
	return 0, jetstream.ErrMsgNotFound
}
func (p *tombstonePrefixSession) DeleteState(_ context.Context, key string, _ uint64) error {
	p.deletes++
	if p.stage == "delete" && key == "test.prefix-3" {
		return nats.ErrTimeout
	}
	return nil
}
func TestTombstonePartialCursorDoesNotSkipUncertainReadGenerationOrDelete(t *testing.T) {
	for _, stage := range []string{"open", "info", "first_read", "read", "value", "decode", "generation", "delete", "hole_prefix", "absent_prefix", "changed_prefix", "dry_prefix", "ack_delete_prefix"} {
		t.Run(stage, func(t *testing.T) {
			port := &tombstonePrefixSession{stage: stage}
			page, err := NewTombstoneScanWithPort(port).Scan(context.Background(), 1, 500, time.Now().UTC(), stage == "dry_prefix")
			if err == nil {
				t.Fatal("injected error not observed")
			}
			want := uint64(3)
			if stage == "open" || stage == "info" || stage == "first_read" {
				want = 0
			}
			if page.RetrySequence != want {
				t.Fatalf("retry=%d want%d page=%+v", page.RetrySequence, want, page)
			}
			if stage == "dry_prefix" && port.deletes != 0 {
				t.Fatal("dry run deleted state")
			}
			if stage == "ack_delete_prefix" && (port.deletes != 1 || page.Deleted != 1) {
				t.Fatalf("acknowledged prefix delete not reported:%+v calls%d", page, port.deletes)
			}
		})
	}
}
