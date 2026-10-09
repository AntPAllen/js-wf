package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"js-wf/client"
	"js-wf/lease"
	"js-wf/retention"
)

type graphProjectionModel struct {
	value      []byte
	revision   uint64
	updates    int
	mode       string
	tomb       []byte
	staleValue []byte
}

func (p *graphProjectionModel) Get(context.Context, string) (lease.KVEntry, error) {
	if p.mode == "drop_then_stale_read" && p.updates > 0 {
		return lease.KVEntry{Value: p.staleValue, Revision: 1}, nil
	}
	if p.mode == "read_unknown" || p.mode == "ack_then_read_unknown" && p.updates > 0 {
		return lease.KVEntry{}, nats.ErrTimeout
	}
	if p.revision == 0 {
		return lease.KVEntry{}, jetstream.ErrKeyNotFound
	}
	return lease.KVEntry{Value: append([]byte(nil), p.value...), Revision: p.revision}, nil
}
func (p *graphProjectionModel) Create(_ context.Context, _ string, data []byte) (uint64, error) {
	if p.revision != 0 {
		return 0, jetstream.ErrKeyExists
	}
	p.value = append([]byte(nil), data...)
	p.revision = 1
	return 1, nil
}
func (p *graphProjectionModel) Update(_ context.Context, _ string, data []byte, r uint64) (uint64, error) {
	p.updates++
	if p.mode == "purge_wins" {
		p.value = p.tomb
		p.revision++
		return 0, jetstream.ErrKeyRevisionMismatch
	}
	if p.mode == "drop" || p.mode == "drop_then_stale_read" {
		return 0, nats.ErrTimeout
	}
	if r != p.revision {
		return 0, jetstream.ErrKeyRevisionMismatch
	}
	p.value = append([]byte(nil), data...)
	p.revision++
	if p.mode == "ack_lost" || p.mode == "ack_then_read_unknown" {
		return 0, nats.ErrTimeout
	}
	return p.revision, nil
}
func TestVerifiedGraphProjectionRepairCASAndUncertainty(t *testing.T) {
	ctx := context.Background()
	verified := []byte(`{"inv_seq":7,"result":42}`)
	tomb, _ := json.Marshal(retention.Tombstone{Tombstone: true, InvSeq: 7, PurgedAt: time.Unix(100, 0), ExpiresAt: time.Unix(200, 0)})
	for _, mode := range []string{"healthy", "ack_lost", "drop", "drop_then_stale_read", "purge_wins", "read_unknown", "ack_then_read_unknown", "malformed", "already_correct", "absent"} {
		t.Run(mode, func(t *testing.T) {
			p := &graphProjectionModel{value: []byte(`{"inv_seq":107,"error":"forged"}`), revision: 1, mode: mode, tomb: tomb}
			if mode == "drop_then_stale_read" {
				p.revision = 2
				p.staleValue = verified
			}
			if mode == "absent" {
				p.revision = 0
			}
			if mode == "malformed" {
				p.value = []byte(`{"tombstone":`)
			}
			if mode == "already_correct" {
				p.value = verified
			}
			err := repairVerifiedGraphOutcome(ctx, p, "flow", "terminal", 7, verified)
			success := mode == "healthy" || mode == "ack_lost" || mode == "already_correct" || mode == "absent"
			if (err == nil) != success {
				t.Fatal(err)
			}
			if success && !bytes.Equal(p.value, verified) {
				t.Fatal(string(p.value))
			}
			if mode == "purge_wins" && (!errors.Is(err, client.ErrPurged) || !bytes.Equal(p.value, tomb)) {
				t.Fatal(err, string(p.value))
			}
			if (mode == "read_unknown" || mode == "malformed" || mode == "already_correct") && p.updates != 0 {
				t.Fatal("unexpected update")
			}
			if p.updates > 1 {
				t.Fatal("uncertain mutation retried")
			}
		})
	}
}
