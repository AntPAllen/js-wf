package retention

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"js-wf/identity"

	"github.com/nats-io/nats.go/jetstream"
)

// TombstoneScan pages over the underlying WF_STATE stream. Stream sequence
// holes count toward the budget, so deleted keys cannot stall the cursor.
type TombstoneScan struct {
	port TombstoneScanPort
}

func NewTombstoneScan(js jetstream.JetStream) *TombstoneScan {
	return NewTombstoneScanWithPort(NewTombstoneScanPort(js))
}

// TombstoneScanPort opens the same per-pass retained handles used by the
// production scanner. Sessions also supply generation lookup and CAS deletion.
type TombstoneScanPort interface {
	Open(context.Context) (TombstoneScanSession, error)
}
type TombstoneScanSession interface {
	TombstoneSweepPort
	LastStateSequence(context.Context) (uint64, error)
	GetStateMessage(context.Context, uint64) (*jetstream.RawStreamMsg, error)
}

func NewTombstoneScanWithPort(port TombstoneScanPort) *TombstoneScan {
	return &TombstoneScan{port: port}
}
func NewTombstoneScanPort(js jetstream.JetStream) TombstoneScanPort {
	return jetStreamTombstoneScanPort{js: js}
}

type jetStreamTombstoneScanPort struct{ js jetstream.JetStream }
type jetStreamTombstoneScanSession struct {
	jetStreamTombstoneSweepPort
	stream jetstream.Stream
}

func (p jetStreamTombstoneScanPort) Open(ctx context.Context) (TombstoneScanSession, error) {
	state, err := p.js.KeyValue(ctx, "WF_STATE")
	if err != nil {
		return nil, err
	}
	stream, err := p.js.Stream(ctx, "KV_WF_STATE")
	if err != nil {
		return nil, err
	}
	inv, err := p.js.Stream(ctx, "WF_INV")
	if err != nil {
		return nil, err
	}
	return jetStreamTombstoneScanSession{jetStreamTombstoneSweepPort: jetStreamTombstoneSweepPort{state: state, inv: inv}, stream: stream}, nil
}
func (p jetStreamTombstoneScanSession) LastStateSequence(ctx context.Context) (uint64, error) {
	info, err := p.stream.Info(ctx)
	if err != nil {
		return 0, err
	}
	return info.State.LastSeq, nil
}
func (p jetStreamTombstoneScanSession) GetStateMessage(ctx context.Context, seq uint64) (*jetstream.RawStreamMsg, error) {
	return p.stream.GetMsg(ctx, seq)
}

type TombstonePage struct {
	RetrySequence uint64   `json:"retry_sequence,omitempty"`
	NextSequence  uint64   `json:"next_sequence"`
	Inspected     int      `json:"inspected"`
	Expired       int      `json:"expired"`
	Eligible      int      `json:"eligible"`
	Deleted       int      `json:"deleted"`
	Keys          []string `json:"keys,omitempty"`
}

// Scan examines at most budget KV stream sequences. It checks each candidate
// against the latest KV revision before deleting, so a newer generation or
// cursor update cannot be removed through an older stream entry.
func (s *TombstoneScan) Scan(ctx context.Context, next uint64, budget int, now time.Time, dryRun bool) (TombstonePage, error) {
	if budget < 1 || now.IsZero() {
		return TombstonePage{}, fmt.Errorf("invalid tombstone scan budget or clock")
	}
	if next == 0 {
		next = 1
	}
	session, err := s.port.Open(ctx)
	if err != nil {
		return TombstonePage{}, err
	}
	last, err := session.LastStateSequence(ctx)
	if err != nil {
		return TombstonePage{}, err
	}
	if next > last {
		return TombstonePage{NextSequence: 1}, nil
	}
	count := min(budget, 4096)
	available := last - next + 1
	wrap := available < uint64(count)
	if wrap {
		count = int(available)
	}
	result := TombstonePage{NextSequence: next}
	const prefix = "$KV.WF_STATE."
	for offset := 0; offset < count; offset++ {
		seq := next + uint64(offset)
		result.NextSequence = seq + 1
		message, err := session.GetStateMessage(ctx, seq)
		if errors.Is(err, jetstream.ErrMsgNotFound) {
			continue
		}
		if err != nil {
			return result, err
		}
		if !strings.HasPrefix(message.Subject, prefix) {
			return result, fmt.Errorf("invalid state subject %q", message.Subject)
		}
		key := strings.TrimPrefix(message.Subject, prefix)
		parts := strings.Split(key, ".")
		if len(parts) != 2 || identity.Validate(parts[0], parts[1]) != nil {
			continue
		}
		value, revision, err := session.StateValue(ctx, key)
		if errors.Is(err, jetstream.ErrKeyNotFound) {
			continue
		}
		if err != nil {
			return result, err
		}
		if revision != seq {
			continue
		}
		result.Inspected++
		expired, eligible, deleted, err := SweepCandidate(ctx, session, key, value, revision, now, dryRun)
		if err != nil {
			return result, err
		}
		if expired {
			result.Expired++
		}
		if eligible {
			result.Eligible++
			result.Keys = append(result.Keys, key)
		}
		if deleted {
			result.Deleted++
		}
	}
	if wrap {
		result.NextSequence = 1
	}
	return result, nil
}
