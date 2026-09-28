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
	js jetstream.JetStream
}

func NewTombstoneScan(js jetstream.JetStream) *TombstoneScan {
	return &TombstoneScan{js: js}
}

type TombstonePage struct {
	NextSequence uint64   `json:"next_sequence"`
	Inspected    int      `json:"inspected"`
	Expired      int      `json:"expired"`
	Eligible     int      `json:"eligible"`
	Deleted      int      `json:"deleted"`
	Keys         []string `json:"keys,omitempty"`
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
	state, err := s.js.KeyValue(ctx, "WF_STATE")
	if err != nil {
		return TombstonePage{}, err
	}
	stream, err := s.js.Stream(ctx, "KV_WF_STATE")
	if err != nil {
		return TombstonePage{}, err
	}
	inv, err := s.js.Stream(ctx, "WF_INV")
	if err != nil {
		return TombstonePage{}, err
	}
	info, err := stream.Info(ctx)
	if err != nil {
		return TombstonePage{}, err
	}
	if next > info.State.LastSeq {
		return TombstonePage{NextSequence: 1}, nil
	}
	count := min(budget, 4096)
	available := info.State.LastSeq - next + 1
	wrap := available < uint64(count)
	if wrap {
		count = int(available)
	}
	result := TombstonePage{NextSequence: next}
	const prefix = "$KV.WF_STATE."
	for offset := 0; offset < count; offset++ {
		seq := next + uint64(offset)
		result.NextSequence = seq + 1
		message, err := stream.GetMsg(ctx, seq)
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
		entry, err := state.Get(ctx, key)
		if errors.Is(err, jetstream.ErrKeyNotFound) {
			continue
		}
		if err != nil {
			return result, err
		}
		if entry.Revision() != seq {
			continue
		}
		result.Inspected++
		expired, eligible, deleted, err := sweepEntry(ctx, state, inv, key, entry, now, dryRun)
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
