package retainedindex

import (
	"context"
	"time"

	"js-wf/internal/graphpublication"
)

// View binds index reads to one destination's captured named forest. A copied
// forest root cannot construct it. Caller-owned lifecycle and write identity
// must still be checked before publishing an Update packet at Count().
type View struct {
	protocol   graphpublication.Protocol
	reader     graphpublication.Reader
	checkpoint []byte
	stream     string
	count      uint64
	now        func() time.Time
}

func OpenView(ctx context.Context, protocol graphpublication.Protocol, reader graphpublication.Reader, stream string, now func() time.Time) (*View, error) {
	if now == nil {
		return nil, ErrInvalid
	}
	checkpoint, err := reader.Checkpoint()
	if err != nil {
		return nil, err
	}
	confirmed, _, err := protocol.ResumeReader(ctx, checkpoint, now())
	if err != nil {
		return nil, err
	}
	forest, err := confirmed.StreamSnapshot(stream)
	if err != nil {
		return nil, err
	}
	return &View{protocol: protocol, reader: confirmed, checkpoint: checkpoint, stream: stream, count: forest.Count, now: now}, nil
}

func (v *View) Count() uint64 { return v.count }

// validate checks even an empty index or a no-op: expired/released snapshots
// cannot prove absence or authorize a new write by observing no object bytes.
func (v *View) validate(ctx context.Context) error {
	if v == nil || v.now == nil {
		return ErrInvalid
	}
	_, _, err := v.protocol.ResumeReader(ctx, v.checkpoint, v.now())
	return err
}

func (v *View) ReadIndex(ctx context.Context, index uint64) ([]byte, error) {
	if v == nil || v.now == nil || index >= v.count {
		return nil, ErrInvalid
	}
	record, err := v.protocol.ReadRetainedStream(ctx, v.reader, v.stream, index, v.now())
	if err != nil {
		return nil, err
	}
	return record.Data, nil
}

func (v *View) Lookup(ctx context.Context, key Key) (uint64, bool, error) {
	if err := v.validate(ctx); err != nil {
		return 0, false, err
	}
	return Lookup(ctx, v, v.count, key)
}

// Update only prepares metadata bytes; it does not publish or grant ownership
// to input payloads. Append this packet at the captured forest count under the
// exact observed head, with the input's own staged grants and lifecycle CAS.
func (v *View) Update(ctx context.Context, key Key, value uint64) ([]byte, uint64, bool, error) {
	if err := v.validate(ctx); err != nil {
		return nil, 0, false, err
	}
	return Update(ctx, v, v.count, key, value)
}
