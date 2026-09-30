package journal

import (
	"context"
	"errors"
	"github.com/nats-io/nats.go/jetstream"
	"testing"
	"time"
)

type manifestReadProbe struct {
	SnapshotReadPort
	failures int
	calls    int
	waits    int
	payload  []byte
	err      error
}

func (p *manifestReadProbe) GetManifest(ctx context.Context, key string) ([]byte, error) {
	deadline, ok := ctx.Deadline()
	if !ok || time.Until(deadline) > 2*time.Second {
		panic("unbounded manifest read")
	}
	p.calls++
	if p.calls <= p.failures {
		return nil, context.DeadlineExceeded
	}
	return p.payload, p.err
}
func (p *manifestReadProbe) Wait(context.Context, time.Duration) error { p.waits++; return nil }
func TestManifestReadsRetryButRejectMalformedData(t *testing.T) {
	for _, test := range []struct {
		failures     int
		payload      []byte
		err          error
		want         error
		calls, waits int
	}{
		{2, nil, jetstream.ErrKeyNotFound, nil, 3, 2},
		{3, nil, nil, context.DeadlineExceeded, 3, 2},
		{0, []byte("{"), nil, ErrGap, 1, 0},
	} {
		port := &manifestReadProbe{failures: test.failures, payload: test.payload, err: test.err}
		store := NewWithSnapshotReadPort(nil, nil, port)
		records, snapshot, err := store.loadSnapshot(context.Background(), "test", "probe")
		if !errors.Is(err, test.want) || port.calls != test.calls || port.waits != test.waits || records != nil || snapshot != nil {
			t.Fatalf("calls=%d waits=%d err=%v want=%v", port.calls, port.waits, err, test.want)
		}
	}
}
