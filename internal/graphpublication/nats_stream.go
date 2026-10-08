package graphpublication

import (
	"context"

	"github.com/nats-io/nats.go/jetstream"
)

// guardedStream protects the pinned SDK stream's mutable Info cache. Info and
// GetMsg/GetLastMsgForSubject access that cache without an SDK mutex. The guard
// covers individual stream calls, not whole protocol operations or publications;
// conditional publication still arbitrates competing actors at the server.
// Waiting for a busy handle observes the caller's context instead of blocking on
// an uninterruptible mutex behind another caller's network request.
type guardedStream struct {
	jetstream.Stream
	gate chan struct{}
}

func guardStream(stream jetstream.Stream) jetstream.Stream {
	gate := make(chan struct{}, 1)
	gate <- struct{}{}
	return &guardedStream{Stream: stream, gate: gate}
}
func guardedCall[T any](ctx context.Context, s *guardedStream, call func() (T, error)) (T, error) {
	var zero T
	if err := ctx.Err(); err != nil {
		return zero, err
	}
	select {
	case <-ctx.Done():
		return zero, ctx.Err()
	case <-s.gate:
	}
	defer func() { s.gate <- struct{}{} }()
	if err := ctx.Err(); err != nil {
		return zero, err
	}
	return call()
}
func (s *guardedStream) Info(c context.Context, o ...jetstream.StreamInfoOpt) (*jetstream.StreamInfo, error) {
	return guardedCall(c, s, func() (*jetstream.StreamInfo, error) { return s.Stream.Info(c, o...) })
}
func (s *guardedStream) GetMsg(c context.Context, seq uint64, o ...jetstream.GetMsgOpt) (*jetstream.RawStreamMsg, error) {
	return guardedCall(c, s, func() (*jetstream.RawStreamMsg, error) { return s.Stream.GetMsg(c, seq, o...) })
}
func (s *guardedStream) GetLastMsgForSubject(c context.Context, subject string) (*jetstream.RawStreamMsg, error) {
	return guardedCall(c, s, func() (*jetstream.RawStreamMsg, error) { return s.Stream.GetLastMsgForSubject(c, subject) })
}
func (s *guardedStream) Purge(c context.Context, o ...jetstream.StreamPurgeOpt) error {
	_, err := guardedCall(c, s, func() (struct{}, error) { return struct{}{}, s.Stream.Purge(c, o...) })
	return err
}
func (s *guardedStream) CachedInfo() *jetstream.StreamInfo {
	info, _ := guardedCall(context.Background(), s, func() (*jetstream.StreamInfo, error) { return s.Stream.CachedInfo(), nil })
	return info
}
