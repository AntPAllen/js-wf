package integrity

import (
	"context"
	"runtime"
	"sync"

	"github.com/nats-io/nats.go/jetstream"
	"js-wf/journal"
)

const journalDecodeBatchRecords = 64
const journalDecodeBatchBytes = 1 << 20

type journalEntryVisitor func(*jetstream.RawStreamMsg, journal.Entry) error
type journalEntryDecoder func(context.Context, []byte, *journal.Entry) error

func readJournalEntries(ctx context.Context, stream jetstream.Stream, read retainedScanner, accept func(*jetstream.RawStreamMsg) bool, visit journalEntryVisitor, parallel bool) error {
	if !parallel {
		return read(ctx, stream, nil, func(m *jetstream.RawStreamMsg) error {
			if !accept(m) {
				return nil
			}
			var e journal.Entry
			if err := journal.UnmarshalEntry(m.Data, &e); err != nil {
				return err
			}
			return visit(m, e)
		})
	}
	return decodeJournalEntries(ctx, func(call context.Context, raw func(*jetstream.RawStreamMsg) error) error {
		return read(call, stream, nil, raw)
	}, accept, visit, func(_ context.Context, data []byte, e *journal.Entry) error {
		return journal.UnmarshalEntry(data, e)
	}, min(4, runtime.GOMAXPROCS(0)))
}

// The scanner retains responsibility for bounds, gaps, resumption and cleanup.
// Only decoding runs concurrently. The calling goroutine alone reduces results
// in source order. At most workers batches plus one accumulating batch retain
// payloads: each has <=64 records and <=1MiB, except a single oversized record.
// The scanner may additionally hold its incoming record while flushing a full
// byte batch; its own bounded delivery buffers are unchanged by this pipeline.
// Scanner payloads must remain immutable after delivery, as RawStreamMsg does.
func decodeJournalEntries(ctx context.Context, scan func(context.Context, func(*jetstream.RawStreamMsg) error) error, accept func(*jetstream.RawStreamMsg) bool, visit journalEntryVisitor, decode journalEntryDecoder, workers int) error {
	workers = max(1, min(4, workers))
	call, cancel := context.WithCancel(ctx)
	type batch struct {
		messages []*jetstream.RawStreamMsg
		entries  []journal.Entry
		bytes    int
		err      error
		done     chan struct{}
	}
	jobs := make(chan *batch)
	var joined sync.WaitGroup
	for range workers {
		joined.Add(1)
		go func() {
			defer joined.Done()
			for {
				select {
				case <-call.Done():
					return
				case b := <-jobs:
					for _, m := range b.messages {
						if b.err = call.Err(); b.err != nil {
							break
						}
						// Decode into the reusable batch slot. Passing a fresh local
						// Entry through the decoder function allocated once per record.
						i := len(b.entries)
						b.entries = b.entries[:i+1]
						if b.err = decode(call, m.Data, &b.entries[i]); b.err != nil {
							clear(b.entries[i:])
							b.entries = b.entries[:i]
							break
						}
					}
					close(b.done)
				}
			}
		}()
	}
	// Cancellation also releases workers on early visitor/scanner/decode errors.
	// Every worker is joined before the caller observes completion.
	defer func() { cancel(); joined.Wait() }()
	free := make([]*batch, workers+1)
	for i := range free {
		free[i] = &batch{messages: make([]*jetstream.RawStreamMsg, 0, journalDecodeBatchRecords), entries: make([]journal.Entry, 0, journalDecodeBatchRecords)}
	}
	acquire := func() *batch {
		b := free[len(free)-1]
		free = free[:len(free)-1]
		b.done = make(chan struct{})
		return b
	}
	current := acquire()
	pending := make([]*batch, 0, workers)
	retire := func() error {
		b := pending[0]
		select {
		case <-call.Done():
			return call.Err()
		case <-b.done:
		}
		for i, e := range b.entries {
			if err := call.Err(); err != nil {
				return err
			}
			if err := visit(b.messages[i], e); err != nil {
				return err
			}
		}
		if b.err != nil {
			return b.err
		}
		clear(b.messages)
		clear(b.entries)
		b.messages, b.entries = b.messages[:0], b.entries[:0]
		b.bytes, b.err = 0, nil
		free = append(free, b)
		copy(pending, pending[1:])
		pending[len(pending)-1] = nil
		pending = pending[:len(pending)-1]
		return nil
	}
	flush := func() error {
		if len(current.messages) == 0 {
			return nil
		}
		if len(pending) == workers {
			if err := retire(); err != nil {
				return err
			}
		}
		select {
		case <-call.Done():
			return call.Err()
		case jobs <- current:
		}
		pending = append(pending, current)
		current = acquire()
		return nil
	}
	var callbackErr error
	scanErr := scan(call, func(m *jetstream.RawStreamMsg) (err error) {
		defer func() {
			if err != nil {
				callbackErr = err
			}
		}()
		if callbackErr != nil {
			return callbackErr
		}
		if err := call.Err(); err != nil {
			return err
		}
		if !accept(m) {
			return nil
		}
		if len(current.messages) > 0 && len(m.Data) > journalDecodeBatchBytes-current.bytes {
			if err := flush(); err != nil {
				return err
			}
		}
		current.messages = append(current.messages, m)
		current.bytes += len(m.Data)
		if len(current.messages) == journalDecodeBatchRecords || current.bytes >= journalDecodeBatchBytes {
			return flush()
		}
		return nil
	})
	if callbackErr != nil {
		return callbackErr
	}
	// Earlier entry/visitor errors take precedence over a later scanner error.
	if err := flush(); err != nil {
		return err
	}
	for len(pending) > 0 {
		if err := retire(); err != nil {
			return err
		}
	}
	return scanErr
}
