package sim

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"time"

	"js-wf/client"
	"js-wf/identity"
	"js-wf/journal"
	"js-wf/provision"
	"js-wf/reconcile"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// SignalTransport models the retained reads and writes used by the production
// signal client and reconciler. StartTransport supplies invocation and run
// stream behavior, including message-ID deduplication and enqueue faults.
type SignalTransport struct {
	*StartTransport
	mu       sync.Mutex
	sequence uint64
	signals  map[uint64]jetstream.RawStreamMsg
	journals map[string][]journal.Record
	state    map[string][]byte
	dedup    map[string]signalDedupEntry
	window   int64
	faults   []SignalFault
}

var _ reconcile.SignalScanPort = (*SignalTransport)(nil)
var _ client.SignalPort = (*SignalTransport)(nil)

func NewSignalTransport(schedule *Scheduler) *SignalTransport {
	return &SignalTransport{
		StartTransport: NewStartTransport(schedule),
		signals:        map[uint64]jetstream.RawStreamMsg{},
		journals:       map[string][]journal.Record{},
		state:          map[string][]byte{},
		dedup:          map[string]signalDedupEntry{},
		window:         (2 * time.Minute).Milliseconds(),
	}
}

// CommitSignal commits a fixture signal without its wakeup. The caller can
// scan before or after a separate enqueue to explore the lost-wakeup cut.
func (m *SignalTransport) CommitSignal(message *nats.Msg) uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.commitSignal(message)
	m.event(TransportEvent{Operation: "publish_signal", Subject: message.Subject, Sequence: m.sequence, DataSHA256: digest(message.Data), Outcome: "ok"})
	return m.sequence
}

func (m *SignalTransport) commitSignal(message *nats.Msg) uint64 {
	m.sequence++
	m.signals[m.sequence] = jetstream.RawStreamMsg{
		Subject: message.Subject, Sequence: m.sequence,
		Header: cloneHeader(message.Header), Data: append([]byte(nil), message.Data...),
	}
	return m.sequence
}

func (m *SignalTransport) PurgeSignal(sequence uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.signals, sequence)
	m.event(TransportEvent{Operation: "purge_signal", Sequence: sequence, Outcome: "ok"})
}

func (m *SignalTransport) SetJournal(typ, id string, records []journal.Record) {
	m.mu.Lock()
	defer m.mu.Unlock()
	key := identity.Key(typ, id)
	m.journals[key] = cloneJournalRecords(records)
	m.event(TransportEvent{Operation: "set_journal", Subject: identity.JournalSubject(typ, id), Sequence: uint64(len(records)), Outcome: "ok"})
}

func (m *SignalTransport) GetSignal(ctx context.Context, sequence uint64) (*jetstream.RawStreamMsg, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	message, exists := m.signals[sequence]
	if !exists {
		m.event(TransportEvent{Operation: "get_signal", Sequence: sequence, Outcome: "not_found"})
		return nil, jetstream.ErrMsgNotFound
	}
	m.event(TransportEvent{Operation: "get_signal", Subject: message.Subject, Sequence: sequence, DataSHA256: digest(message.Data), Outcome: "ok"})
	message.Header = cloneHeader(message.Header)
	message.Data = append([]byte(nil), message.Data...)
	return &message, nil
}

func (m *SignalTransport) LastSignalSequence(ctx context.Context) (uint64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.event(TransportEvent{Operation: "signal_stream_info", Sequence: m.sequence, Outcome: "ok"})
	return m.sequence, nil
}

func (m *SignalTransport) ReadJournal(ctx context.Context, typ, id string) ([]journal.Record, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	records := cloneJournalRecords(m.journals[identity.Key(typ, id)])
	m.event(TransportEvent{Operation: "read_journal", Subject: identity.JournalSubject(typ, id), Sequence: uint64(len(records)), Outcome: "ok"})
	return records, nil
}

func (m *SignalTransport) EnqueueSignal(ctx context.Context, typ, id string, sequence uint64) error {
	if sequence == 0 {
		return fmt.Errorf("zero signal sequence")
	}
	key := identity.Key(typ, id)
	return m.EnqueueRun(ctx, identity.RunSubject(typ, id, provision.Partitions), []byte(key), "signal-wakeup:"+strconv.FormatUint(sequence, 10))
}

func cloneJournalRecords(records []journal.Record) []journal.Record {
	copyRecords := append([]journal.Record(nil), records...)
	for i := range copyRecords {
		copyRecords[i].Payload = append([]byte(nil), copyRecords[i].Payload...)
	}
	return copyRecords
}
