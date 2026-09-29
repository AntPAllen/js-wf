package sim

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"js-wf/retention"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

type blobSweepStream struct {
	last     uint64
	messages map[uint64]retention.BlobSweepMessage
}

type blobSweepObject struct {
	data    []byte
	created time.Time
}

// BlobSweepTransport models the retained stream, KV, and Object Store edges
// read by production quiescent blob reclamation.
type BlobSweepTransport struct {
	mu          sync.Mutex
	schedule    *Scheduler
	state       *KVTransport
	streams     map[string]*blobSweepStream
	objects     map[string]blobSweepObject
	deleteFault AppendFault
	readFault   string
}

var _ retention.BlobSweepPort = (*BlobSweepTransport)(nil)

func NewBlobSweepTransport(schedule *Scheduler) *BlobSweepTransport {
	streams := map[string]*blobSweepStream{}
	for _, name := range []string{"WF_INV", "WF_SIG", "WF_JRN", "WF_TIMER", "WF_PURGE"} {
		streams[name] = &blobSweepStream{messages: map[uint64]retention.BlobSweepMessage{}}
	}
	return &BlobSweepTransport{schedule: schedule, state: NewKVTransport(schedule, 0), streams: streams, objects: map[string]blobSweepObject{}}
}

func (m *BlobSweepTransport) State() *KVTransport { return m.state }

func (m *BlobSweepTransport) Publish(name string, header nats.Header, data []byte) (uint64, error) {
	return m.PublishSubject(name, "", header, data)
}

func (m *BlobSweepTransport) PublishSubject(name, subject string, header nats.Header, data []byte) (uint64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	stream := m.streams[name]
	if stream == nil {
		return 0, fmt.Errorf("unknown blob sweep stream %q", name)
	}
	stream.last++
	stream.messages[stream.last] = retention.BlobSweepMessage{Subject: subject, Header: cloneHeader(header), Data: append([]byte(nil), data...)}
	m.event(TransportEvent{Operation: "blob_stream_publish", Subject: name, Sequence: stream.last, DataSHA256: digest(data), Outcome: "ok"})
	return stream.last, nil
}

func (m *BlobSweepTransport) Purge(name string, sequence uint64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	stream := m.streams[name]
	if stream == nil {
		return fmt.Errorf("unknown blob sweep stream %q", name)
	}
	delete(stream.messages, sequence)
	m.event(TransportEvent{Operation: "blob_stream_purge", Subject: name, Sequence: sequence, Outcome: "ok"})
	return nil
}

func (m *BlobSweepTransport) PutObject(name string, data []byte, created time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.objects[name] = blobSweepObject{data: append([]byte(nil), data...), created: created}
	m.event(TransportEvent{Operation: "blob_put", Subject: name, DataSHA256: digest(data), Outcome: "ok"})
}

func (m *BlobSweepTransport) HasObject(name string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, ok := m.objects[name]
	return ok
}

func (m *BlobSweepTransport) QueueDeleteFault(fault AppendFault) error {
	if fault != DropBeforeCommit && fault != LoseAckAfterCommit {
		return fmt.Errorf("invalid blob delete fault %q", fault)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.deleteFault = fault
	return nil
}

func (m *BlobSweepTransport) QueueReadFault(name string) error {
	if name == "" {
		return fmt.Errorf("empty blob read fault name")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.readFault = name
	return nil
}

func (m *BlobSweepTransport) StreamRange(ctx context.Context, name string) (uint64, uint64, error) {
	if err := ctx.Err(); err != nil {
		return 0, 0, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	stream := m.streams[name]
	if stream == nil {
		return 0, 0, jetstream.ErrStreamNotFound
	}
	var first uint64
	for sequence := range stream.messages {
		if first == 0 || sequence < first {
			first = sequence
		}
	}
	m.event(TransportEvent{Operation: "blob_stream_range", Subject: name, Expected: first, Sequence: stream.last, Outcome: "ok"})
	return first, stream.last, nil
}

func (m *BlobSweepTransport) StreamMessage(ctx context.Context, name string, sequence uint64) (retention.BlobSweepMessage, error) {
	if err := ctx.Err(); err != nil {
		return retention.BlobSweepMessage{}, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	stream := m.streams[name]
	if stream == nil {
		return retention.BlobSweepMessage{}, jetstream.ErrStreamNotFound
	}
	message, ok := stream.messages[sequence]
	if !ok {
		m.event(TransportEvent{Operation: "blob_stream_get", Subject: name, Sequence: sequence, Outcome: "not_found"})
		return retention.BlobSweepMessage{}, jetstream.ErrMsgNotFound
	}
	m.event(TransportEvent{Operation: "blob_stream_get", Subject: name, Sequence: sequence, DataSHA256: digest(message.Data), Outcome: "ok"})
	return retention.BlobSweepMessage{Subject: message.Subject, Header: cloneHeader(message.Header), Data: append([]byte(nil), message.Data...)}, nil
}

func (m *BlobSweepTransport) StateKeys(ctx context.Context) ([]string, error) {
	return m.state.Keys(ctx)
}

func (m *BlobSweepTransport) StateValue(ctx context.Context, key string) ([]byte, error) {
	entry, err := m.state.Get(ctx, key)
	if err != nil {
		return nil, err
	}
	return entry.Value, nil
}

func (m *BlobSweepTransport) ObjectBytes(ctx context.Context, name string) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.readFault == name {
		m.readFault = ""
		m.event(TransportEvent{Operation: "blob_get", Subject: name, Outcome: "transient_unavailable"})
		return nil, ErrTransportLost
	}
	object, ok := m.objects[name]
	if !ok {
		m.event(TransportEvent{Operation: "blob_get", Subject: name, Outcome: "not_found"})
		return nil, jetstream.ErrObjectNotFound
	}
	m.event(TransportEvent{Operation: "blob_get", Subject: name, DataSHA256: digest(object.data), Outcome: "ok"})
	return append([]byte(nil), object.data...), nil
}

func (m *BlobSweepTransport) Objects(ctx context.Context) ([]retention.BlobSweepObject, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	names := make([]string, 0, len(m.objects))
	for name := range m.objects {
		names = append(names, name)
	}
	sort.Strings(names)
	if len(names) == 0 {
		m.event(TransportEvent{Operation: "blob_list", Outcome: "no_objects"})
		return nil, jetstream.ErrNoObjectsFound
	}
	all := make([]retention.BlobSweepObject, 0, len(names))
	for _, name := range names {
		all = append(all, retention.BlobSweepObject{Name: name, ModTime: m.objects[name].created})
	}
	m.event(TransportEvent{Operation: "blob_list", Sequence: uint64(len(all)), Outcome: "ok"})
	return all, nil
}

func (m *BlobSweepTransport) DeleteObject(ctx context.Context, name string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	fault := m.deleteFault
	m.deleteFault = ""
	event := TransportEvent{Operation: "blob_delete", Subject: name}
	if fault == DropBeforeCommit {
		event.Outcome = string(fault)
		m.event(event)
		return ErrTransportLost
	}
	if _, ok := m.objects[name]; !ok {
		event.Outcome = "not_found"
		m.event(event)
		return jetstream.ErrObjectNotFound
	}
	delete(m.objects, name)
	if fault == LoseAckAfterCommit {
		event.Outcome = string(fault)
		m.event(event)
		return ErrTransportLost
	}
	event.Outcome = "ok"
	m.event(event)
	return nil
}

func (m *BlobSweepTransport) event(event TransportEvent) {
	event.AtMillis = m.schedule.NowMillis()
	m.schedule.RecordTransport(event)
}
