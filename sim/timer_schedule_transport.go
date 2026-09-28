package sim

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"js-wf/worker"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

type timerPublication struct {
	message *nats.Msg
	id      string
	due     time.Time
	fired   bool
}

// TimerScheduleTransport models only timer publish acknowledgment, message-ID
// deduplication, and eventual native target delivery. Fallback records remain
// retained for the separate fallback scanner to route.
type TimerScheduleTransport struct {
	schedule    *Scheduler
	base        time.Time
	window      int64
	ids         map[string]int64
	faults      []AppendFault
	native      []timerPublication
	fallback    []Message
	runs        []Message
	streamSeq   uint64
	fallbackSeq uint64
}

var _ worker.TimerSchedulePort = (*TimerScheduleTransport)(nil)

func NewTimerScheduleTransport(schedule *Scheduler, base time.Time) *TimerScheduleTransport {
	return &TimerScheduleTransport{schedule: schedule, base: base, window: (2 * time.Minute).Milliseconds(), ids: map[string]int64{}}
}

func (m *TimerScheduleTransport) QueueFault(kind AppendFault) error {
	if kind != DropBeforeCommit && kind != LoseAckAfterCommit {
		return fmt.Errorf("invalid timer publish fault %q", kind)
	}
	m.faults = append(m.faults, kind)
	return nil
}

func (m *TimerScheduleTransport) takeFault() AppendFault {
	if len(m.faults) == 0 {
		return ""
	}
	fault := m.faults[0]
	m.faults = m.faults[1:]
	return fault
}

func (m *TimerScheduleTransport) duplicate(messageID string) bool {
	previous, ok := m.ids[messageID]
	return ok && m.schedule.NowMillis()-previous < m.window
}

func (m *TimerScheduleTransport) PublishFallback(ctx context.Context, subject string, payload []byte, messageID string) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	var timer struct {
		FireAt time.Time `json:"fire_at"`
	}
	if json.Unmarshal(payload, &timer) != nil || timer.FireAt.IsZero() {
		return false, fmt.Errorf("invalid fallback timer payload")
	}
	fault := m.takeFault()
	event := TransportEvent{Operation: "publish_fallback_timer", Subject: subject, DataSHA256: digest(payload)}
	if fault == DropBeforeCommit {
		event.Outcome = string(fault)
		m.event(event)
		return false, ErrTransportLost
	}
	if m.duplicate(messageID) {
		event.Outcome = "duplicate"
		m.event(event)
		return true, nil
	}
	m.fallbackSeq++
	m.fallback = append(m.fallback, Message{Subject: subject, Sequence: m.fallbackSeq, Data: append([]byte(nil), payload...)})
	m.ids[messageID] = m.schedule.NowMillis()
	event.Sequence = m.fallbackSeq
	if fault == LoseAckAfterCommit {
		event.Outcome = string(fault)
		m.event(event)
		return false, ErrTransportLost
	}
	event.Outcome = "ok"
	m.event(event)
	return false, nil
}

func (m *TimerScheduleTransport) PublishNative(ctx context.Context, message *nats.Msg, messageID string) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	value := message.Header.Get(jetstream.ScheduleHeader)
	if !strings.HasPrefix(value, "@at ") || message.Header.Get(jetstream.ScheduleTargetHeader) == "" {
		return false, fmt.Errorf("invalid native schedule headers")
	}
	due, err := time.Parse(time.RFC3339Nano, strings.TrimPrefix(value, "@at "))
	if err != nil {
		return false, err
	}
	fault := m.takeFault()
	event := TransportEvent{Operation: "publish_native_timer", Subject: message.Subject, DataSHA256: digest(message.Data)}
	if fault == DropBeforeCommit {
		event.Outcome = string(fault)
		m.event(event)
		return false, ErrTransportLost
	}
	if m.duplicate(messageID) {
		event.Outcome = "duplicate"
		m.event(event)
		return true, nil
	}
	m.streamSeq++
	copyMessage := &nats.Msg{Subject: message.Subject, Data: append([]byte(nil), message.Data...), Header: cloneHeader(message.Header)}
	m.native = append(m.native, timerPublication{message: copyMessage, id: messageID, due: due})
	m.ids[messageID] = m.schedule.NowMillis()
	event.Sequence = m.streamSeq
	if fault == LoseAckAfterCommit {
		event.Outcome = string(fault)
		m.event(event)
		return false, ErrTransportLost
	}
	event.Outcome = "ok"
	m.event(event)
	return false, nil
}

func (m *TimerScheduleTransport) Advance(delay time.Duration) error {
	if delay < 0 || delay%time.Millisecond != 0 {
		return fmt.Errorf("invalid timer advance %s", delay)
	}
	if err := m.schedule.AdvanceMillis(delay.Milliseconds()); err != nil {
		return err
	}
	now := m.base.Add(time.Duration(m.schedule.NowMillis()) * time.Millisecond)
	for i := range m.native {
		entry := &m.native[i]
		if entry.fired || now.Before(entry.due) {
			continue
		}
		entry.fired = true
		m.streamSeq++
		target := entry.message.Header.Get(jetstream.ScheduleTargetHeader)
		m.runs = append(m.runs, Message{Subject: target, Sequence: m.streamSeq, Data: append([]byte(nil), entry.message.Data...)})
		m.event(TransportEvent{Operation: "deliver_native_timer", Subject: target, Sequence: m.streamSeq, DataSHA256: digest(entry.message.Data), Outcome: "ok"})
	}
	return nil
}

func (m *TimerScheduleTransport) NativeSources() []*nats.Msg {
	out := make([]*nats.Msg, len(m.native))
	for i, entry := range m.native {
		out[i] = &nats.Msg{Subject: entry.message.Subject, Data: append([]byte(nil), entry.message.Data...), Header: cloneHeader(entry.message.Header)}
	}
	return out
}

func (m *TimerScheduleTransport) FallbackRecords() []Message {
	out := append([]Message(nil), m.fallback...)
	for i := range out {
		out[i].Data = append([]byte(nil), out[i].Data...)
	}
	return out
}

func (m *TimerScheduleTransport) Runs() []Message {
	out := append([]Message(nil), m.runs...)
	for i := range out {
		out[i].Data = append([]byte(nil), out[i].Data...)
	}
	return out
}

func (m *TimerScheduleTransport) event(event TransportEvent) {
	event.AtMillis = m.schedule.NowMillis()
	m.schedule.trace.Transport = append(m.schedule.trace.Transport, event)
}
