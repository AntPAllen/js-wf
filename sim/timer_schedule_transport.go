package sim

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"js-wf/reconcile"
	"js-wf/worker"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

type timerPublication struct {
	message  *nats.Msg
	id       string
	due      time.Time
	fired    bool
	sequence uint64
	retired  bool
}

// TimerScheduleTransport models only timer publish acknowledgment, message-ID
// deduplication, and eventual native target delivery. Fallback records remain
// retained for the separate fallback scanner to route.
type TimerScheduleTransport struct {
	schedule          *Scheduler
	base              time.Time
	leaderClockOffset time.Duration
	window            int64
	ids               map[string]int64
	faults            []AppendFault
	native            []timerPublication
	fallback          []Message
	deleted           map[uint64]bool
	state             map[string][]byte
	wakeupIDs         map[string]int64
	wakeupFault       []AppendFault
	deleteFault       []AppendFault
	runs              []Message
	streamSeq         uint64
	fallbackSeq       uint64
	scheduleQuorum    bool
	healDelayMillis   int64
	deliverAfter      int64
	onNativeDelivery  func(*nats.Msg, time.Time)
	onFallbackWakeup  func(*nats.Msg, time.Time)
}

var _ worker.TimerSchedulePort = (*TimerScheduleTransport)(nil)
var _ reconcile.FallbackTimerScanPort = (*TimerScheduleTransport)(nil)

func NewTimerScheduleTransport(schedule *Scheduler, base time.Time) *TimerScheduleTransport {
	return &TimerScheduleTransport{schedule: schedule, base: base, window: (2 * time.Minute).Milliseconds(), ids: map[string]int64{}, deleted: map[uint64]bool{}, state: map[string][]byte{}, wakeupIDs: map[string]int64{}, scheduleQuorum: true}
}

// LeaderNow is the modeled scheduling leader's wall clock. Controller receipt
// timestamps and transport retry/dedup budgets remain on virtual elapsed time.
func (m *TimerScheduleTransport) LeaderNow() time.Time {
	return m.base.Add(time.Duration(m.schedule.NowMillis()) * time.Millisecond).Add(m.leaderClockOffset)
}

// SetLeaderClockOffset models a wall-clock source transition. Comparing retained
// absolute deadlines to this clock is a model assumption, not a NATS contract.
func (m *TimerScheduleTransport) SetLeaderClockOffset(offset time.Duration) error {
	if offset%time.Millisecond != 0 {
		return fmt.Errorf("invalid timer leader clock offset %s", offset)
	}
	m.leaderClockOffset = offset
	m.event(TransportEvent{Operation: "timer_leader_clock", Outcome: offset.String()})
	m.deliverDue()
	return nil
}

// SetScheduleQuorum models the scheduling leader's inability to deliver
// targets during a route cut. Due targets remain retained until the route
// heals and its modeled recovery delay passes.
func (m *TimerScheduleTransport) SetScheduleQuorum(available bool) {
	if m.scheduleQuorum == available {
		return
	}
	m.scheduleQuorum = available
	outcome := "lost"
	if available {
		outcome = "healed"
		m.deliverAfter = m.schedule.NowMillis() + m.healDelayMillis
	}
	m.event(TransportEvent{Operation: "timer_schedule_quorum", Outcome: outcome, AtMillis: m.schedule.NowMillis()})
	if available {
		m.deliverDue()
	}
}

// SetHealDelay models bounded server-side leader recovery after routes heal.
// The duration is chosen by the seeded workload in virtual time; it does not
// assert NATS's exact recovery latency.
func (m *TimerScheduleTransport) SetHealDelay(delay time.Duration) error {
	if delay < 0 || delay%time.Millisecond != 0 {
		return fmt.Errorf("invalid timer heal delay %s", delay)
	}
	m.healDelayMillis = delay.Milliseconds()
	return nil
}

// OnNativeDelivery connects due timer targets to a modeled durable consumer.
func (m *TimerScheduleTransport) OnNativeDelivery(callback func(*nats.Msg, time.Time)) {
	m.onNativeDelivery = callback
}

// OnFallbackWakeup connects committed scanner wakeups to a modeled consumer.
func (m *TimerScheduleTransport) OnFallbackWakeup(callback func(*nats.Msg, time.Time)) {
	m.onFallbackWakeup = callback
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
	m.native = append(m.native, timerPublication{message: copyMessage, id: messageID, due: due, sequence: m.streamSeq})
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
	m.deliverDue()
	return nil
}

func (m *TimerScheduleTransport) deliverDue() {
	if !m.scheduleQuorum || m.schedule.NowMillis() < m.deliverAfter {
		return
	}
	now := m.base.Add(time.Duration(m.schedule.NowMillis()) * time.Millisecond)
	for i := range m.native {
		entry := &m.native[i]
		if entry.fired || entry.retired || m.LeaderNow().Before(entry.due) {
			continue
		}
		entry.fired = true
		m.streamSeq++
		target := entry.message.Header.Get(jetstream.ScheduleTargetHeader)
		m.runs = append(m.runs, Message{Subject: target, Sequence: m.streamSeq, Data: append([]byte(nil), entry.message.Data...)})
		if m.onNativeDelivery != nil {
			m.onNativeDelivery(&nats.Msg{Subject: target, Data: append([]byte(nil), entry.message.Data...), Header: cloneHeader(entry.message.Header)}, now)
		}
		m.event(TransportEvent{Operation: "deliver_native_timer", Subject: target, Sequence: m.streamSeq, DataSHA256: digest(entry.message.Data), Outcome: "ok"})
	}
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

func (m *TimerScheduleTransport) RetainedFallbackRecords() []Message {
	var out []Message
	for _, message := range m.fallback {
		if !m.deleted[message.Sequence] {
			out = append(out, Message{Subject: message.Subject, Sequence: message.Sequence, Data: append([]byte(nil), message.Data...)})
		}
	}
	return out
}

func (m *TimerScheduleTransport) SetState(key string, value []byte) {
	m.state[key] = append([]byte(nil), value...)
	m.event(TransportEvent{Operation: "set_timer_state", Subject: key, DataSHA256: digest(value), Outcome: "ok"})
}

func (m *TimerScheduleTransport) QueueWakeupFault(kind AppendFault) error {
	if kind != DropBeforeCommit && kind != LoseAckAfterCommit {
		return fmt.Errorf("invalid fallback wakeup fault %q", kind)
	}
	m.wakeupFault = append(m.wakeupFault, kind)
	return nil
}

func (m *TimerScheduleTransport) QueueDeleteFault(kind AppendFault) error {
	if kind != DropBeforeCommit && kind != LoseAckAfterCommit {
		return fmt.Errorf("invalid timer delete fault %q", kind)
	}
	m.deleteFault = append(m.deleteFault, kind)
	return nil
}

func (m *TimerScheduleTransport) LastTimerSequence(ctx context.Context) (uint64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	m.event(TransportEvent{Operation: "timer_stream_info", Sequence: m.fallbackSeq, Outcome: "ok"})
	return m.fallbackSeq, nil
}

func (m *TimerScheduleTransport) GetTimer(ctx context.Context, sequence uint64) (*jetstream.RawStreamMsg, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if sequence == 0 || sequence > m.fallbackSeq || m.deleted[sequence] {
		m.event(TransportEvent{Operation: "get_fallback_timer", Sequence: sequence, Outcome: "not_found"})
		return nil, jetstream.ErrMsgNotFound
	}
	message := m.fallback[sequence-1]
	m.event(TransportEvent{Operation: "get_fallback_timer", Subject: message.Subject, Sequence: sequence, DataSHA256: digest(message.Data), Outcome: "ok"})
	return &jetstream.RawStreamMsg{Subject: message.Subject, Sequence: message.Sequence, Data: append([]byte(nil), message.Data...)}, nil
}

func (m *TimerScheduleTransport) StateValue(ctx context.Context, key string) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	value, exists := m.state[key]
	if !exists {
		m.event(TransportEvent{Operation: "get_timer_state", Subject: key, Outcome: "not_found"})
		return nil, jetstream.ErrKeyNotFound
	}
	m.event(TransportEvent{Operation: "get_timer_state", Subject: key, DataSHA256: digest(value), Outcome: "ok"})
	return append([]byte(nil), value...), nil
}

func (m *TimerScheduleTransport) PublishWakeup(ctx context.Context, message *nats.Msg, messageID string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	var fault AppendFault
	if len(m.wakeupFault) != 0 {
		fault, m.wakeupFault = m.wakeupFault[0], m.wakeupFault[1:]
	}
	event := TransportEvent{Operation: "publish_fallback_wakeup", Subject: message.Subject, DataSHA256: digest(message.Data)}
	if fault == DropBeforeCommit {
		event.Outcome = string(fault)
		m.event(event)
		return ErrTransportLost
	}
	if previous, exists := m.wakeupIDs[messageID]; exists && m.schedule.NowMillis()-previous < m.window {
		event.Outcome = "duplicate"
		m.event(event)
		return nil
	}
	m.streamSeq++
	m.runs = append(m.runs, Message{Subject: message.Subject, Sequence: m.streamSeq, Data: append([]byte(nil), message.Data...)})
	m.wakeupIDs[messageID] = m.schedule.NowMillis()
	if m.onFallbackWakeup != nil {
		m.onFallbackWakeup(&nats.Msg{Subject: message.Subject, Data: append([]byte(nil), message.Data...), Header: cloneHeader(message.Header)}, m.base.Add(time.Duration(m.schedule.NowMillis())*time.Millisecond))
	}
	event.Sequence = m.streamSeq
	if fault == LoseAckAfterCommit {
		event.Outcome = string(fault)
		m.event(event)
		return ErrTransportLost
	}
	event.Outcome = "ok"
	m.event(event)
	return nil
}

func (m *TimerScheduleTransport) DeleteTimer(ctx context.Context, sequence uint64) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	var fault AppendFault
	if len(m.deleteFault) != 0 {
		fault, m.deleteFault = m.deleteFault[0], m.deleteFault[1:]
	}
	event := TransportEvent{Operation: "delete_fallback_timer", Sequence: sequence}
	if fault == DropBeforeCommit {
		event.Outcome = string(fault)
		m.event(event)
		return ErrTransportLost
	}
	if sequence == 0 || sequence > m.fallbackSeq || m.deleted[sequence] {
		event.Outcome = "not_found"
		m.event(event)
		return jetstream.ErrMsgNotFound
	}
	m.deleted[sequence] = true
	if fault == LoseAckAfterCommit {
		event.Outcome = string(fault)
		m.event(event)
		return ErrTransportLost
	}
	event.Outcome = "ok"
	m.event(event)
	return nil
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
	m.schedule.RecordTransport(event)
}

// Native source retirement preserves publication history while removing retained
// hints and suppressing their future modeled delivery.
func (m *TimerScheduleTransport) NativeTimerSubjects(ctx context.Context, typ, id string) ([]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	subjects := map[string]bool{}
	prefix := "wf.schedule." + typ + "." + id + "."
	for _, hint := range m.native {
		if !hint.fired && !hint.retired && strings.HasPrefix(hint.message.Subject, prefix) {
			subjects[hint.message.Subject] = true
		}
	}
	out := make([]string, 0, len(subjects))
	for subject := range subjects {
		out = append(out, subject)
	}
	// Production sorts this list before making transport decisions.
	return out, nil
}
func (m *TimerScheduleTransport) LastNativeTimer(ctx context.Context, subject string) (*jetstream.RawStreamMsg, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	for i := len(m.native) - 1; i >= 0; i-- {
		hint := m.native[i]
		if !hint.fired && !hint.retired && hint.message.Subject == subject {
			return &jetstream.RawStreamMsg{Subject: subject, Sequence: hint.sequence, Header: cloneHeader(hint.message.Header), Data: append([]byte(nil), hint.message.Data...)}, nil
		}
	}
	return nil, jetstream.ErrMsgNotFound
}
func (m *TimerScheduleTransport) DeleteNativeTimer(ctx context.Context, sequence uint64) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	var fault AppendFault
	if len(m.deleteFault) > 0 {
		fault, m.deleteFault = m.deleteFault[0], m.deleteFault[1:]
	}
	event := TransportEvent{Operation: "delete_native_timer", Sequence: sequence}
	if fault == DropBeforeCommit {
		event.Outcome = string(fault)
		m.event(event)
		return ErrTransportLost
	}
	for i := range m.native {
		hint := &m.native[i]
		if hint.sequence == sequence && !hint.retired && !hint.fired {
			hint.retired = true
			if fault == LoseAckAfterCommit {
				event.Outcome = string(fault)
				m.event(event)
				return ErrTransportLost
			}
			event.Outcome = "ok"
			m.event(event)
			return nil
		}
	}
	event.Outcome = "not_found"
	m.event(event)
	return jetstream.ErrMsgNotFound
}
