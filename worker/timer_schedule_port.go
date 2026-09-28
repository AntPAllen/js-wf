package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"js-wf/identity"
	"js-wf/provision"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// TimerSchedulePort is the durable publish boundary for native and fallback
// timer creation. The timer identity and headers are selected by production
// code before the transport sees them.
type TimerSchedulePort interface {
	PublishFallback(context.Context, string, []byte, string) (duplicate bool, err error)
	PublishNative(context.Context, *nats.Msg, string) (duplicate bool, err error)
}

type jetStreamTimerSchedulePort struct{ js jetstream.JetStream }

func NewTimerSchedulePort(js jetstream.JetStream) TimerSchedulePort {
	return jetStreamTimerSchedulePort{js: js}
}

func (p jetStreamTimerSchedulePort) PublishFallback(ctx context.Context, subject string, payload []byte, messageID string) (bool, error) {
	ack, err := p.js.Publish(ctx, subject, payload, jetstream.WithMsgID(messageID))
	if err != nil {
		return false, err
	}
	return ack.Duplicate, nil
}

func (p jetStreamTimerSchedulePort) PublishNative(ctx context.Context, message *nats.Msg, messageID string) (bool, error) {
	ack, err := p.js.PublishMsg(ctx, message, jetstream.WithMsgID(messageID))
	if err != nil {
		return false, err
	}
	return ack.Duplicate, nil
}

// ScheduleTimerWithPort publishes one timer identity. A committed publish
// whose acknowledgment is lost returns an error; repeating the same identity
// is safe within the stream's duplicate window.
func ScheduleTimerWithPort(ctx context.Context, port TimerSchedulePort, native bool, typ, id string, invSeq, step uint64, fireAt time.Time) (newPublish bool, err error) {
	if err := identity.Validate(typ, id); err != nil {
		return false, err
	}
	if invSeq == 0 || fireAt.IsZero() {
		return false, fmt.Errorf("invalid timer generation or fire time")
	}
	attemptCtx, stopAttempt := context.WithTimeout(ctx, 3*time.Second)
	defer stopAttempt()
	messageID := "timer:" + identity.Key(typ, id) + ":" + strconv.FormatUint(invSeq, 10) + ":" + strconv.FormatUint(step, 10)
	if !native {
		payload, err := json.Marshal(struct {
			FireAt time.Time `json:"fire_at"`
		}{fireAt})
		if err != nil {
			return false, err
		}
		duplicate, err := port.PublishFallback(attemptCtx, identity.TimerSubject(typ, id, invSeq, step), payload, messageID)
		return err == nil && !duplicate, err
	}
	target := identity.RunSubject(typ, id, provision.Partitions)
	source := fmt.Sprintf("wf.schedule.%s.%s.%d", typ, id, step)
	message := &nats.Msg{Subject: source, Data: []byte(identity.Key(typ, id)), Header: nats.Header{}}
	message.Header.Set(jetstream.ScheduleHeader, "@at "+fireAt.UTC().Format(time.RFC3339Nano))
	message.Header.Set(jetstream.ScheduleTargetHeader, target)
	message.Header.Set(identity.TimerInvSeqHeader, strconv.FormatUint(invSeq, 10))
	message.Header.Set(identity.TimerStepHeader, strconv.FormatUint(step, 10))
	duplicate, err := port.PublishNative(attemptCtx, message, messageID)
	return err == nil && !duplicate, err
}
