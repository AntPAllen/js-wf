package reconcile

import (
	"context"
	"testing"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"js-wf/identity"
)

type nativeRetireRacePort struct {
	hint        *jetstream.RawStreamMsg
	deleted     uint64
	replacement *jetstream.RawStreamMsg
}

func (p *nativeRetireRacePort) NativeTimerSubjects(context.Context, string, string) ([]string, error) {
	return []string{p.hint.Subject}, nil
}
func (p *nativeRetireRacePort) LastNativeTimer(context.Context, string) (*jetstream.RawStreamMsg, error) {
	copy := *p.hint
	return &copy, nil
}
func (p *nativeRetireRacePort) DeleteNativeTimer(_ context.Context, sequence uint64) error {
	p.deleted = sequence
	// A new generation arrives after the retained read and before deletion.
	p.hint = p.replacement
	if sequence == p.hint.Sequence {
		tomb := *p.hint
		tomb.Data = nil
		p.hint = &tomb
	}
	return nil
}
func TestNativeRetirementDeleteDoesNotEraseConcurrentReplacement(t *testing.T) {
	port := &nativeRetireRacePort{hint: &jetstream.RawStreamMsg{Subject: "wf.schedule.test.one.1", Sequence: 11, Data: []byte("test.one"), Header: nats.Header{identity.TimerInvSeqHeader: {"7"}, identity.TimerStepHeader: {"1"}}}}
	newer := *port.hint
	newer.Sequence = 12
	newer.Header = nats.Header{identity.TimerInvSeqHeader: {"8"}, identity.TimerStepHeader: {"1"}}
	port.replacement = &newer
	removed, err := RetireNativeTimerHints(context.Background(), port, "test", "one", 7, false)
	if err != nil || removed != 1 || port.deleted != 11 || port.hint.Sequence != 12 || string(port.hint.Data) != "test.one" {
		t.Fatalf("removed=%d err=%v port=%+v", removed, err, port)
	}
}
func TestNativeRetirementRejectsUnboundHint(t *testing.T) {
	for _, mode := range []string{"subject", "data", "step", "generation"} {
		t.Run(mode, func(t *testing.T) {
			hint := &jetstream.RawStreamMsg{Subject: "wf.schedule.test.one.1", Sequence: 11, Data: []byte("test.one"), Header: nats.Header{identity.TimerInvSeqHeader: {"7"}, identity.TimerStepHeader: {"1"}}}
			switch mode {
			case "subject":
				hint.Subject = "wf.schedule.test.other.1"
			case "data":
				hint.Data = []byte("test.other")
			case "step":
				hint.Header.Set(identity.TimerStepHeader, "2")
			case "generation":
				hint.Header.Set(identity.TimerInvSeqHeader, "invalid")
			}
			port := &nativeRetireRacePort{hint: hint}
			if _, err := RetireNativeTimerHints(context.Background(), port, "test", "one", 7, false); err == nil || port.deleted != 0 {
				t.Fatalf("unbound deletion err=%v seq=%d", err, port.deleted)
			}
		})
	}
}
