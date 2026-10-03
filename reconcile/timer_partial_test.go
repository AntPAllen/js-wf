package reconcile

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"js-wf/journal"
)

type timerPrefixPort struct {
	suspendedPrefixPort
	stage string
}

func (p *timerPrefixPort) LastInvocationSequence(context.Context) (uint64, error) {
	if p.stage == "hole_info" {
		return 0, nats.ErrTimeout
	}
	return 3, nil
}
func (p *timerPrefixPort) ReadJournal(_ context.Context, _, id string) ([]journal.Record, error) {
	switch id {
	case "prefix-1":
		return nil, nil
	case "prefix-2":
		return []journal.Record{{Entry: journal.Entry{Kind: journal.Started}}}, nil
	}
	if p.stage == "journal" {
		return nil, nats.ErrTimeout
	}
	if p.stage == "retirement" {
		return []journal.Record{{Entry: journal.Entry{Kind: journal.Completed}}}, nil
	}
	return []journal.Record{{Entry: journal.Entry{Kind: journal.StepRequested, Payload: []byte(`{"kind":"timer","fire_at":"2020-01-01T00:00:00Z","clock_domain":"utc-quorum-v1"}`)}, Sequence: 6}}, nil
}
func (p *timerPrefixPort) EnqueueTimer(context.Context, string, string, uint64) error {
	return nats.ErrTimeout
}
func TestTimerPartialCursorDoesNotSkipUnconfirmedReadRetirementClockOrEnqueue(t *testing.T) {
	for _, stage := range []string{"first_read", "read", "hole_info", "journal", "retirement", "clock", "enqueue"} {
		t.Run(stage, func(t *testing.T) {
			port := &timerPrefixPort{stage: stage}
			switch stage {
			case "first_read":
				port.readError = 1
			case "read":
				port.readError = 3
			case "hole_info":
				port.hole = 3
			case "retirement":
				port.retirementError = 3
			}
			scan := NewTimerScanWithPort(port)
			scan.DomainNow = func(context.Context, string) (time.Time, error) {
				if stage == "clock" {
					return time.Time{}, nats.ErrTimeout
				}
				return time.Now(), nil
			}
			result, err := scan.Scan(context.Background(), 1, 500, false)
			if err == nil {
				t.Fatal("injected error not observed")
			}
			want := uint64(3)
			if stage == "first_read" {
				want = 0
			}
			if result.RetrySequence != want {
				t.Fatalf("retry=%d want%d result=%+v", result.RetrySequence, want, result)
			}
		})
	}
}

type fallbackPrefixPort struct {
	stage              string
	published, deleted int
}

func (p *fallbackPrefixPort) LastTimerSequence(context.Context) (uint64, error) {
	if p.stage == "info" {
		return 0, nats.ErrTimeout
	}
	return 3, nil
}
func (p *fallbackPrefixPort) GetTimer(_ context.Context, seq uint64) (*jetstream.RawStreamMsg, error) {
	if p.stage == "first_read" || (seq == 3 && p.stage == "read") {
		return nil, nats.ErrTimeout
	}
	fire := "2100-01-01T00:00:00Z"
	domain := ""
	if seq == 3 {
		fire = "2020-01-01T00:00:00Z"
		domain = "utc-quorum-v1"
	}
	return &jetstream.RawStreamMsg{Subject: fmt.Sprintf("wf.timer.test.prefix-%d.10.1", seq), Sequence: seq, Data: []byte(fmt.Sprintf(`{"fire_at":%q,"clock_domain":%q}`, fire, domain))}, nil
}
func (p *fallbackPrefixPort) StateValue(_ context.Context, key string) ([]byte, error) {
	if strings.HasSuffix(key, "prefix-3") {
		if p.stage == "state" {
			return nil, nats.ErrTimeout
		}
		if p.stage == "retired_delete" && strings.HasPrefix(key, "purging.") {
			return []byte("10"), nil
		}
	}
	return nil, jetstream.ErrKeyNotFound
}
func (p *fallbackPrefixPort) PublishWakeup(context.Context, *nats.Msg, string) error {
	p.published++
	if p.stage == "publish" {
		return nats.ErrTimeout
	}
	return nil
}
func (p *fallbackPrefixPort) DeleteTimer(context.Context, uint64) error {
	p.deleted++
	return nats.ErrTimeout
}
func TestFallbackTimerPartialCursorDoesNotSkipUncertainPublicationOrDeletion(t *testing.T) {
	for _, stage := range []string{"info", "first_read", "read", "state", "clock", "publish", "delete", "retired_delete"} {
		t.Run(stage, func(t *testing.T) {
			port := &fallbackPrefixPort{stage: stage}
			scan := NewFallbackTimerScanWithPort(port, func(context.Context) (time.Time, error) { return time.Now(), nil })
			scan.DomainNow = func(context.Context, string) (time.Time, error) {
				if stage == "clock" {
					return time.Time{}, nats.ErrTimeout
				}
				return time.Now(), nil
			}
			var events []RepairEvent
			scan.Observe = func(e RepairEvent) { events = append(events, e) }
			result, err := scan.Scan(context.Background(), 1, 500, false)
			if err == nil {
				t.Fatal("injected error not observed")
			}
			want := uint64(3)
			if stage == "info" || stage == "first_read" {
				want = 0
			}
			if result.RetrySequence != want {
				t.Fatalf("retry=%d want%d result=%+v", result.RetrySequence, want, result)
			}
			if stage == "publish" && port.deleted != 0 {
				t.Fatal("deleted after uncertain publication")
			}
			if stage == "delete" && (port.published != 1 || port.deleted != 1 || len(events) != 1 || events[0].Outcome != "acknowledged") {
				t.Fatalf("publication acknowledgment lost after deletion failure:%+v", events)
			}
		})
	}
}
