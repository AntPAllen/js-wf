package reconcile

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"js-wf/identity"
	"js-wf/journal"
)

// GraphFallbackTimerScanPort supplies native source validation in addition to
// disposable timer hints. StateValue is never consulted in graph mode.
type GraphFallbackTimerScanPort interface {
	FallbackTimerScanPort
	LastInvocation(context.Context, string) (*jetstream.RawStreamMsg, error)
	GraphRepairSchedulingPort
}

func NewFallbackTimerScanWithGraphJournal(js jetstream.JetStream, graph *journal.GraphStore) (*FallbackTimerScan, error) {
	if graph == nil || !graph.CanonicalStarts() {
		return nil, fmt.Errorf("graph fallback scanner requires canonical Start store")
	}
	s := NewFallbackTimerScan(js)
	s.graph = graph
	return s, nil
}

func NewFallbackTimerScanWithGraphJournalPort(port GraphFallbackTimerScanPort, graph *journal.GraphStore, now func(context.Context) (time.Time, error)) (*FallbackTimerScan, error) {
	if port == nil || graph == nil || !graph.CanonicalStarts() || now == nil {
		return nil, fmt.Errorf("graph fallback scanner requires canonical Start store, source port and clock")
	}
	s := NewFallbackTimerScanWithPort(port, now)
	s.graph = graph
	return s, nil
}

func (p *jetStreamFallbackTimerScanPort) LastInvocation(ctx context.Context, subject string) (*jetstream.RawStreamMsg, error) {
	stream, err := p.js.Stream(ctx, "WF_INV")
	if err != nil {
		return nil, err
	}
	return stream.GetLastMsgForSubject(ctx, subject)
}
func (p *jetStreamFallbackTimerScanPort) GraphRepairBlocked(ctx context.Context, typ, id string) (bool, error) {
	return graphRepairBlocked(ctx, p.js, typ, id)
}

// A hint cannot override canonical retirement, generation or timer declaration.
// Readers are postponed during live delivery leases, without deleting the hint.
func (s *FallbackTimerScan) graphTimerDecision(ctx context.Context, typ, id string, generation, step uint64, fireAt time.Time, domain string) (retired, ready bool, err error) {
	port, ok := s.port.(GraphFallbackTimerScanPort)
	if !ok {
		return false, false, fmt.Errorf("graph fallback source port missing")
	}
	status, err := s.graph.InspectStart(ctx, typ, id)
	if err != nil {
		return false, false, err
	}
	if status == nil || status.State.Pending {
		return false, false, journal.ErrUnknown
	}
	if status.State.Invocation != generation {
		if status.State.Invocation > generation {
			return true, true, nil
		}
		return false, false, journal.ErrUnknown
	}
	if status.Retired || status.Purging || status.Kind == journal.Completed || status.Kind == journal.Failed {
		return true, true, nil
	}
	blocked, err := port.GraphRepairBlocked(ctx, typ, id)
	if err != nil {
		return false, false, fmt.Errorf("%w: %w", journal.ErrUnknown, err)
	}
	if blocked {
		return false, false, nil
	}
	input, err := port.LastInvocation(ctx, identity.InvocationSubject(typ, id))
	if err != nil || input == nil || input.Sequence != generation {
		return false, false, fmt.Errorf("%w: graph fallback invocation source: %v", journal.ErrUnknown, err)
	}
	view, err := s.graph.OpenExisting(ctx, typ, id, generation)
	if err != nil {
		return false, false, err
	}
	if view == nil {
		return false, false, journal.ErrUnknown
	}
	defer func() {
		cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), 3*time.Second)
		defer cancel()
		if closeErr := view.Close(cleanup); err == nil && closeErr != nil {
			retired, ready, err = false, false, closeErr
		}
	}()
	if err = view.ValidateStartInvocation(ctx, input); err != nil {
		return false, false, err
	}
	var position uint64
	for i := uint64(0); i < view.Count(); i++ {
		record, readErr := view.Read(ctx, i)
		if readErr != nil {
			return false, false, readErr
		}
		if record.Kind == journal.StepRequested {
			if position == step {
				var request struct {
					Kind        string    `json:"kind"`
					FireAt      time.Time `json:"fire_at"`
					ClockDomain string    `json:"clock_domain"`
				}
				if json.Unmarshal(record.Payload, &request) != nil || (request.Kind != "timer" && request.Kind != "timer_start") || !request.FireAt.Equal(fireAt) || request.ClockDomain != domain {
					return false, false, journal.ErrGap
				}
				return false, true, nil
			}
			position++
		} else if record.Kind == journal.StepCompleted {
			position++
		}
	}
	return false, false, journal.ErrGap
}
