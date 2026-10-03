package reconcile

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"js-wf/client"
	"js-wf/identity"
	"js-wf/journal"

	"github.com/nats-io/nats.go/jetstream"
)

type SignalScan struct {
	Observe func(RepairEvent)
	port    SignalScanPort
}

func NewSignalScan(js jetstream.JetStream) *SignalScan {
	return NewSignalScanWithPort(&jetStreamSignalScanPort{js: js, client: client.New(js), jrn: journal.New(js)})
}

// SignalScanPort contains the retained reads and run enqueue used by Scan.
type SignalScanPort interface {
	GetSignal(context.Context, uint64) (*jetstream.RawStreamMsg, error)
	LastSignalSequence(context.Context) (uint64, error)
	ReadJournal(context.Context, string, string) ([]journal.Record, error)
	LastInvocation(context.Context, string) (*jetstream.RawStreamMsg, error)
	EnqueueSignal(context.Context, string, string, uint64) error
}

func NewSignalScanWithPort(port SignalScanPort) *SignalScan { return &SignalScan{port: port} }

type jetStreamSignalScanPort struct {
	js     jetstream.JetStream
	client *client.Client
	jrn    *journal.Store
	mu     sync.Mutex
	sig    jetstream.Stream
	inv    jetstream.Stream
}

func (p *jetStreamSignalScanPort) stream(ctx context.Context, name string) (jetstream.Stream, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if name == "WF_SIG" && p.sig != nil {
		return p.sig, nil
	}
	if name == "WF_INV" && p.inv != nil {
		return p.inv, nil
	}
	stream, err := p.js.Stream(ctx, name)
	if err != nil {
		return nil, err
	}
	if name == "WF_SIG" {
		p.sig = stream
	} else {
		p.inv = stream
	}
	return stream, nil
}

func (p *jetStreamSignalScanPort) GetSignal(ctx context.Context, sequence uint64) (*jetstream.RawStreamMsg, error) {
	stream, err := p.stream(ctx, "WF_SIG")
	if err != nil {
		return nil, err
	}
	return stream.GetMsg(ctx, sequence)
}

func (p *jetStreamSignalScanPort) LastSignalSequence(ctx context.Context) (uint64, error) {
	stream, err := p.stream(ctx, "WF_SIG")
	if err != nil {
		return 0, err
	}
	info, err := stream.Info(ctx)
	if err != nil {
		return 0, err
	}
	return info.State.LastSeq, nil
}

func (p *jetStreamSignalScanPort) ReadJournal(ctx context.Context, typ, id string) ([]journal.Record, error) {
	records, _, err := p.jrn.Read(ctx, typ, id)
	return records, err
}

func (p *jetStreamSignalScanPort) LastInvocation(ctx context.Context, subject string) (*jetstream.RawStreamMsg, error) {
	stream, err := p.stream(ctx, "WF_INV")
	if err != nil {
		return nil, err
	}
	return stream.GetLastMsgForSubject(ctx, subject)
}

func (p *jetStreamSignalScanPort) EnqueueSignal(ctx context.Context, typ, id string, sequence uint64) error {
	return p.client.Enqueue(ctx, typ, id, fmt.Sprintf("signal-wakeup:%d", sequence))
}

type signalJournalState struct {
	terminal bool
	consumed map[uint64]bool
}

// Scan repairs signal publishes whose matching wakeup was interrupted. The
// cursor is a WF_SIG stream sequence. It is safe to scan the same range again;
// the worker's lease and terminal check make duplicate wakeups harmless.
func (s *SignalScan) Scan(ctx context.Context, next uint64, budget int, dryRun bool) (result ScanResult, scanErr error) {
	if budget < 1 {
		return ScanResult{}, fmt.Errorf("scan budget must be positive")
	}
	if next == 0 {
		next = 1
	}
	cache := map[string]signalJournalState{}
	result = ScanResult{NextSequence: next}
	initial, confirmed := next, next
	defer func() {
		if scanErr != nil && confirmed > initial {
			result.RetrySequence = confirmed
		}
	}()
	for scanned := 0; scanned < budget; scanned++ {
		m, err := s.port.GetSignal(ctx, next)
		if errors.Is(err, jetstream.ErrMsgNotFound) {
			last, infoErr := s.port.LastSignalSequence(ctx)
			if infoErr != nil {
				return result, infoErr
			}
			if next > last {
				result.NextSequence = 1
				return result, nil
			}
			next++
			result.NextSequence = next
			confirmed = next
			continue
		}
		if err != nil {
			return result, err
		}
		next = m.Sequence + 1
		result.NextSequence = next
		result.Inspected++
		parts := strings.Split(m.Subject, ".")
		if len(parts) != 5 || parts[0] != "wf" || parts[1] != "sig" {
			return result, fmt.Errorf("invalid signal subject %q", m.Subject)
		}
		typ, id := parts[2], parts[3]
		if err := identity.Validate(typ, id); err != nil {
			return result, err
		}
		key := identity.Key(typ, id)
		state, ok := cache[key]
		if !ok {
			records, err := s.port.ReadJournal(ctx, typ, id)
			if err != nil {
				return result, err
			}
			state = signalJournalState{consumed: map[uint64]bool{}}
			for _, r := range records {
				if r.Kind == journal.Completed || r.Kind == journal.Failed {
					state.terminal = true
				}
				if r.Kind == journal.SignalConsumed {
					var event struct {
						Sequence uint64 `json:"sig_seq"`
					}
					if err := json.Unmarshal(r.Payload, &event); err != nil {
						return result, err
					}
					state.consumed[event.Sequence] = true
				}
			}
			cache[key] = state
		}
		if state.terminal || state.consumed[m.Sequence] {
			confirmed = next
			continue
		}
		invocation, err := s.port.LastInvocation(ctx, identity.InvocationSubject(typ, id))
		if errors.Is(err, jetstream.ErrMsgNotFound) {
			confirmed = next
			continue
		} else if err != nil {
			return result, err
		}
		if generation := m.Header.Get("Wf-Inv-Seq"); generation != "" && generation != strconv.FormatUint(invocation.Sequence, 10) {
			confirmed = next
			continue
		}
		result.Reenqueued++
		event := RepairEvent{Kind: "signal", Type: typ, ID: id, Reason: "unconsumed_signal_nonterminal_generation", SourceSequence: m.Sequence, InvocationSequence: invocation.Sequence}
		if dryRun {
			reportRepair(s.Observe, event, true, nil)
			confirmed = next
			continue
		}
		err = s.port.EnqueueSignal(ctx, typ, id, m.Sequence)
		reportRepair(s.Observe, event, false, err)
		if err != nil {
			return result, err
		}
		confirmed = next
	}
	return result, nil
}

// RunSignalLoop keeps one signal reconciler active with a KV lease.
func RunSignalLoop(ctx context.Context, js jetstream.JetStream, workerID string, interval time.Duration, budget int) error {
	return runLoop(ctx, js, workerID, "signal", interval, budget, NewSignalScan(js).Scan)
}
