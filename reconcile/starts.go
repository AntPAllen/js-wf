package reconcile

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"js-wf/client"

	"github.com/nats-io/nats.go/jetstream"
)

type StartScan struct {
	Observe func(RepairEvent)
	port    StartScanPort
}

func NewStartScan(js jetstream.JetStream) *StartScan {
	return NewStartScanWithPort(&jetStreamStartScanPort{js: js, client: client.New(js)})
}

// StartScanPort contains the stream reads and run publish used by Scan.
type StartScanPort interface {
	GetInvocation(context.Context, uint64) (*jetstream.RawStreamMsg, error)
	LastInvocationSequence(context.Context) (uint64, error)
	JournalExists(context.Context, string) (bool, error)
	EnqueueStart(context.Context, string, string, uint64) error
}

func NewStartScanWithPort(port StartScanPort) *StartScan { return &StartScan{port: port} }

type jetStreamStartScanPort struct {
	js     jetstream.JetStream
	client *client.Client
	mu     sync.Mutex
	inv    jetstream.Stream
	jrn    jetstream.Stream
}

func (p *jetStreamStartScanPort) stream(ctx context.Context, name string) (jetstream.Stream, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if name == "WF_INV" && p.inv != nil {
		return p.inv, nil
	}
	if name == "WF_JRN" && p.jrn != nil {
		return p.jrn, nil
	}
	stream, err := p.js.Stream(ctx, name)
	if err != nil {
		return nil, err
	}
	if name == "WF_INV" {
		p.inv = stream
	} else {
		p.jrn = stream
	}
	return stream, nil
}

func (p *jetStreamStartScanPort) GetInvocation(ctx context.Context, sequence uint64) (*jetstream.RawStreamMsg, error) {
	stream, err := p.stream(ctx, "WF_INV")
	if err != nil {
		return nil, err
	}
	return stream.GetMsg(ctx, sequence)
}

func (p *jetStreamStartScanPort) LastInvocationSequence(ctx context.Context) (uint64, error) {
	stream, err := p.stream(ctx, "WF_INV")
	if err != nil {
		return 0, err
	}
	info, err := stream.Info(ctx)
	if err != nil {
		return 0, err
	}
	return info.State.LastSeq, nil
}

func (p *jetStreamStartScanPort) JournalExists(ctx context.Context, subject string) (bool, error) {
	stream, err := p.stream(ctx, "WF_JRN")
	if err != nil {
		return false, err
	}
	_, err = stream.GetLastMsgForSubject(ctx, subject)
	if errors.Is(err, jetstream.ErrMsgNotFound) {
		return false, nil
	}
	return err == nil, err
}

func (p *jetStreamStartScanPort) EnqueueStart(ctx context.Context, typ, id string, sequence uint64) error {
	return p.client.Enqueue(ctx, typ, id, "start:"+typ+"."+id+":"+strconv.FormatUint(sequence, 10))
}

type ScanResult struct {
	// RetrySequence is an optional checkpoint of a fully inspected prefix when
	// Scan returns a transient error. The failing invocation is never skipped.
	// Scanners that do not certify such a prefix leave it zero.
	RetrySequence uint64      `json:"retry_sequence,omitempty"`
	NextSequence  uint64      `json:"next_sequence"`
	Inspected     int         `json:"inspected"` // retained messages; holes also consume budget
	Reenqueued    int         `json:"reenqueued"`
	Removed       int         `json:"removed,omitempty"`
	Candidates    []Candidate `json:"candidates,omitempty"`
}

type Candidate struct {
	Type       string `json:"type"`
	ID         string `json:"id"`
	Reason     string `json:"reason"`
	JournalSeq uint64 `json:"journal_seq"`
}

// Scan repairs the gap between an acked WF_INV publish and its WF_RUN publish.
// next is a stream sequence cursor; zero starts at the beginning. The caller
// persists the cursor and runs the scan periodically. An extra wakeup is safe
// only once the worker treats an existing terminal journal as a no-op.
func (s *StartScan) Scan(ctx context.Context, next uint64, budget int, dryRun bool) (result ScanResult, scanErr error) {
	if budget < 1 {
		return ScanResult{}, fmt.Errorf("scan budget must be positive")
	}
	if next == 0 {
		next = 1
	}
	result = ScanResult{NextSequence: next}
	initial, confirmed := next, next
	defer func() {
		if scanErr != nil && confirmed > initial {
			result.RetrySequence = confirmed
		}
	}()
	for scanned := 0; scanned < budget; scanned++ {
		m, err := s.port.GetInvocation(ctx, next)
		if errors.Is(err, jetstream.ErrMsgNotFound) {
			last, infoErr := s.port.LastInvocationSequence(ctx)
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
		if len(parts) != 4 {
			return result, fmt.Errorf("invalid invocation subject %q", m.Subject)
		}
		typ, id := parts[2], parts[3]
		exists, err := s.port.JournalExists(ctx, "wf.jrn."+typ+"."+id)
		if err != nil {
			return result, err
		}
		if exists {
			confirmed = next
			continue
		}
		result.Reenqueued++
		event := RepairEvent{Kind: "start", Type: typ, ID: id, Reason: "missing_journal", SourceSequence: m.Sequence, InvocationSequence: m.Sequence}
		if dryRun {
			reportRepair(s.Observe, event, true, nil)
			confirmed = next
			continue
		}
		err = s.port.EnqueueStart(ctx, typ, id, m.Sequence)
		reportRepair(s.Observe, event, false, err)
		if err != nil {
			return result, err
		}
		confirmed = next
	}
	return result, nil
}

// RunStartLoop scans continuously with a KV lease so one process is active at
// a time. A replacement starts from the beginning; repeated enqueue attempts
// carry the same message ID and workers treat extra wakeups idempotently.
func RunStartLoop(ctx context.Context, js jetstream.JetStream, workerID string, interval time.Duration, budget int) error {
	return runLoop(ctx, js, workerID, "start", interval, budget, NewStartScan(js).Scan)
}
