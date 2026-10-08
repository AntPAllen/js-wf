package integrity

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/nats-io/nats.go/jetstream"
	"js-wf/identity"
)

// CaptureInvocationCutoff accepts a leader message read, not a StreamInfo
// snapshot. minimum is the highest acknowledged Start in the completed cohort.
// The caller retains its existing request/retry budget and must quiesce that
// cohort and forbid its purge/reuse throughout the audit.
func CaptureInvocationCutoff(ctx context.Context, minimum uint64, read func(context.Context) (*jetstream.RawStreamMsg, error)) (uint64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if minimum == 0 || read == nil {
		return 0, errors.New("acknowledged cohort boundary and leader reader required")
	}
	raw, err := read(ctx)
	if err != nil {
		return 0, err
	}
	if raw == nil {
		return 0, errors.New("cohort leader read returned nil")
	}
	parts := strings.Split(raw.Subject, ".")
	if len(parts) != 4 || parts[0] != "wf" || parts[1] != "inv" || identity.Validate(parts[2], parts[3]) != nil {
		return 0, errors.New("cohort leader read returned invalid invocation")
	}
	if raw.Sequence < minimum {
		return 0, fmt.Errorf("cohort tail %d precedes acknowledged Start %d", raw.Sequence, minimum)
	}
	return raw.Sequence, nil
}

// An explicitly captured cohort is authoritative over a stale positive or
// stale empty metadata reply. Metadata still supplies the retained first
// sequence; reads verify every requested coordinate under the existing budget.
func retainedScanBounds(state jetstream.StreamState, cutoff *uint64) (first, last uint64) {
	first, last = state.FirstSeq, state.LastSeq
	if cutoff != nil {
		last = *cutoff
		if first == 0 && last > 0 {
			first = 1
		}
	}
	return
}

type RetainedPointReadPort interface {
	Info(context.Context, ...jetstream.StreamInfoOpt) (*jetstream.StreamInfo, error)
	GetMsg(context.Context, uint64, ...jetstream.GetMsgOpt) (*jetstream.RawStreamMsg, error)
}

// ScanRetainedPointReadsThrough runs the production ordered point scanner
// against its narrow read port, including the immutable captured boundary.
func ScanRetainedPointReadsThrough(ctx context.Context, port RetainedPointReadPort, cutoff uint64, visit func(*jetstream.RawStreamMsg) error) error {
	return scanPointThrough(ctx, port, &cutoff, visit)
}

// ValidateCompletedCohortReport preserves the matrix's exact completed-cohort
// count gate. A checker report can legitimately describe running workflows;
// the completed-cohort caller must require every expected journal and terminal.
func ValidateCompletedCohortReport(report Report, expected int) error {
	if expected < 1 || report.Invocations != expected || report.Journals != expected || report.Terminal != expected {
		return fmt.Errorf("completed cohort report=%+v expected=%d", report, expected)
	}
	return nil
}
