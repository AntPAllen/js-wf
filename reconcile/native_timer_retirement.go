package reconcile

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/nats-io/nats.go/jetstream"
	"js-wf/identity"
)

// NativeTimerRetirePort exposes retained hints, not delivery or clock decisions.
// A terminal journal is required before RetireNativeTimerHints is called.
type NativeTimerRetirePort interface {
	NativeTimerSubjects(context.Context, string, string) ([]string, error)
	LastNativeTimer(context.Context, string) (*jetstream.RawStreamMsg, error)
	DeleteNativeTimer(context.Context, uint64) error
}

// RetireNativeTimerHints removes only observed messages for the terminal
// invocation generation. Sequence deletion cannot erase a concurrent replacement
// on the same subject; later publications are found on the next scan. Lost delete
// replies are ambiguous and retries are safe. Pending timers never enter here.
func RetireNativeTimerHints(ctx context.Context, port NativeTimerRetirePort, typ, id string, generation uint64, dryRun bool) (int, error) {
	if err := identity.Validate(typ, id); err != nil {
		return 0, err
	}
	if generation == 0 {
		return 0, fmt.Errorf("invalid terminal generation")
	}
	subjects, err := port.NativeTimerSubjects(ctx, typ, id)
	if err != nil {
		return 0, err
	}
	sort.Strings(subjects)
	var removed int
	prefix := "wf.schedule." + typ + "." + id + "."
	for _, subject := range subjects {
		if !strings.HasPrefix(subject, prefix) {
			return removed, fmt.Errorf("native schedule subject outside terminal invocation: %q", subject)
		}
		step, err := strconv.ParseUint(strings.TrimPrefix(subject, prefix), 10, 64)
		if err != nil {
			return removed, fmt.Errorf("invalid native schedule step: %q", subject)
		}
		hint, err := port.LastNativeTimer(ctx, subject)
		if errors.Is(err, jetstream.ErrMsgNotFound) {
			continue
		}
		if err != nil {
			return removed, err
		}
		if hint == nil || hint.Subject != subject || hint.Sequence == 0 || string(hint.Data) != identity.Key(typ, id) {
			return removed, fmt.Errorf("invalid retained native schedule: %q", subject)
		}
		invSeq, err := strconv.ParseUint(hint.Header.Get(identity.TimerInvSeqHeader), 10, 64)
		if err != nil || invSeq == 0 || hint.Header.Get(identity.TimerStepHeader) != strconv.FormatUint(step, 10) {
			return removed, fmt.Errorf("invalid native schedule generation/step: %q", subject)
		}
		if invSeq != generation {
			continue
		}
		if !dryRun {
			if err := port.DeleteNativeTimer(ctx, hint.Sequence); err != nil && !errors.Is(err, jetstream.ErrMsgNotFound) {
				return removed, err
			}
		}
		removed++
	}
	return removed, nil
}

func (p jetStreamNativeTimerRetirePort) NativeTimerSubjects(ctx context.Context, typ, id string) ([]string, error) {
	stream, err := p.js.Stream(ctx, "WF_RUN")
	if err != nil {
		return nil, err
	}
	info, err := stream.Info(ctx, jetstream.WithSubjectFilter("wf.schedule."+typ+"."+id+".*"))
	if err != nil {
		return nil, err
	}
	subjects := make([]string, 0, len(info.State.Subjects))
	for subject := range info.State.Subjects {
		subjects = append(subjects, subject)
	}
	return subjects, nil
}
func (p jetStreamNativeTimerRetirePort) LastNativeTimer(ctx context.Context, subject string) (*jetstream.RawStreamMsg, error) {
	stream, err := p.js.Stream(ctx, "WF_RUN")
	if err != nil {
		return nil, err
	}
	return stream.GetLastMsgForSubject(ctx, subject)
}
func (p jetStreamNativeTimerRetirePort) DeleteNativeTimer(ctx context.Context, sequence uint64) error {
	stream, err := p.js.Stream(ctx, "WF_RUN")
	if err != nil {
		return err
	}
	return stream.DeleteMsg(ctx, sequence)
}

type jetStreamNativeTimerRetirePort struct{ js jetstream.JetStream }

func (p *jetStreamTimerScanPort) NativeTimerSubjects(ctx context.Context, typ, id string) ([]string, error) {
	return (jetStreamNativeTimerRetirePort{p.js}).NativeTimerSubjects(ctx, typ, id)
}
func (p *jetStreamTimerScanPort) LastNativeTimer(ctx context.Context, subject string) (*jetstream.RawStreamMsg, error) {
	return (jetStreamNativeTimerRetirePort{p.js}).LastNativeTimer(ctx, subject)
}
func (p *jetStreamTimerScanPort) DeleteNativeTimer(ctx context.Context, seq uint64) error {
	return (jetStreamNativeTimerRetirePort{p.js}).DeleteNativeTimer(ctx, seq)
}
func (p *jetStreamSuspendedScanPort) NativeTimerSubjects(ctx context.Context, typ, id string) ([]string, error) {
	return (jetStreamNativeTimerRetirePort{p.js}).NativeTimerSubjects(ctx, typ, id)
}
func (p *jetStreamSuspendedScanPort) LastNativeTimer(ctx context.Context, subject string) (*jetstream.RawStreamMsg, error) {
	return (jetStreamNativeTimerRetirePort{p.js}).LastNativeTimer(ctx, subject)
}
func (p *jetStreamSuspendedScanPort) DeleteNativeTimer(ctx context.Context, seq uint64) error {
	return (jetStreamNativeTimerRetirePort{p.js}).DeleteNativeTimer(ctx, seq)
}
