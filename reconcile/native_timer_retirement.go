package reconcile

import (
	"context"
	"fmt"

	"github.com/nats-io/nats.go/jetstream"
	"js-wf/internal/natsutil"
	"js-wf/retention"
)

// NativeTimerRetirePort exposes retained generation-bound timer hints.
type NativeTimerRetirePort = retention.NativeTimerRetirePort

// RetireNativeTimerHints shares retirement validation with the purge pipeline.
func RetireNativeTimerHints(ctx context.Context, port NativeTimerRetirePort, typ, id string, generation uint64, dryRun bool) (int, error) {
	return retention.RetireNativeTimerHints(ctx, port, typ, id, generation, dryRun)
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
	return DeleteNativeTimerWithPort(ctx, legacyNativeTimerDeletePort{p.js}, sequence)
}

// NativeTimerDeletePort preserves the API error before the modern SDK's
// DeleteMsg turns it into an untyped description. Only the observed sequence
// is deleted; the caller has already checked the terminal generation.
type NativeTimerDeletePort interface {
	DeleteRetainedTimer(context.Context, uint64) error
}

func DeleteNativeTimerWithPort(ctx context.Context, port NativeTimerDeletePort, sequence uint64) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if port == nil || sequence == 0 {
		return fmt.Errorf("invalid native timer delete request")
	}
	return natsutil.NormalizeMessageDeleteError(port.DeleteRetainedTimer(ctx, sequence))
}

type legacyNativeTimerDeletePort struct{ js jetstream.JetStream }

func (p legacyNativeTimerDeletePort) DeleteRetainedTimer(ctx context.Context, sequence uint64) error {
	return natsutil.RawDeleteStreamMessage(ctx, p.js, "WF_RUN", sequence)
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
