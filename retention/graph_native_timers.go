package retention

import (
	"context"
	"github.com/nats-io/nats.go/jetstream"
	"js-wf/internal/natsutil"
)

func (p *jetStreamPurgePort) NativeTimerSubjects(ctx context.Context, typ, id string) ([]string, error) {
	stream, err := p.stream(ctx, "WF_RUN")
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
func (p *jetStreamPurgePort) LastNativeTimer(ctx context.Context, subject string) (*jetstream.RawStreamMsg, error) {
	stream, err := p.stream(ctx, "WF_RUN")
	if err != nil {
		return nil, err
	}
	return stream.GetLastMsgForSubject(ctx, subject)
}
func (p *jetStreamPurgePort) DeleteNativeTimer(ctx context.Context, sequence uint64) error {
	return natsutil.NormalizeMessageDeleteError(natsutil.RawDeleteStreamMessage(ctx, p.js, "WF_RUN", sequence))
}
