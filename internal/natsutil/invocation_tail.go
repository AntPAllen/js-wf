package natsutil

import (
	"context"
	"errors"
	"strings"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"js-wf/identity"
)

// GetInvocationTailFromLeader reads the actual retained WF_INV tail through
// the administrative message API. A StreamInfo snapshot can trail acknowledged
// starts during leader changes and is not a committed cohort boundary.
func GetInvocationTailFromLeader(ctx context.Context, js jetstream.JetStream) (*jetstream.RawStreamMsg, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	options := js.Options()
	if _, ok := ctx.Deadline(); !ok {
		var stop context.CancelFunc
		ctx, stop = context.WithTimeout(ctx, options.DefaultTimeout)
		defer stop()
	}
	if js.Conn() == nil {
		return nil, errors.New("invocation leader read requires NATS connection")
	}
	var opts []nats.JSOpt
	if options.APIPrefix != "" {
		opts = append(opts, nats.APIPrefix(options.APIPrefix))
	} else if options.Domain != "" {
		opts = append(opts, nats.Domain(options.Domain))
	}
	if options.ClientTrace != nil {
		opts = append(opts, nats.ClientTrace{RequestSent: options.ClientTrace.RequestSent, ResponseReceived: options.ClientTrace.ResponseReceived})
	}
	admin, err := js.Conn().JetStream(opts...)
	if err != nil {
		return nil, err
	}
	raw, err := admin.GetLastMsg("WF_INV", "wf.inv.>", nats.Context(ctx))
	if err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		if errors.Is(err, nats.ErrMsgNotFound) {
			return nil, jetstream.ErrMsgNotFound
		}
		if errors.Is(err, nats.ErrStreamNotFound) {
			return nil, jetstream.ErrStreamNotFound
		}
		var api *nats.APIError
		if errors.As(err, &api) {
			return nil, &jetstream.APIError{Code: api.Code, ErrorCode: jetstream.ErrorCode(api.ErrorCode), Description: api.Description}
		}
		return nil, err
	}
	if raw == nil {
		return nil, errors.New("invocation leader read returned nil")
	}
	parts := strings.Split(raw.Subject, ".")
	if len(parts) != 4 || parts[0] != "wf" || parts[1] != "inv" || identity.Validate(parts[2], parts[3]) != nil || raw.Sequence == 0 {
		return nil, errors.New("invocation leader read returned invalid identity")
	}
	return &jetstream.RawStreamMsg{Subject: raw.Subject, Sequence: raw.Sequence, Header: raw.Header, Data: raw.Data, Time: raw.Time}, nil
}
