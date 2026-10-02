// Package natsutil preserves reply forms used by the pinned NATS client.
package natsutil

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

var ErrInvalidDeleteReply = errors.New("message delete reply did not confirm success")

// NormalizeMessageDeleteError retains typed API errors across the legacy and
// modern SDK boundary. Other transports keep their original error identity.
func NormalizeMessageDeleteError(err error) error {
	var api *nats.APIError
	if errors.As(err, &api) {
		// 10057 is a general deletion failure, also used for denied deletion
		// and store errors. Its exact not-found description is the only benign
		// case besides the ordinary 404/10037 message-not-found response.
		if api.ErrorCode == 10037 || (api.Code == 500 && api.ErrorCode == 10057 && api.Description == "no message found") {
			return jetstream.ErrMsgNotFound
		}
		return &jetstream.APIError{Code: api.Code, ErrorCode: jetstream.ErrorCode(api.ErrorCode), Description: api.Description}
	}
	return err
}

// DeleteStreamMessage uses a public client API that preserves deletion errors.
func DeleteStreamMessage(ctx context.Context, js jetstream.JetStream, stream string, sequence uint64) error {
	return NormalizeMessageDeleteError(RawDeleteStreamMessage(ctx, js, stream, sequence))
}

// RawDeleteStreamMessage returns the original legacy API reply error for narrow
// transport adapters. Routing, client trace and deadline policy follow js.
func RawDeleteStreamMessage(ctx context.Context, js jetstream.JetStream, stream string, sequence uint64) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	options := js.Options()
	// Preserve the modern client's default deadline semantics and API routing.
	if _, present := ctx.Deadline(); !present {
		var stop context.CancelFunc
		ctx, stop = context.WithTimeout(ctx, options.DefaultTimeout)
		defer stop()
	}
	var legacyOptions []nats.JSOpt
	if options.Domain != "" {
		legacyOptions = append(legacyOptions, nats.Domain(options.Domain))
	} else if options.APIPrefix != "" {
		legacyOptions = append(legacyOptions, nats.APIPrefix(options.APIPrefix))
	}
	// Unlike modern DeleteMsg, the legacy method retains API errors. It does
	// not require success=true on an error-free response, so retain that
	// validation here without interpreting flattened error strings.
	var confirmed bool
	legacyOptions = append(legacyOptions, nats.ClientTrace{
		RequestSent: func(subject string, data []byte) {
			if options.ClientTrace != nil && options.ClientTrace.RequestSent != nil {
				options.ClientTrace.RequestSent(subject, data)
			}
		},
		ResponseReceived: func(subject string, data []byte, headers nats.Header) {
			var reply struct {
				Success bool `json:"success"`
			}
			confirmed = json.Unmarshal(data, &reply) == nil && reply.Success
			if options.ClientTrace != nil && options.ClientTrace.ResponseReceived != nil {
				options.ClientTrace.ResponseReceived(subject, data, headers)
			}
		},
	})
	legacy, err := js.Conn().JetStream(legacyOptions...)
	if err != nil {
		return err
	}
	if err := legacy.DeleteMsg(stream, sequence, nats.Context(ctx)); err != nil {
		return err
	}
	if !confirmed {
		return ErrInvalidDeleteReply
	}
	return nil
}
