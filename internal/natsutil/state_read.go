package natsutil

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strings"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

var stateReadKeyPattern = regexp.MustCompile(`^[-/_=\.a-zA-Z0-9]+$`)

// GetStateMsgFromLeader reads the latest value and exact revision in WF_STATE.
// Snapshot manifests can refer to a prefix that has already been purged, so
// neither stale positive values nor weak absence are safe reconstruction roots.
// Administrative GetLastMsg routes to the stream leader without changing
// AllowDirect, and receives the context explicitly for the pinned legacy SDK.
func GetStateMsgFromLeader(ctx context.Context, js jetstream.JetStream, key string) (*nats.RawStreamMsg, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if !stateReadKeyPattern.MatchString(key) || strings.HasPrefix(key, ".") || strings.HasSuffix(key, ".") {
		return nil, jetstream.ErrInvalidKey
	}
	options := js.Options()
	if _, ok := ctx.Deadline(); !ok {
		var stop context.CancelFunc
		ctx, stop = context.WithTimeout(ctx, options.DefaultTimeout)
		defer stop()
	}
	if js.Conn() == nil {
		return nil, errors.New("state leader read requires NATS connection")
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
	subject := "$KV.WF_STATE." + key
	message, err := admin.GetLastMsg("KV_WF_STATE", subject, nats.Context(ctx))
	if err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		if errors.Is(err, nats.ErrMsgNotFound) {
			return nil, jetstream.ErrKeyNotFound
		}
		if errors.Is(err, nats.ErrStreamNotFound) {
			return nil, jetstream.ErrBucketNotFound
		}
		var api *nats.APIError
		if errors.As(err, &api) {
			return nil, &jetstream.APIError{Code: api.Code, ErrorCode: jetstream.ErrorCode(api.ErrorCode), Description: api.Description}
		}
		return nil, err
	}
	if message == nil || message.Subject != subject || message.Sequence == 0 {
		return nil, fmt.Errorf("state leader read: invalid identity for %s", key)
	}
	switch message.Header.Get("KV-Operation") {
	case "DEL", "PURGE":
		return nil, jetstream.ErrKeyNotFound
	}
	switch message.Header.Get(jetstream.MarkerReasonHeader) {
	case "MaxAge", "Purge", "Remove":
		return nil, jetstream.ErrKeyNotFound
	}
	return message, nil
}
