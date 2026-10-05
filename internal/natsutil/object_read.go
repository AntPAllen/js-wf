package natsutil

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// GetObjectBytes keeps the SDK's context-bounded chunk reader, but confirms
// missing metadata with the stream leader. Direct-enabled Object Stores may
// report absence from a follower that has not seen an acknowledged write.
// A confirmed existing object is retried within the caller's budget. This
// protects absence decisions; successful reads still require the caller's
// recorded content hash, as with other runtime-owned immutable references.
func GetObjectBytes(ctx context.Context, js jetstream.JetStream, bucket, name string) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if _, ok := ctx.Deadline(); !ok {
		var stop context.CancelFunc
		ctx, stop = context.WithTimeout(ctx, js.Options().DefaultTimeout)
		defer stop()
	}
	objects, err := js.ObjectStore(ctx, bucket)
	if err != nil {
		return nil, err
	}
	var admin nats.JetStreamContext
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		data, err := objects.GetBytes(ctx, name)
		if !errors.Is(err, jetstream.ErrObjectNotFound) {
			return data, err
		}
		if admin == nil {
			if js.Conn() == nil {
				return nil, errors.New("object absence confirmation requires NATS connection")
			}
			options := js.Options()
			var opts []nats.JSOpt
			if options.APIPrefix != "" {
				opts = append(opts, nats.APIPrefix(options.APIPrefix))
			} else if options.Domain != "" {
				opts = append(opts, nats.Domain(options.Domain))
			}
			if options.ClientTrace != nil {
				opts = append(opts, nats.ClientTrace{RequestSent: options.ClientTrace.RequestSent, ResponseReceived: options.ClientTrace.ResponseReceived})
			}
			admin, err = js.Conn().JetStream(opts...)
			if err != nil {
				return nil, err
			}
		}
		subject := "$O." + bucket + ".M." + base64.URLEncoding.EncodeToString([]byte(name))
		// Explicit Context is essential: the pinned legacy GetInfo and factory
		// context do not propagate it into administrative GetLastMsg requests.
		message, err := admin.GetLastMsg("OBJ_"+bucket, subject, nats.Context(ctx))
		if errors.Is(err, nats.ErrMsgNotFound) {
			return nil, jetstream.ErrObjectNotFound
		}
		if errors.Is(err, nats.ErrStreamNotFound) {
			return nil, jetstream.ErrBucketNotFound
		}
		if err != nil {
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			var api *nats.APIError
			if errors.As(err, &api) {
				return nil, &jetstream.APIError{Code: api.Code, ErrorCode: jetstream.ErrorCode(api.ErrorCode), Description: api.Description}
			}
			return nil, err
		}
		var info jetstream.ObjectInfo
		if message == nil || message.Subject != subject || message.Sequence == 0 || json.Unmarshal(message.Data, &info) != nil || info.Name != name || info.Bucket != bucket {
			return nil, jetstream.ErrBadObjectMeta
		}
		if info.Deleted {
			return nil, jetstream.ErrObjectNotFound
		}
		if info.NUID == "" {
			return nil, jetstream.ErrBadObjectMeta
		}
		timer := time.NewTimer(100 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		case <-timer.C:
		}
	}
}
