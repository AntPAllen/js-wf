package integrity

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// Direct KV reads may come from followers. An invariant check must ask the
// stream leader before certifying a terminal value or its absence. The legacy
// administrative GetLastMsg API defaults to leader routing, independently of
// the stream's AllowDirect setting; no stream configuration is changed.
func terminalLeaderReader(js jetstream.JetStream) func(context.Context, string) ([]byte, error) {
	var once sync.Once
	var admin nats.JetStreamContext
	var initErr error
	return func(ctx context.Context, key string) ([]byte, error) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		once.Do(func() {
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
			if js.Conn() == nil {
				initErr = errors.New("terminal leader read requires NATS connection")
				return
			}
			admin, initErr = js.Conn().JetStream(opts...)
		})
		if initErr != nil {
			return nil, initErr
		}
		subject := "$KV.WF_STATE." + key
		message, err := admin.GetLastMsg("KV_WF_STATE", subject, nats.Context(ctx))
		if errors.Is(err, nats.ErrMsgNotFound) {
			return nil, jetstream.ErrKeyNotFound
		}
		if err != nil {
			return nil, err
		}
		if message == nil || message.Subject != subject || message.Sequence == 0 {
			return nil, fmt.Errorf("terminal leader read: invalid identity for %s", key)
		}
		switch message.Header.Get("KV-Operation") {
		case "DEL", "PURGE":
			return nil, jetstream.ErrKeyNotFound
		}
		switch message.Header.Get(jetstream.MarkerReasonHeader) {
		case "MaxAge", "Purge", "Remove":
			return nil, jetstream.ErrKeyNotFound
		}
		return message.Data, nil
	}
}
