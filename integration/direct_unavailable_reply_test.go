package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"js-wf/internal/natsutil"
	"sync/atomic"
	"testing"
	"time"
)

// A custom API responder sends actual NATS status headers through the pinned
// nats.go decoder. This tests wire decoding, not a server election mechanism.
func TestDirectReadUnavailableReplyClassification(t *testing.T) {
	_, cluster := setup(t)
	nc := cluster.Clients[0]
	ctx, stop := context.WithTimeout(context.Background(), 10*time.Second)
	defer stop()
	const prefix, name = "fixture.unavailable", "AUDIT"
	var reads atomic.Int64
	info, _ := json.Marshal(jetstream.StreamInfo{Config: jetstream.StreamConfig{Name: name, AllowDirect: true}})
	infoSub, err := nc.Subscribe(prefix+".STREAM.INFO."+name, func(msg *nats.Msg) { _ = msg.Respond(info) })
	if err != nil {
		t.Fatal(err)
	}
	defer infoSub.Unsubscribe()
	readSub, err := nc.Subscribe(prefix+".DIRECT.GET."+name, func(msg *nats.Msg) {
		response := nats.NewMsg(msg.Reply)
		count := reads.Add(1)
		if count == 1 {
			response.Header.Set("Status", "500")
			response.Header.Set("Description", "JetStream system temporarily unavailable")
		} else if count == 2 {
			response.Header.Set("Status", "404")
			response.Header.Set("Description", "No Messages")
		} else {
			response.Header.Set("Status", "503")
			response.Header.Set("Description", "JetStream system temporarily unavailable")
		}
		_ = msg.RespondMsg(response)
	})
	if err != nil {
		t.Fatal(err)
	}
	defer readSub.Unsubscribe()
	if err := nc.Flush(); err != nil {
		t.Fatal(err)
	}
	js, err := jetstream.NewWithAPIPrefix(nc, prefix)
	if err != nil {
		t.Fatal(err)
	}
	stream, err := js.Stream(ctx, name)
	if err != nil {
		t.Fatal(err)
	}
	_, err = stream.GetMsg(ctx, 1)
	var api *jetstream.APIError
	if err == nil || errors.As(err, &api) || !natsutil.IsUnavailable(err) {
		t.Fatalf("decoded direct reply=%T %v API=%v", err, err, api)
	}
	t.Logf("decoded direct status 500: %T %v", err, err)
	_, err = stream.GetMsg(ctx, 1)
	if !errors.Is(err, jetstream.ErrMsgNotFound) || natsutil.IsUnavailable(err) || reads.Load() != 2 {
		t.Fatalf("404 semantic absence err=%v reads=%d", err, reads.Load())
	}
	_, err = stream.GetMsg(ctx, 1)
	if !errors.Is(err, nats.ErrNoResponders) || reads.Load() != 3 {
		t.Fatalf("core 503 reply err=%v reads=%d", err, reads.Load())
	}

}
