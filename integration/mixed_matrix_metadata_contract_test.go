//go:build linux

package integration_test

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"js-wf/testcluster"
)

// Exercise the SDK's decoding of wire API replies, not manually constructed
// Go errors. The custom API prefix keeps the fixture's real JetStream API
// separate from the deliberately controlled metadata responses.
func TestMatrixMetadataReplyContract(t *testing.T) {
	cluster, err := testcluster.Start(t.TempDir(), 1)
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Close()
	nc := cluster.Clients[0]
	for _, tc := range []struct {
		name       string
		code       int
		failures   int
		wantCalls  int
		wantFailed bool
	}{
		{"temporary_then_success", 10008, 2, 3, false},
		{"temporary_exhausted", 10008, 10, 3, true},
		{"stream_missing", 10059, 10, 1, true},
		{"unclassified_api_error", 10158, 10, 1, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			prefix := "matrix.metadata." + tc.name
			var calls atomic.Int32
			sub, err := nc.Subscribe(prefix+".STREAM.INFO.WF_RUN", func(msg *nats.Msg) {
				n := calls.Add(1)
				body := `{"config":{"name":"WF_RUN"},"state":{"messages":0}}`
				if int(n) <= tc.failures {
					body = fmt.Sprintf(`{"error":{"code":503,"err_code":%d,"description":"controlled metadata reply"}}`, tc.code)
				}
				if err := msg.Respond([]byte(body)); err != nil {
					t.Errorf("metadata response: %v", err)
				}
			})
			if err != nil {
				t.Fatal(err)
			}
			defer sub.Unsubscribe()
			if err := nc.FlushTimeout(time.Second); err != nil {
				t.Fatal(err)
			}
			js, err := jetstream.NewWithAPIPrefix(nc, prefix)
			if err != nil {
				t.Fatal(err)
			}
			ctx, stop := context.WithTimeout(context.Background(), 3*time.Second)
			defer stop()
			stream, err := matrixReadMetadata(ctx, func(ctx context.Context) (jetstream.Stream, error) {
				return js.Stream(ctx, "WF_RUN")
			})
			if (err != nil) != tc.wantFailed || int(calls.Load()) != tc.wantCalls {
				t.Fatalf("wire replies: calls=%d want=%d error=%v want_failed=%v", calls.Load(), tc.wantCalls, err, tc.wantFailed)
			}
			if !tc.wantFailed && (stream == nil || stream.CachedInfo().Config.Name != "WF_RUN") {
				t.Fatal("successful reply did not produce the requested stream handle")
			}
		})
	}
}
