package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"js-wf/client"
	"js-wf/internal/natsutil"
	"js-wf/journal"
	"js-wf/reconcile"
	"js-wf/worker"
)

// A real NATS request/reply boundary forwards normal JetStream APIs unchanged.
// The selected delete reply is injected after retaining an actual native hint;
// absent/lost cases perform the actual deletion first. Both public API routing
// forms and the preserved client trace callbacks are checked.
func TestNativeTimerDeleteReplyClassification(t *testing.T) {
	all, _ := setup(t)
	ctx, stop := context.WithTimeout(context.Background(), 60*time.Second)
	defer stop()
	for _, route := range []string{"prefix", "domain"} {
		for _, mode := range []string{"unavailable", "already_absent", "ordinary_not_found", "delete_denied", "store_failure", "lost_reply", "missing_success"} {
			t.Run(route+"/"+mode, func(t *testing.T) {
				id := route + "-" + mode
				h, err := client.New(all[0]).Start(ctx, "test", id, []byte(`null`))
				if err != nil {
					t.Fatal(err)
				}
				store := journal.New(all[0])
				seq, err := store.Append(ctx, "test", id, journal.Entry{Kind: journal.Started, Index: 0, Epoch: 1, Payload: []byte(`null`)}, 0)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := store.Append(ctx, "test", id, journal.Entry{Kind: journal.Completed, Index: 1, Epoch: 1, Payload: []byte(`{"result":42}`)}, seq); err != nil {
					t.Fatal(err)
				}
				due := time.Now().Add(time.Second)
				if _, err := worker.ScheduleTimerDeadlineWithPort(ctx, worker.NewTimerSchedulePort(all[1]), true, "test", id, h.InvSeq, 1, worker.TimerDeadline{FireAt: due, ClockDomain: "utc-quorum-v1", ScheduleAt: due.Add(time.Minute)}); err != nil {
					t.Fatal(err)
				}
				prefix := "DELETE_REPLY_" + id + "."
				if route == "domain" {
					prefix = "$JS." + id + ".API."
				}
				var deletes, sent, received atomic.Int64
				var forwardingError atomic.Pointer[error]
				nc := all[2].Conn()
				sub, err := nc.Subscribe(prefix+">", func(message *nats.Msg) {
					requestCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
					defer cancel()
					realSubject := "$JS.API." + strings.TrimPrefix(message.Subject, prefix)
					isDelete := strings.HasSuffix(message.Subject, ".STREAM.MSG.DELETE.WF_RUN")
					if isDelete && deletes.Add(1) == 1 {
						if mode == "already_absent" || mode == "ordinary_not_found" || mode == "lost_reply" {
							reply, err := nc.RequestWithContext(requestCtx, realSubject, message.Data)
							if err != nil {
								forwardingError.Store(&err)
								return
							}
							var result struct {
								Success bool `json:"success"`
							}
							if json.Unmarshal(reply.Data, &result) != nil || !result.Success {
								err = fmt.Errorf("real delete did not commit: %s", reply.Data)
								forwardingError.Store(&err)
								return
							}
						}
						if mode == "lost_reply" {
							return
						}
						if mode == "missing_success" {
							_ = message.Respond([]byte(`{"success":false}`))
							return
						}
						api := jetstream.APIError{Code: 500, ErrorCode: 10057, Description: "no message found"}
						switch mode {
						case "unavailable":
							api = jetstream.APIError{Code: 503, ErrorCode: 10008, Description: "JetStream system temporarily unavailable"}
						case "ordinary_not_found":
							api.Code = 404
							api.ErrorCode = 10037
						case "delete_denied":
							api.Description = "message delete not permitted"
						case "store_failure":
							api.Description = "disk I/O failure"
						}
						body, _ := json.Marshal(struct {
							Error jetstream.APIError `json:"error"`
						}{api})
						_ = message.Respond(body)
						return
					}
					reply, err := nc.RequestWithContext(requestCtx, realSubject, message.Data)
					if err != nil {
						forwardingError.Store(&err)
						return
					}
					_ = message.RespondMsg(&nats.Msg{Data: reply.Data, Header: reply.Header})
				})
				if err != nil {
					t.Fatal(err)
				}
				defer sub.Unsubscribe()
				if err := nc.FlushTimeout(time.Second); err != nil {
					t.Fatal(err)
				}
				trace := jetstream.WithClientTrace(&jetstream.ClientTrace{RequestSent: func(subject string, _ []byte) {
					if strings.HasSuffix(subject, ".STREAM.MSG.DELETE.WF_RUN") {
						sent.Add(1)
					}
				}, ResponseReceived: func(subject string, _ []byte, _ nats.Header) {
					if strings.HasSuffix(subject, ".STREAM.MSG.DELETE.WF_RUN") {
						received.Add(1)
					}
				}})
				var routed jetstream.JetStream
				if route == "domain" {
					routed, err = jetstream.NewWithDomain(nc, id, trace)
				} else {
					routed, err = jetstream.NewWithAPIPrefix(nc, prefix, trace)
				}
				if err != nil {
					t.Fatal(err)
				}
				scan := reconcile.NewSuspendedScan(routed)
				attempt, cancel := context.WithTimeout(ctx, 2*time.Second)
				result, scanErr := scan.Scan(attempt, h.InvSeq, 1, false)
				cancel()
				var typed *jetstream.APIError
				permanent := mode == "delete_denied" || mode == "store_failure"
				if mode == "missing_success" {
					if !errors.Is(scanErr, natsutil.ErrInvalidDeleteReply) {
						t.Fatalf("missing success was accepted: %v", scanErr)
					}
				} else if mode == "unavailable" || permanent {
					want := jetstream.ErrorCode(10057)
					if mode == "unavailable" {
						want = 10008
					}
					if !errors.As(scanErr, &typed) || typed.ErrorCode != want {
						t.Fatalf("delete reply lost typed API cause: result=%+v err=%v", result, scanErr)
					}
				} else if mode == "lost_reply" {
					if !errors.Is(scanErr, context.DeadlineExceeded) {
						t.Fatalf("lost reply became non-ambiguous: %v", scanErr)
					}
				} else if scanErr != nil || result.Removed != 1 {
					t.Fatalf("already absent delete killed retirement: %+v %v", result, scanErr)
				}
				if deletes.Load() != 1 || sent.Load() != 1 || received.Load() != map[bool]int64{true: 0, false: 1}[mode == "lost_reply"] {
					t.Fatalf("routing/trace mismatch deletes=%d sent=%d received=%d", deletes.Load(), sent.Load(), received.Load())
				}
				if failure := forwardingError.Load(); failure != nil {
					t.Fatal(*failure)
				}
				if _, err := scan.Scan(ctx, h.InvSeq, 1, false); err != nil {
					t.Fatal("later cleanup", err)
				}
				run, err := all[1].Stream(ctx, "WF_RUN")
				if err != nil {
					t.Fatal(err)
				}
				if _, err := run.GetLastMsgForSubject(ctx, "wf.schedule.test."+id+".1"); !errors.Is(err, jetstream.ErrMsgNotFound) {
					t.Fatalf("physical hint retained: %v", err)
				}
			})
		}
	}
}
