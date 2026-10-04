//go:build linux

package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

type matrixClockPublisher struct {
	jetstream.JetStream
	publish func(context.Context, string, []byte) (*jetstream.PubAck, error)
}

func (p matrixClockPublisher) Publish(ctx context.Context, subject string, data []byte, _ ...jetstream.PublishOpt) (*jetstream.PubAck, error) {
	return p.publish(ctx, subject, data)
}

type matrixClockReader struct {
	jetstream.Stream
	read func(context.Context, uint64) (*jetstream.RawStreamMsg, error)
}

func (r matrixClockReader) GetMsg(ctx context.Context, sequence uint64, _ ...jetstream.GetMsgOpt) (*jetstream.RawStreamMsg, error) {
	return r.read(ctx, sequence)
}

func TestMatrixWorkerClockReplyIdentity(t *testing.T) {
	cases := []struct {
		name   string
		change func(**jetstream.PubAck, **jetstream.RawStreamMsg)
	}{
		{"matching", func(**jetstream.PubAck, **jetstream.RawStreamMsg) {}},
		{"wrong_message_sequence", func(_ **jetstream.PubAck, m **jetstream.RawStreamMsg) { (*m).Sequence++ }},
		{"wrong_message_subject", func(_ **jetstream.PubAck, m **jetstream.RawStreamMsg) { (*m).Subject = "matrix.clock.other" }},
		{"wrong_message_payload", func(_ **jetstream.PubAck, m **jetstream.RawStreamMsg) { (*m).Data = []byte(`{"worker":"other"}`) }},
		{"missing_message_timestamp", func(_ **jetstream.PubAck, m **jetstream.RawStreamMsg) { (*m).Time = time.Time{} }},
		{"missing_message", func(_ **jetstream.PubAck, m **jetstream.RawStreamMsg) { *m = nil }},
		{"wrong_ack_stream", func(a **jetstream.PubAck, _ **jetstream.RawStreamMsg) { (*a).Stream = "OTHER" }},
		{"zero_ack_sequence", func(a **jetstream.PubAck, _ **jetstream.RawStreamMsg) { (*a).Sequence = 0 }},
		{"missing_ack", func(a **jetstream.PubAck, _ **jetstream.RawStreamMsg) { *a = nil }},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			base := filepath.Join(t.TempDir(), "worker")
			prior := []byte("last confirmed sample")
			if err := os.WriteFile(base+"-clock.json", prior, 0600); err != nil {
				t.Fatal(err)
			}
			ctx := context.Background()
			var reply *jetstream.RawStreamMsg
			publisher := matrixClockPublisher{publish: func(got context.Context, subject string, data []byte) (*jetstream.PubAck, error) {
				if got != ctx || subject != "matrix.clock.worker" {
					t.Fatal("publication changed context or worker identity")
				}
				var sample matrixWorkerClockSample
				if err := json.Unmarshal(data, &sample); err != nil || sample.Worker != "worker" {
					t.Fatalf("publication identity: %s, %v", data, err)
				}
				ack := &jetstream.PubAck{Stream: "MATRIX_CLOCK", Sequence: 7}
				reply = &jetstream.RawStreamMsg{Subject: subject, Sequence: 7, Data: append([]byte(nil), data...), Time: sample.WorkerAt}
				test.change(&ack, &reply)
				return ack, nil
			}}
			reader := matrixClockReader{read: func(got context.Context, sequence uint64) (*jetstream.RawStreamMsg, error) {
				if got != ctx || sequence != 7 {
					t.Fatal("read did not use the publication's context and sequence")
				}
				return reply, nil
			}}
			err := writeMatrixWorkerClock(ctx, publisher, reader, base, "worker")
			data, readErr := os.ReadFile(base + "-clock.json")
			if readErr != nil {
				t.Fatal(readErr)
			}
			if test.name != "matching" {
				if err == nil {
					t.Fatal("unrelated or incomplete reply accepted as clock evidence")
				}
				if string(data) != string(prior) {
					t.Fatal("rejected reply replaced the last confirmed sample")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			var sample matrixWorkerClockSample
			if err := json.Unmarshal(data, &sample); err != nil {
				t.Fatal(err)
			}
			if sample.Worker != "worker" || sample.Sequence != 7 || !sample.ServerAt.Equal(reply.Time) || sample.Offset != 0 {
				t.Fatalf("confirmed sample identity changed: %+v", sample)
			}
			if err := validateMatrixWorkerClock(sample, 0, time.Now()); err != nil {
				t.Fatalf("matching reply failed the existing freshness/offset gate: %v", err)
			}
		})
	}
}

func TestMatrixWorkerClockTransportFailurePreservesCause(t *testing.T) {
	for _, phase := range []string{"publish", "read"} {
		t.Run(phase, func(t *testing.T) {
			publisher := matrixClockPublisher{publish: func(context.Context, string, []byte) (*jetstream.PubAck, error) {
				if phase == "publish" {
					return nil, context.DeadlineExceeded
				}
				return &jetstream.PubAck{Stream: "MATRIX_CLOCK", Sequence: 7}, nil
			}}
			reader := matrixClockReader{read: func(context.Context, uint64) (*jetstream.RawStreamMsg, error) {
				return nil, context.DeadlineExceeded
			}}
			base := filepath.Join(t.TempDir(), "worker")
			err := writeMatrixWorkerClock(context.Background(), publisher, reader, base, "worker")
			if !errors.Is(err, context.DeadlineExceeded) || !strings.Contains(err.Error(), phase+" clock probe") {
				t.Fatalf("transport failure lost phase or cause: %v", err)
			}
			if _, err := os.Stat(base + "-clock.json"); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("failed transport published clock evidence: %v", err)
			}
		})
	}
}
