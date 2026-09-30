//go:build linux

package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

func captureMatrixQueueDiagnostics(t *testing.T, run jetstream.Stream) {
	t.Helper()
	ctx, done := context.WithTimeout(context.Background(), 10*time.Second)
	defer done()
	attempt, stop := context.WithTimeout(ctx, 2*time.Second)
	info, err := run.Info(attempt)
	stop()
	t.Logf("fresh queue metadata=%+v err=%v", info, err)
	diagnostic := struct {
		Stream              *jetstream.StreamInfo     `json:"stream,omitempty"`
		Consumers           []*jetstream.ConsumerInfo `json:"consumers"`
		Messages            []*jetstream.RawStreamMsg `json:"messages,omitempty"`
		MessageScanComplete bool                      `json:"message_scan_complete"`
		LastScannedSequence uint64                    `json:"last_scanned_sequence,omitempty"`
		MessageErrors       []string                  `json:"message_errors,omitempty"`
		StreamError         string                    `json:"stream_error,omitempty"`
		ConsumerError       string                    `json:"consumer_error,omitempty"`
	}{Stream: info}
	if err != nil {
		diagnostic.StreamError = err.Error()
	}
	consumers := run.ListConsumers(ctx)
	for consumer := range consumers.Info() {
		diagnostic.Consumers = append(diagnostic.Consumers, consumer)
		if consumer.NumPending != 0 || consumer.NumAckPending != 0 {
			t.Logf("queue consumer=%s pending=%d ack_pending=%d delivered=%+v ack_floor=%+v cluster=%+v", consumer.Name, consumer.NumPending, consumer.NumAckPending, consumer.Delivered, consumer.AckFloor, consumer.Cluster)
		}
	}
	if err := consumers.Err(); err != nil {
		diagnostic.ConsumerError = err.Error()
		t.Logf("queue consumers: %v", err)
	}
	if info != nil && info.State.Msgs > 0 {
		rawCtx, stopRaw := context.WithTimeout(context.Background(), 5*time.Second)
		defer stopRaw()
		for seq, reads := info.State.FirstSeq, 0; seq <= info.State.LastSeq && reads < 2000 && rawCtx.Err() == nil; seq, reads = seq+1, reads+1 {
			attempt, stop := context.WithTimeout(rawCtx, 500*time.Millisecond)
			message, err := run.GetMsg(attempt, seq)
			stop()
			diagnostic.LastScannedSequence = seq
			if err == nil {
				diagnostic.Messages = append(diagnostic.Messages, message)
			} else if !errors.Is(err, jetstream.ErrMsgNotFound) {
				diagnostic.MessageErrors = append(diagnostic.MessageErrors, fmt.Sprintf("seq=%d: %v", seq, err))
			}
		}
		diagnostic.MessageScanComplete = diagnostic.LastScannedSequence == info.State.LastSeq && len(diagnostic.MessageErrors) == 0
		t.Logf("queue raw retained messages=%d reported=%d errors=%v", len(diagnostic.Messages), info.State.Msgs, diagnostic.MessageErrors)
	}
	if prefix := os.Getenv("MATRIX_ARTIFACT_PREFIX"); prefix != "" {
		data, err := json.MarshalIndent(diagnostic, "", "  ")
		if err == nil {
			err = os.WriteFile(prefix+"-queue.json", append(data, '\n'), 0644)
		}
		if err != nil {
			t.Errorf("queue diagnostic artifact: %v", err)
		}
	}
}
