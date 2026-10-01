//go:build linux

package integration_test

import (
	"context"
	"errors"
	"fmt"
	"github.com/nats-io/nats.go/jetstream"
	"sort"
	"testing"
	"time"
)

type mixedRunDrainDiagnostic struct {
	CapturedAt     time.Time                 `json:"captured_at"`
	DrainError     string                    `json:"drain_error"`
	LastDrainInfo  *jetstream.StreamInfo     `json:"last_drain_info"`
	Messages       []*jetstream.RawStreamMsg `json:"messages"`
	MessageErrors  map[uint64]string         `json:"message_errors"`
	Consumers      []*jetstream.ConsumerInfo `json:"consumers"`
	ConsumerErrors map[uint32]string         `json:"consumer_errors"`
	Truncated      bool                      `json:"truncated"`
	ContextError   string                    `json:"context_error,omitempty"`
}

// Snapshot reads after a failed gate diagnose it; they never turn that gate
// into success. Bound both sequence scans and calls, and record incomplete reads.
func collectMixedRunDrainDiagnostic(ctx context.Context, js jetstream.JetStream, info *jetstream.StreamInfo, parts map[uint32]bool, drainErr error) mixedRunDrainDiagnostic {
	report := mixedRunDrainDiagnostic{CapturedAt: time.Now().UTC(), LastDrainInfo: info, MessageErrors: map[uint64]string{}, ConsumerErrors: map[uint32]string{}}
	if drainErr != nil {
		report.DrainError = drainErr.Error()
	}
	if info != nil && info.State.Msgs > 0 {
		sequence := info.State.FirstSeq
		for scanned := 0; sequence != 0 && sequence <= info.State.LastSeq && ctx.Err() == nil; scanned++ {
			if scanned == 4096 || len(report.Messages) == 64 {
				report.Truncated = true
				break
			}
			attempt, stop := context.WithTimeout(ctx, time.Second)
			stream, err := js.Stream(attempt, "WF_RUN")
			var message *jetstream.RawStreamMsg
			if err == nil {
				message, err = stream.GetMsg(attempt, sequence)
			}
			stop()
			if err == nil {
				report.Messages = append(report.Messages, message)
			} else if !errors.Is(err, jetstream.ErrMsgNotFound) {
				report.MessageErrors[sequence] = err.Error()
			}
			if sequence == info.State.LastSeq {
				break
			}
			sequence++
		}
	}
	sorted := make([]uint32, 0, len(parts))
	for part := range parts {
		sorted = append(sorted, part)
	}
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	for _, part := range sorted {
		if ctx.Err() != nil {
			report.Truncated = true
			break
		}
		attempt, stop := context.WithTimeout(ctx, time.Second)
		consumer, err := js.Consumer(attempt, "WF_RUN", fmt.Sprintf("WF_P_%02d", part))
		var state *jetstream.ConsumerInfo
		if err == nil {
			state, err = consumer.Info(attempt)
		}
		stop()
		if err != nil {
			report.ConsumerErrors[part] = err.Error()
		} else {
			report.Consumers = append(report.Consumers, state)
		}
	}
	if ctx.Err() != nil {
		report.Truncated = true
		report.ContextError = ctx.Err().Error()
	}
	return report
}

func TestMixedRunDrainDiagnosticRetainsRawMessageAndConsumer(t *testing.T) {
	all, _ := setup(t)
	ctx, stop := context.WithTimeout(context.Background(), 10*time.Second)
	defer stop()
	consumer, err := all[0].CreateOrUpdateConsumer(ctx, "WF_RUN", jetstream.ConsumerConfig{Durable: "WF_P_00", AckPolicy: jetstream.AckExplicitPolicy, FilterSubject: "wf.run.0", AckWait: 13 * time.Second, Replicas: 3})
	if err != nil {
		t.Fatal(err)
	}
	var middle uint64
	for i := 1; i <= 3; i++ {
		ack, err := all[0].Publish(ctx, "wf.run.0", []byte(fmt.Sprintf("test.job-%d", i)), jetstream.WithMsgID(fmt.Sprintf("diagnostic-%d", i)))
		if err != nil {
			t.Fatal(err)
		}
		if i == 2 {
			middle = ack.Sequence
		}
	}
	stream, err := all[0].Stream(ctx, "WF_RUN")
	if err != nil {
		t.Fatal(err)
	}
	if err := stream.DeleteMsg(ctx, middle); err != nil {
		t.Fatal(err)
	}
	batch, err := consumer.Fetch(1, jetstream.FetchMaxWait(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	var delivered uint64
	for message := range batch.Messages() {
		metadata, err := message.Metadata()
		if err != nil {
			t.Fatal(err)
		}
		delivered = metadata.Sequence.Stream
	}
	if batch.Error() != nil || delivered == 0 {
		t.Fatalf("fetch=%v delivered=%d", batch.Error(), delivered)
	}
	info, err := stream.Info(ctx)
	if err != nil {
		t.Fatal(err)
	}
	report := collectMixedRunDrainDiagnostic(ctx, all[1], info, map[uint32]bool{0: true}, context.DeadlineExceeded)
	if report.Truncated || len(report.Messages) != 2 || len(report.MessageErrors) != 0 || len(report.Consumers) != 1 || len(report.ConsumerErrors) != 0 || report.Consumers[0].NumAckPending != 1 || report.Messages[0].Sequence != delivered || string(report.Messages[0].Data) != "test.job-1" || report.Messages[0].Header.Get("Nats-Msg-Id") != "diagnostic-1" || report.DrainError != context.DeadlineExceeded.Error() {
		t.Fatalf("diagnostic=%+v", report)
	}
	canceled, stopCanceled := context.WithCancel(ctx)
	stopCanceled()
	incomplete := collectMixedRunDrainDiagnostic(canceled, all[1], info, map[uint32]bool{0: true}, context.DeadlineExceeded)
	if !incomplete.Truncated || incomplete.ContextError == "" || incomplete.DrainError != report.DrainError {
		t.Fatalf("incomplete=%+v", incomplete)
	}
	t.Logf("raw identity/header preserved across deleted sequence; ack_pending=%d; failed gate unchanged", report.Consumers[0].NumAckPending)
}
