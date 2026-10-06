//go:build linux

package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/nats-io/nats.go/jetstream"
)

type bulkFaultControlMsg struct {
	jetstream.Msg
	sequence uint64
}

func (m *bulkFaultControlMsg) Metadata() (*jetstream.MsgMetadata, error) {
	return &jetstream.MsgMetadata{Sequence: jetstream.SequencePair{Stream: m.sequence}}, nil
}

type bulkFaultControlConsumer struct {
	jetstream.Consumer
	messages []jetstream.Msg
	info     *jetstream.ConsumerInfo
	infoErr  error
	queries  int
}

func (c *bulkFaultControlConsumer) Info(context.Context) (*jetstream.ConsumerInfo, error) {
	c.queries++
	return c.info, c.infoErr
}

func (c *bulkFaultControlConsumer) Consume(handler jetstream.MessageHandler, _ ...jetstream.PullConsumeOpt) (jetstream.ConsumeContext, error) {
	for _, msg := range c.messages {
		handler(msg)
	}
	return nil, nil
}

type bulkFaultControlStream struct {
	jetstream.Stream
	consumer jetstream.Consumer
}

func (s bulkFaultControlStream) CreateConsumer(context.Context, jetstream.ConsumerConfig) (jetstream.Consumer, error) {
	return s.consumer, nil
}

func TestMatrixBulkFaultRejectsUnprovenTargetWithoutChangingDelivery(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	fault := &matrixBulkCursorFault{ctx: ctx, cancel: cancel}
	// Invalid public metadata must abort before any server operation. Multiple
	// delivered messages must neither retry the destructive action nor mutate,
	// duplicate, omit or reorder the underlying handler's input.
	consumer := &bulkFaultControlConsumer{info: &jetstream.ConsumerInfo{Stream: "WF_JRN", Config: jetstream.ConsumerConfig{Replicas: 5}}}
	for _, sequence := range []uint64{127, 128, 129, 130} {
		consumer.messages = append(consumer.messages, &bulkFaultControlMsg{sequence: sequence})
	}
	wrapped := matrixBulkFaultConsumer{Consumer: consumer, fault: fault}
	var delivered []jetstream.Msg
	_, err := wrapped.Consume(func(msg jetstream.Msg) { delivered = append(delivered, msg) })
	if err != nil || !reflect.DeepEqual(delivered, consumer.messages) || consumer.queries != 1 {
		t.Fatalf("fault adapter changed delivery or retried injection: %v queries=%d", err, consumer.queries)
	}
	proof := fault.snapshot()
	if ctx.Err() == nil || proof.Error == "" || proof.Target != nil || !proof.Kill.SourceStopped.IsZero() {
		t.Fatal("unproven target was accepted or stage remained active")
	}
}

func TestMatrixBulkFaultMetadataFailureLeavesActualCursorForCleanup(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	failure := errors.New("public metadata unavailable")
	actual := &bulkFaultControlConsumer{infoErr: failure}
	fault := &matrixBulkCursorFault{ctx: ctx, cancel: cancel}
	stream := matrixBulkFaultStream{Stream: bulkFaultControlStream{consumer: actual}, fault: fault}
	consumer, err := stream.CreateConsumer(ctx, jetstream.ConsumerConfig{})
	if err != nil || consumer != actual || ctx.Err() == nil || fault.snapshot().Error != failure.Error() {
		t.Fatal("metadata failure hid the actual cursor from normal scanner cleanup")
	}
}

type bulkPreparationControlList struct {
	info chan *jetstream.ConsumerInfo
}

func (l bulkPreparationControlList) Info() <-chan *jetstream.ConsumerInfo { return l.info }
func (l bulkPreparationControlList) Err() error                           { return nil }

type bulkPreparationControlStream struct {
	jetstream.Stream
	listed        []*jetstream.ConsumerInfo
	list          bulkPreparationControlList
	deleteErr     error
	postConsumers int
	deletions     int
}

func (s *bulkPreparationControlStream) ListConsumers(context.Context) jetstream.ConsumerInfoLister {
	s.list.info = make(chan *jetstream.ConsumerInfo, len(s.listed))
	for _, info := range s.listed {
		s.list.info <- info
	}
	close(s.list.info)
	return s.list
}

func (s *bulkPreparationControlStream) DeleteConsumer(context.Context, string) error {
	if len(s.list.info) != 0 {
		return errors.New("deletion during unfinished inventory")
	}
	s.deletions++
	return s.deleteErr
}

func (s *bulkPreparationControlStream) Info(context.Context, ...jetstream.StreamInfoOpt) (*jetstream.StreamInfo, error) {
	return &jetstream.StreamInfo{State: jetstream.StreamState{Consumers: s.postConsumers}}, nil
}

type bulkPreparationControlJS struct {
	jetstream.JetStream
	journal *bulkPreparationControlStream
}

func (js bulkPreparationControlJS) Stream(_ context.Context, name string) (jetstream.Stream, error) {
	if name == "WF_JRN" {
		return js.journal, nil
	}
	return &bulkPreparationControlStream{}, nil
}

func TestMatrixBulkPreparationInventoryAndAbsenceControls(t *testing.T) {
	for _, tc := range []struct {
		name          string
		duplicate     bool
		deleteErr     error
		postConsumers int
		durable       string
		wantError     bool
		wantDeletions int
	}{
		{name: "duplicate-listing", duplicate: true, wantDeletions: 1},
		{name: "expired-and-zero", deleteErr: jetstream.ErrConsumerNotFound, wantDeletions: 1},
		{name: "absence-with-consumer-left", deleteErr: jetstream.ErrConsumerNotFound, postConsumers: 1, wantError: true, wantDeletions: 1},
		{name: "unavailable-delete", deleteErr: errors.New("unavailable"), wantError: true, wantDeletions: 1},
		{name: "durable-must-remain", durable: "must-retain", wantError: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			info := &jetstream.ConsumerInfo{Stream: "WF_JRN", Name: "wf-audit-old", Config: jetstream.ConsumerConfig{Durable: tc.durable, MemoryStorage: true, AckPolicy: jetstream.AckNonePolicy}}
			stream := &bulkPreparationControlStream{listed: []*jetstream.ConsumerInfo{info}, deleteErr: tc.deleteErr, postConsumers: tc.postConsumers}
			if tc.duplicate {
				stream.listed = append(stream.listed, info)
			}
			root := t.TempDir()
			err := matrixPrepareCopiedBulkCursors(t.Context(), bulkPreparationControlJS{journal: stream}, root)
			if (err != nil) != tc.wantError || stream.deletions != tc.wantDeletions {
				t.Fatalf("preparation verdict/deletions differ: %v deletions=%d", err, stream.deletions)
			}
			var proof struct {
				Listed   []*jetstream.ConsumerInfo `json:"listed"`
				Receipts []struct {
					AlreadyAbsent bool   `json:"already_absent"`
					Error         string `json:"error"`
				} `json:"receipts"`
				Error string `json:"error"`
			}
			data, readErr := os.ReadFile(filepath.Join(root, "bulk-cursor-preparation.json"))
			if readErr != nil || json.Unmarshal(data, &proof) != nil || len(proof.Listed) != len(stream.listed) || len(proof.Receipts) != tc.wantDeletions || (proof.Error != "<nil>") != tc.wantError {
				t.Fatal("preparation evidence omitted listing, actions or failure")
			}
			if errors.Is(tc.deleteErr, jetstream.ErrConsumerNotFound) && (!proof.Receipts[0].AlreadyAbsent || proof.Receipts[0].Error != jetstream.ErrConsumerNotFound.Error()) {
				t.Fatal("absence response was relabeled as a successful deletion")
			}
		})
	}
}
