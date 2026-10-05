package integrity

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

func TestCallbackR1ReplayRequiresActualPositionLoss(t *testing.T) {
	for _, kind := range []string{"position-loss", "at-accepted", "ahead", "zero-position", "ack-mismatch", "pending-ack", "redelivered", "other-name", "other-stream", "other-created", "other-leader", "other-config", "disk", "replicated", "explicit-ack", "unknown-initial", "no-accepted", "api-error"} {
		t.Run(kind, func(t *testing.T) {
			initial := &jetstream.ConsumerInfo{Name: "audit", Stream: "WF_JRN", Created: time.Unix(1, 0), Config: jetstream.ConsumerConfig{MemoryStorage: true, Replicas: 1, AckPolicy: jetstream.AckNonePolicy, OptStartSeq: 1}, Cluster: &jetstream.ClusterInfo{Leader: "owner"}}
			current := *initial
			current.Delivered = jetstream.SequenceInfo{Consumer: 1126, Stream: 1126}
			current.AckFloor = current.Delivered
			accepted := uint64(1961)
			c := &byteLeaderInfoConsumer{info: &current}
			switch kind {
			case "at-accepted":
				current.Delivered.Stream = accepted
				current.AckFloor = current.Delivered
			case "ahead":
				current.Delivered.Stream = accepted + 1
				current.AckFloor = current.Delivered
			case "zero-position":
				current.Delivered = jetstream.SequenceInfo{}
				current.AckFloor = current.Delivered
			case "ack-mismatch":
				current.AckFloor.Consumer--
			case "pending-ack":
				current.NumAckPending = 1
			case "redelivered":
				current.NumRedelivered = 1
			case "other-name":
				current.Name = "other"
			case "other-stream":
				current.Stream = "other"
			case "other-created":
				current.Created = current.Created.Add(time.Second)
			case "other-leader":
				current.Cluster = &jetstream.ClusterInfo{Leader: "other"}
			case "other-config":
				current.Config.OptStartSeq++
			case "disk":
				initial.Config.MemoryStorage = false
				current.Config = initial.Config
			case "replicated":
				initial.Config.Replicas = 3
				current.Config = initial.Config
			case "explicit-ack":
				initial.Config.AckPolicy = jetstream.AckExplicitPolicy
				current.Config = initial.Config
			case "unknown-initial":
				initial.Cluster = nil
			case "no-accepted":
				accepted = 0
			case "api-error":
				c.err = errors.New("consumer info semantic failure")
			}
			confirmed, err := confirmedCallbackConsumerPositionLoss(context.Background(), c, initial, accepted)
			if confirmed != (kind == "position-loss") || err != c.err {
				t.Fatalf("confirmed=%v err=%v", confirmed, err)
			}
		})
	}
}
