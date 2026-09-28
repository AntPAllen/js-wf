package provision

import (
	"context"
	"fmt"

	"github.com/nats-io/nats.go/jetstream"
)

// JournalCapacity is a point-in-time view of the configured WF_JRN byte cap.
// Poll it and alert whenever Alert is true; writing can still reach the cap
// between polls.
type JournalCapacity struct {
	UsedBytes   uint64  `json:"used_bytes"`
	LimitBytes  int64   `json:"limit_bytes"`
	Utilization float64 `json:"utilization"`
	Alert       bool    `json:"alert"`
}

// CheckJournalCapacity reports an alert at 70% of a configured byte cap.
// An unbounded stream has no denominator and cannot be monitored this way.
func CheckJournalCapacity(ctx context.Context, js jetstream.JetStream) (JournalCapacity, error) {
	stream, err := js.Stream(ctx, "WF_JRN")
	if err != nil {
		return JournalCapacity{}, err
	}
	info, err := stream.Info(ctx)
	if err != nil {
		return JournalCapacity{}, err
	}
	if info.Config.MaxBytes <= 0 {
		return JournalCapacity{}, fmt.Errorf("WF_JRN has no byte cap; configure one to monitor 70%% usage")
	}
	used := info.State.Bytes
	limit := info.Config.MaxBytes
	utilization := float64(used) / float64(limit)
	return JournalCapacity{UsedBytes: used, LimitBytes: limit, Utilization: utilization, Alert: utilization >= 0.70}, nil
}
