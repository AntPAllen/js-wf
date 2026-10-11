package integrity

import (
	"fmt"
	"js-wf/internal/checkpoint"
	"js-wf/journal"
	"time"
)

// This independently decodes the purge marker only after the retired cursor
// declares Purging and the invocation source is absent. It does not establish
// wall-clock expiry, the erased terminal result or the historical purge order.
func auditRetiredTombstone(raw []byte, invocation uint64) error {
	var marker struct {
		Tombstone  bool      `json:"tombstone"`
		Invocation uint64    `json:"inv_seq"`
		PurgedAt   time.Time `json:"purged_at"`
		ExpiresAt  time.Time `json:"expires_at"`
	}
	if err := checkpoint.DecodeUnambiguous(raw, &marker); err != nil {
		return err
	}
	if !marker.Tombstone || invocation == 0 || marker.Invocation != invocation || marker.PurgedAt.IsZero() || !marker.ExpiresAt.After(marker.PurgedAt) {
		return fmt.Errorf("invalid retired tombstone generation/marker/times")
	}
	return nil
}

// Forests have been retired. This admits the remaining projection, without
// claiming result ownership or rejected-operation provenance from lost history.
func auditRetiredProjection(raw []byte, invocation uint64, kind journal.Kind) error {
	if err := auditOutcomeFields(raw); err != nil {
		return err
	}
	var o auditedGraphOutcome
	if err := checkpoint.DecodeUnambiguous(raw, &o); err != nil {
		return err
	}
	if o.InvSeq != invocation || (kind == journal.Failed) != (o.Error != "") {
		return fmt.Errorf("generation/kind differs")
	}
	if kind == journal.Failed && (len(o.Result) != 0 || o.ResultRef != "" || o.ResultHash != "") {
		return fmt.Errorf("failed projection carries a result")
	}
	if o.ResultRef != "" {
		if len(o.Result) != 0 || !graphAuditHash(o.ResultHash) {
			return fmt.Errorf("invalid result descriptor")
		}
	} else if o.ResultHash != "" {
		return fmt.Errorf("result hash without pointer")
	}
	if o.LimitRequest != nil || o.LimitEntry != nil {
		if kind != journal.Failed || o.Error != journal.ErrTooLong.Error() || o.LimitRequest != nil && o.LimitEntry != nil {
			return fmt.Errorf("invalid rejected-limit projection")
		}
	}
	return nil
}
