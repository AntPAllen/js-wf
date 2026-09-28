package retention

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"js-wf/identity"
	"js-wf/journal"

	"github.com/nats-io/nats.go/jetstream"
)

type BlobSweepResult struct {
	Objects    int `json:"objects"`
	Referenced int `json:"referenced"`
	Eligible   int `json:"eligible"`
	Deleted    int `json:"deleted"`
}

// SweepBlobsQuiescent reclaims unreferenced runtime objects. All workflow
// writers and clients must be stopped for the entire call: Object Store Delete
// has no compare-and-delete condition, so a concurrent upload or publication
// could otherwise race the mark phase. minAge also protects recent orphans.
func SweepBlobsQuiescent(ctx context.Context, js jetstream.JetStream, minAge time.Duration, now time.Time) (BlobSweepResult, error) {
	if minAge < 0 || now.IsZero() {
		return BlobSweepResult{}, fmt.Errorf("invalid blob sweep age or clock")
	}
	objects, err := js.ObjectStore(ctx, "WF_BLOB")
	if err != nil {
		return BlobSweepResult{}, err
	}
	references := map[string]struct{}{}
	mark := func(name string) {
		if name != "" {
			references[name] = struct{}{}
		}
	}
	for _, streamName := range []string{"WF_INV", "WF_SIG", "WF_JRN"} {
		stream, err := js.Stream(ctx, streamName)
		if err != nil {
			return BlobSweepResult{}, err
		}
		info, err := stream.Info(ctx)
		if err != nil {
			return BlobSweepResult{}, err
		}
		for seq := info.State.FirstSeq; seq != 0 && seq <= info.State.LastSeq; seq++ {
			message, err := stream.GetMsg(ctx, seq)
			if errors.Is(err, jetstream.ErrMsgNotFound) {
				continue
			}
			if err != nil {
				return BlobSweepResult{}, err
			}
			switch streamName {
			case "WF_INV":
				mark(message.Header.Get("Wf-Input-Ref"))
			case "WF_SIG":
				mark(message.Header.Get("Wf-Signal-Ref"))
			case "WF_JRN":
				var entry journal.Entry
				if err := json.Unmarshal(message.Data, &entry); err != nil {
					return BlobSweepResult{}, fmt.Errorf("journal sequence %d: %w", seq, err)
				}
				if err := markEntryRefs(entry, mark); err != nil {
					return BlobSweepResult{}, fmt.Errorf("journal sequence %d: %w", seq, err)
				}
			}
		}
	}
	state, err := js.KeyValue(ctx, "WF_STATE")
	if err != nil {
		return BlobSweepResult{}, err
	}
	keys, err := state.Keys(ctx)
	if err != nil && !errors.Is(err, jetstream.ErrNoKeysFound) {
		return BlobSweepResult{}, err
	}
	for _, key := range keys {
		value, err := state.Get(ctx, key)
		if errors.Is(err, jetstream.ErrKeyNotFound) {
			continue
		}
		if err != nil {
			return BlobSweepResult{}, err
		}
		if strings.HasPrefix(key, "snap.") {
			parts := strings.Split(strings.TrimPrefix(key, "snap."), ".")
			if len(parts) != 2 || identity.Validate(parts[0], parts[1]) != nil {
				return BlobSweepResult{}, fmt.Errorf("invalid snapshot key %q", key)
			}
			var snap journal.Snapshot
			if err := json.Unmarshal(value.Value(), &snap); err != nil || snap.Object == "" || snap.SHA256 == "" {
				return BlobSweepResult{}, fmt.Errorf("invalid snapshot manifest %q", key)
			}
			mark(snap.Object)
			data, err := objects.GetBytes(ctx, snap.Object)
			if err != nil {
				return BlobSweepResult{}, fmt.Errorf("snapshot object %q: %w", snap.Object, err)
			}
			digest := sha256.Sum256(data)
			if hex.EncodeToString(digest[:]) != snap.SHA256 {
				return BlobSweepResult{}, fmt.Errorf("snapshot object %q hash mismatch", snap.Object)
			}
			var records []journal.Record
			if err := json.Unmarshal(data, &records); err != nil {
				return BlobSweepResult{}, fmt.Errorf("snapshot object %q: %w", snap.Object, err)
			}
			for _, record := range records {
				if err := markEntryRefs(record.Entry, mark); err != nil {
					return BlobSweepResult{}, fmt.Errorf("snapshot object %q: %w", snap.Object, err)
				}
			}
			continue
		}
		parts := strings.Split(key, ".")
		if len(parts) != 2 || identity.Validate(parts[0], parts[1]) != nil {
			continue
		}
		raw := bytes.TrimSpace(value.Value())
		if len(raw) == 0 || raw[0] != '{' {
			continue // e.g. a reconciler cursor
		}
		var stateValue struct {
			ResultRef string `json:"result_ref"`
		}
		if err := json.Unmarshal(raw, &stateValue); err != nil {
			return BlobSweepResult{}, err
		}
		mark(stateValue.ResultRef)
	}
	all, err := objects.List(ctx)
	if errors.Is(err, jetstream.ErrNoObjectsFound) {
		return BlobSweepResult{Referenced: len(references)}, nil
	}
	if err != nil {
		return BlobSweepResult{}, err
	}
	result := BlobSweepResult{Objects: len(all), Referenced: len(references)}
	for _, object := range all {
		if object == nil || !runtimeBlobName(object.Name) || object.ModTime.IsZero() || now.Before(object.ModTime.Add(minAge)) {
			continue
		}
		if _, ok := references[object.Name]; ok {
			continue
		}
		result.Eligible++
		if err := objects.Delete(ctx, object.Name); err != nil {
			return result, fmt.Errorf("delete object %q: %w", object.Name, err)
		}
		result.Deleted++
	}
	return result, nil
}

func runtimeBlobName(name string) bool {
	for _, prefix := range []string{"input-", "signal-", "step-result-", "terminal-result-", "snapshot-"} {
		if strings.HasPrefix(name, prefix) {
			return true
		}
	}
	return false
}

func markEntryRefs(entry journal.Entry, mark func(string)) error {
	if len(entry.Payload) == 0 {
		return nil
	}
	switch entry.Kind {
	case journal.StepCompleted, journal.SignalConsumed, journal.Completed, journal.Failed:
		var payload struct {
			ResultRef string `json:"result_ref"`
			Ref       string `json:"ref"`
		}
		if err := json.Unmarshal(entry.Payload, &payload); err != nil {
			return err
		}
		mark(payload.ResultRef)
		mark(payload.Ref)
	}
	return nil
}
