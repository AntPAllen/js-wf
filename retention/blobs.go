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

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

type BlobSweepResult struct {
	Objects    int `json:"objects"`
	Referenced int `json:"referenced"`
	Eligible   int `json:"eligible"`
	Deleted    int `json:"deleted"`
}

type BlobSweepMessage struct {
	Header nats.Header
	Data   []byte
}

type BlobSweepObject struct {
	Name    string
	ModTime time.Time
}

// BlobSweepPort contains only retained reads and object operations needed by
// the production quiescent mark-and-sweep pass.
type BlobSweepPort interface {
	StreamRange(context.Context, string) (uint64, uint64, error)
	StreamMessage(context.Context, string, uint64) (BlobSweepMessage, error)
	StateKeys(context.Context) ([]string, error)
	StateValue(context.Context, string) ([]byte, error)
	ObjectBytes(context.Context, string) ([]byte, error)
	Objects(context.Context) ([]BlobSweepObject, error)
	DeleteObject(context.Context, string) error
}

type jetStreamBlobSweepPort struct {
	js      jetstream.JetStream
	objects jetstream.ObjectStore
	streams map[string]jetstream.Stream
	state   jetstream.KeyValue
}

func (p *jetStreamBlobSweepPort) stream(ctx context.Context, name string) (jetstream.Stream, error) {
	if stream := p.streams[name]; stream != nil {
		return stream, nil
	}
	stream, err := p.js.Stream(ctx, name)
	if err == nil {
		p.streams[name] = stream
	}
	return stream, err
}

func (p *jetStreamBlobSweepPort) StreamRange(ctx context.Context, name string) (uint64, uint64, error) {
	stream, err := p.stream(ctx, name)
	if err != nil {
		return 0, 0, err
	}
	info, err := stream.Info(ctx)
	if err != nil {
		return 0, 0, err
	}
	return info.State.FirstSeq, info.State.LastSeq, nil
}

func (p *jetStreamBlobSweepPort) StreamMessage(ctx context.Context, name string, sequence uint64) (BlobSweepMessage, error) {
	stream, err := p.stream(ctx, name)
	if err != nil {
		return BlobSweepMessage{}, err
	}
	message, err := stream.GetMsg(ctx, sequence)
	if err != nil {
		return BlobSweepMessage{}, err
	}
	return BlobSweepMessage{Header: message.Header, Data: message.Data}, nil
}

func (p *jetStreamBlobSweepPort) stateKV(ctx context.Context) (jetstream.KeyValue, error) {
	if p.state != nil {
		return p.state, nil
	}
	state, err := p.js.KeyValue(ctx, "WF_STATE")
	if err == nil {
		p.state = state
	}
	return state, err
}

func (p *jetStreamBlobSweepPort) StateKeys(ctx context.Context) ([]string, error) {
	state, err := p.stateKV(ctx)
	if err != nil {
		return nil, err
	}
	return state.Keys(ctx)
}

func (p *jetStreamBlobSweepPort) StateValue(ctx context.Context, key string) ([]byte, error) {
	state, err := p.stateKV(ctx)
	if err != nil {
		return nil, err
	}
	entry, err := state.Get(ctx, key)
	if err != nil {
		return nil, err
	}
	return entry.Value(), nil
}

func (p *jetStreamBlobSweepPort) ObjectBytes(ctx context.Context, name string) ([]byte, error) {
	return p.objects.GetBytes(ctx, name)
}

func (p *jetStreamBlobSweepPort) Objects(ctx context.Context) ([]BlobSweepObject, error) {
	all, err := p.objects.List(ctx)
	if err != nil {
		return nil, err
	}
	objects := make([]BlobSweepObject, 0, len(all))
	for _, object := range all {
		if object == nil {
			objects = append(objects, BlobSweepObject{})
			continue
		}
		objects = append(objects, BlobSweepObject{Name: object.Name, ModTime: object.ModTime})
	}
	return objects, nil
}

func (p *jetStreamBlobSweepPort) DeleteObject(ctx context.Context, name string) error {
	return p.objects.Delete(ctx, name)
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
	return SweepBlobsQuiescentWithPort(ctx, &jetStreamBlobSweepPort{js: js, objects: objects, streams: map[string]jetstream.Stream{}}, minAge, now)
}

// SweepBlobsQuiescentWithPort runs the production mark-and-sweep logic over a
// supplied transport. Callers must keep all writers quiescent for the pass.
func SweepBlobsQuiescentWithPort(ctx context.Context, port BlobSweepPort, minAge time.Duration, now time.Time) (BlobSweepResult, error) {
	if port == nil || minAge < 0 || now.IsZero() {
		return BlobSweepResult{}, fmt.Errorf("invalid blob sweep port, age, or clock")
	}
	references := map[string]struct{}{}
	mark := func(name string) {
		if name != "" {
			references[name] = struct{}{}
		}
	}
	for _, streamName := range []string{"WF_INV", "WF_SIG", "WF_JRN"} {
		first, last, err := port.StreamRange(ctx, streamName)
		if err != nil {
			return BlobSweepResult{}, err
		}
		for seq := first; seq != 0 && seq <= last; seq++ {
			message, err := port.StreamMessage(ctx, streamName, seq)
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
	keys, err := port.StateKeys(ctx)
	if err != nil && !errors.Is(err, jetstream.ErrNoKeysFound) {
		return BlobSweepResult{}, err
	}
	for _, key := range keys {
		value, err := port.StateValue(ctx, key)
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
			if err := json.Unmarshal(value, &snap); err != nil || snap.Object == "" || snap.SHA256 == "" {
				return BlobSweepResult{}, fmt.Errorf("invalid snapshot manifest %q", key)
			}
			mark(snap.Object)
			data, err := port.ObjectBytes(ctx, snap.Object)
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
		raw := bytes.TrimSpace(value)
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
	all, err := port.Objects(ctx)
	if errors.Is(err, jetstream.ErrNoObjectsFound) {
		return BlobSweepResult{Referenced: len(references)}, nil
	}
	if err != nil {
		return BlobSweepResult{}, err
	}
	result := BlobSweepResult{Objects: len(all), Referenced: len(references)}
	for _, object := range all {
		if !runtimeBlobName(object.Name) || object.ModTime.IsZero() || now.Before(object.ModTime.Add(minAge)) {
			continue
		}
		if _, ok := references[object.Name]; ok {
			continue
		}
		result.Eligible++
		if err := port.DeleteObject(ctx, object.Name); err != nil {
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
