package integrity

import (
	"bytes"
	"encoding/json"
	"fmt"
	"js-wf/internal/graphpublication"
	"js-wf/journal"
	"math"
)

// DecodeUnambiguous has already rejected duplicate, unknown and aliased fields.
// Check presence separately: encoding/json accepts null for scalar fields and
// leaves the destination at zero, obscuring malformed persisted metadata.
func auditGraphCursorFields(raw json.RawMessage) error {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		return err
	}
	for _, field := range []string{"schema", "invocation", "base", "count", "epoch", "kind", "retired"} {
		if _, ok := fields[field]; !ok {
			return fmt.Errorf("missing cursor field %s", field)
		}
	}
	for field, value := range fields {
		if bytes.Equal(bytes.TrimSpace(value), []byte("null")) {
			return fmt.Errorf("null cursor field %s", field)
		}
	}
	return nil
}

// Derive capabilities from the persisted version, independently of GraphStore
// configuration/admission. Projection-only retirement does not waive metadata.
func auditGraphCursorShape(c auditedGraphCursor, root graphpublication.Root) error {
	starts, signals, checkpoints, archives := false, false, false, false
	switch c.Schema {
	case "js-wf-graph-journal-cursor-v1":
	case "js-wf-graph-runtime-cursor-v2":
		starts = true
	case "js-wf-graph-runtime-cursor-v4":
		starts, signals = true, true
	case "js-wf-graph-runtime-cursor-v5":
		starts, signals, checkpoints = true, true, true
	case "js-wf-graph-runtime-cursor-v6":
		starts, signals, checkpoints, archives = true, true, true, true
	default:
		return fmt.Errorf("unsupported cursor version")
	}
	expectedRoot := graphpublication.ApplicationSchema
	if starts {
		expectedRoot = graphpublication.StreamsSchema
	}
	if root.Schema != expectedRoot || root.Head == 0 {
		return fmt.Errorf("cursor/authority versions differ")
	}
	if starts {
		if c.Start == nil || c.Invocation != 0 && c.Invocation <= c.PreviousInvocation {
			return fmt.Errorf("invalid start/generation declaration")
		}
	} else if c.Start != nil || c.PreviousInvocation != 0 || c.Invocation == 0 {
		return fmt.Errorf("legacy cursor carries canonical start state")
	}
	if c.Count == 0 {
		if c.Kind != "" || c.Epoch != 0 || c.Retired {
			return fmt.Errorf("empty cursor carries journal state")
		}
	} else {
		switch c.Kind {
		case journal.Started, journal.StepRequested, journal.StepCompleted, journal.Suspended, journal.SignalConsumed, journal.Attempt, journal.Completed, journal.Failed:
		default:
			return fmt.Errorf("unknown cursor kind")
		}
		if c.Count == 1 && c.Kind != journal.Started || c.Count > 1 && c.Kind == journal.Started {
			return fmt.Errorf("cursor kind/count differs")
		}
	}
	if c.Purging && c.Kind != journal.Completed && c.Kind != journal.Failed {
		return fmt.Errorf("purging nonterminal cursor")
	}
	if c.Retired && root.Token != "" {
		return fmt.Errorf("retired cursor retains publication token")
	}
	if starts && c.Invocation == 0 && (c.Count != 0 || c.Epoch != 0 || c.Kind != "" || c.Retired || c.Purging) {
		return fmt.Errorf("pending cursor carries execution state")
	}
	if !signals && (c.SignalInputs != 0 || c.SignalBindings != 0 || c.SignalSource != 0 || c.SignalConsumed != 0 || c.SignalRepair != 0) {
		return fmt.Errorf("version does not support signal state")
	}
	if c.SignalInputs > math.MaxInt64 || c.SignalBindings > c.SignalInputs || c.SignalConsumed > c.SignalBindings || c.SignalRepair != 0 && c.SignalRepair >= c.SignalInputs || c.SignalBindings > 0 && c.SignalSource == 0 {
		return fmt.Errorf("invalid signal cursor bounds/source")
	}
	if c.Invocation == 0 && (c.SignalInputs != 0 || c.SignalBindings != 0 || c.SignalSource != 0 || c.SignalConsumed != 0 || c.SignalRepair != 0) {
		return fmt.Errorf("pending cursor carries signal state")
	}
	if c.RetainedFrom > c.Count || !archives && c.RetainedFrom != 0 || c.RetainedFrom != 0 && len(c.Checkpoint) == 0 {
		return fmt.Errorf("version/archive boundary differs")
	}
	if len(c.Checkpoint) != 0 {
		if !checkpoints {
			return fmt.Errorf("version does not support checkpoint")
		}
		if _, err := auditCheckpointPointer(c); err != nil {
			return err
		}
	}
	input, archive, foundInput, foundArchive := uint64(0), uint64(0), false, false
	for _, stream := range root.Streams {
		switch stream.Name {
		case "input":
			if !starts {
				return fmt.Errorf("legacy input forest")
			}
			input, foundInput = stream.Graph.Count, true
		case "archive":
			if !archives {
				return fmt.Errorf("version does not support archive forest")
			}
			archive, foundArchive = stream.Graph.Count, true
		case "signal-input", "signal-queue":
			if !signals {
				return fmt.Errorf("version does not support signal forest")
			}
		default:
			return fmt.Errorf("unknown cursor forest")
		}
	}
	if starts {
		wantInput, wantArchive := uint64(1), c.RetainedFrom
		if c.Retired {
			wantInput, wantArchive = 0, 0
		}
		if !foundInput || input != wantInput || archive != wantArchive || c.RetainedFrom > 0 && !foundArchive {
			return fmt.Errorf("input/archive forest census differs")
		}
	}
	return nil
}
