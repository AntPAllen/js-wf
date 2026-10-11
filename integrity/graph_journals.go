package integrity

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"

	"github.com/nats-io/nats.go/jetstream"
	"js-wf/identity"
	"js-wf/journal"
)

// GraphJournalSnapshot extends physical reference auditing with raw WF_INV
// sources and terminal projection reads. All sources must belong to this one
// isolated graph namespace; callers cannot silently omit an invocation cohort.
type GraphJournalSnapshot struct {
	Graph          GraphReferenceSnapshot
	Invocations    map[string]*jetstream.RawStreamMsg // keys are type.id
	ReadProjection func(context.Context, string) ([]byte, error)
}

type GraphJournalReport struct {
	References                                                 GraphReferenceReport
	Invocations, Journals, Entries, Terminal, Pending, Retired int
}

// This decoder is independent of GraphStore's cursor validation and traversal.
type auditedGraphCursor struct {
	Schema             string              `json:"schema"`
	Invocation         uint64              `json:"invocation"`
	Base               uint64              `json:"base"`
	Count              uint64              `json:"count"`
	Epoch              uint64              `json:"epoch"`
	Kind               journal.Kind        `json:"kind"`
	Retired            bool                `json:"retired"`
	Purging            bool                `json:"purging,omitempty"`
	Start              *journal.GraphStart `json:"start,omitempty"`
	PreviousInvocation uint64              `json:"previous_invocation,omitempty"`
	SignalInputs       uint64              `json:"signal_inputs,omitempty"`
	SignalBindings     uint64              `json:"signal_bindings,omitempty"`
	SignalSource       uint64              `json:"signal_source,omitempty"`
	SignalConsumed     uint64              `json:"signal_consumed,omitempty"`
	SignalRepair       uint64              `json:"signal_repair,omitempty"`
	RetainedFrom       uint64              `json:"retained_from,omitempty"`
	Checkpoint         json.RawMessage     `json:"checkpoint,omitempty"`
}

type auditedGraphJournal struct {
	cursor            auditedGraphCursor
	key               string
	journal           journalAudit
	inputSeen         bool
	checkpoint        *auditedCheckpointPointer
	checkpointRequest journal.Record
	checkpointSeen    bool
	sdkPosition       uint64
	lastStepRequest   uint64
}

func graphDestinationForKey(key string) string { return "journal/" + digest([]byte("wf.jrn."+key)) }

// CheckGraphJournals verifies current canonical generations, complete logical
// journal order (including archived prefixes), epoch/worker consistency, request
// and completion ordering, terminal generation/result edges and exact terminal
// projection bytes. It shares only entry serialization and the independent
// retained-journal accumulator; no production graph/journal reader is called.
// Checkpoint pointer, owned frame identity/anchor/stage/locals and SDK position
// are independently bound to the declared request and completion. Protected
// reader snapshots receive reference auditing, not application replay.
// Materialized frame state semantics, signal binding/index semantics, orphan projections,
// lease history, client linearizability and I4/I5 remain separate audits.
func CheckGraphJournals(ctx context.Context, snapshot GraphJournalSnapshot) (report GraphJournalReport, err error) {
	if snapshot.Invocations == nil || snapshot.ReadProjection == nil {
		return report, fmt.Errorf("missing graph journal source/projection reader")
	}
	sources := map[string]*jetstream.RawStreamMsg{}
	keys := map[string]string{}
	for key, source := range snapshot.Invocations {
		parts := strings.Split(key, ".")
		if len(parts) != 2 || identity.Validate(parts[0], parts[1]) != nil || source == nil || source.Sequence == 0 || source.Subject != "wf.inv."+key {
			return report, fmt.Errorf("invalid graph invocation source")
		}
		destination := graphDestinationForKey(key)
		sources[destination] = source
		keys[destination] = key
	}
	report.Invocations = len(sources)
	states := map[string]*auditedGraphJournal{}
	consumed := map[string]bool{}
	for destination, root := range snapshot.Graph.Roots {
		var c auditedGraphCursor
		if err := json.Unmarshal(root.Application, &c); err != nil {
			return report, fmt.Errorf("%s: invalid canonical cursor: %w", destination, err)
		}
		switch c.Schema {
		case "js-wf-graph-journal-cursor-v1", "js-wf-graph-runtime-cursor-v2", "js-wf-graph-runtime-cursor-v4", "js-wf-graph-runtime-cursor-v5", "js-wf-graph-runtime-cursor-v6":
		default:
			return report, fmt.Errorf("%s: unsupported canonical cursor schema", destination)
		}
		if !strings.HasPrefix(destination, "journal/") || !graphAuditHash(strings.TrimPrefix(destination, "journal/")) || c.Count > journal.MaxEntries || c.Base > math.MaxUint64-c.Count || c.RetainedFrom > c.Count || c.SignalBindings > c.SignalInputs || c.SignalConsumed > c.SignalBindings {
			return report, fmt.Errorf("%s: invalid canonical cursor bounds", destination)
		}
		counts := map[string]uint64{}
		for _, stream := range root.Streams {
			counts[stream.Name] = stream.Graph.Count
		}
		key := keys[destination]
		if c.Start != nil {
			start := c.Start
			key = identity.Key(start.Request.Type, start.Request.ID)
			if start.Schema != "js-wf-canonical-start-v1" || identity.Validate(start.Request.Type, start.Request.ID) != nil || graphDestinationForKey(key) != destination || !graphAuditID(start.Token) || !graphAuditHash(start.InputSHA256) || start.InputSize < 0 || start.InputSize > snapshot.Graph.PayloadLimit {
				return report, fmt.Errorf("%s: invalid canonical start identity", destination)
			}
		} else if c.Schema != "js-wf-graph-journal-cursor-v1" {
			return report, fmt.Errorf("%s: canonical input descriptor missing", destination)
		}
		if c.Retired {
			if !c.Purging || c.Invocation == 0 || c.Count == 0 || c.Kind != journal.Completed && c.Kind != journal.Failed || root.Graph.Count != 0 {
				return report, fmt.Errorf("%s: invalid retired journal", destination)
			}
			for _, n := range counts {
				if n != 0 {
					return report, fmt.Errorf("%s: retired journal retains live forest", destination)
				}
			}
			if sources[destination] != nil {
				return report, fmt.Errorf("%s: retired journal still has invocation", destination)
			}
			report.Retired++
			continue
		}
		if root.Graph.Count != c.Count-c.RetainedFrom || counts["archive"] != c.RetainedFrom || c.RetainedFrom > 0 && (c.Schema != "js-wf-graph-runtime-cursor-v6" || len(c.Checkpoint) == 0) {
			return report, fmt.Errorf("%s: archive/live journal counts differ", destination)
		}
		if c.Start != nil && counts["input"] != 1 {
			return report, fmt.Errorf("%s: canonical input forest missing", destination)
		}
		if counts["signal-input"] != c.SignalInputs || counts["signal-queue"] != c.SignalBindings {
			return report, fmt.Errorf("%s: signal forest counts differ", destination)
		}
		if c.Invocation == 0 {
			if c.Start == nil || c.Count != 0 || c.Epoch != 0 || c.Kind != "" || c.Purging {
				return report, fmt.Errorf("%s: invalid pending start", destination)
			}
			report.Pending++
		} else if c.Invocation <= c.PreviousInvocation {
			return report, fmt.Errorf("%s: invocation generation did not advance", destination)
		}
		source := sources[destination]
		if source == nil && c.Invocation != 0 {
			return report, fmt.Errorf("%s: canonical invocation source missing", destination)
		}
		if source != nil {
			if c.Invocation != 0 && source.Sequence != c.Invocation || c.Invocation == 0 && source.Sequence <= c.PreviousInvocation {
				return report, fmt.Errorf("%s: invocation sequence differs", destination)
			}
			if c.Start != nil && !auditStartSource(*c.Start, source) {
				return report, fmt.Errorf("%s: canonical source pointer differs", destination)
			}
			consumed[destination] = true
		}
		state := &auditedGraphJournal{cursor: c, key: key}
		if len(c.Checkpoint) != 0 {
			pointer, err := auditCheckpointPointer(c)
			if err != nil {
				return report, fmt.Errorf("%s: %w", destination, err)
			}
			state.checkpoint = pointer
		}
		states[destination] = state
	}
	for destination := range sources {
		if !consumed[destination] {
			return report, fmt.Errorf("%s: source without canonical journal", destination)
		}
	}
	graph := snapshot.Graph
	prior := graph.VisitRecord
	graph.VisitRecord = func(call context.Context, record GraphAuditRecord) error {
		if prior != nil {
			if err := prior(call, record); err != nil {
				return err
			}
		}
		if record.ReaderID != "" {
			return nil
		}
		state := states[record.Destination]
		if state == nil {
			return nil
		}
		c := state.cursor
		if record.Stream == "input" {
			var start journal.GraphStart
			if c.Start == nil || record.Index != 0 || json.Unmarshal(record.Record.Data, &start) != nil || start != *c.Start || len(record.Record.Blobs) != 1 || record.Record.Blobs[0].Hash != start.InputSHA256 {
				return fmt.Errorf("%s: canonical owned input differs", record.Destination)
			}
			data, err := graph.LoadObject(call, record.Record.Blobs[0].Reference.Object, graph.PayloadLimit)
			if err != nil {
				return err
			}
			if len(data) != start.InputSize || digest(data) != start.InputSHA256 {
				return fmt.Errorf("%s: canonical input bytes differ", record.Destination)
			}
			state.inputSeen = true
			return nil
		}
		if record.Stream != "" && record.Stream != "archive" {
			return nil
		}
		index := record.Index
		if record.Stream == "" {
			index += c.RetainedFrom
		}
		var envelope struct {
			Schema      string `json:"schema"`
			Invocation  uint64 `json:"invocation"`
			Sequence    uint64 `json:"sequence"`
			EntrySHA256 string `json:"entry_sha256"`
		}
		if json.Unmarshal(record.Record.Data, &envelope) != nil || envelope.Schema != "js-wf-graph-journal-entry-v1" || envelope.Invocation != c.Invocation || envelope.Sequence != c.Base+index+1 || !graphAuditHash(envelope.EntrySHA256) {
			return fmt.Errorf("%s: canonical journal envelope differs", record.Destination)
		}
		var entry journal.Entry
		found := false
		for _, link := range record.Record.Blobs {
			if link.Hash == envelope.EntrySHA256 {
				if found {
					return fmt.Errorf("duplicate canonical entry edge")
				}
				found = true
				data, err := graph.LoadObject(call, link.Reference.Object, journal.MaxGraphEntryBytes)
				if err != nil {
					return err
				}
				if len(data) > journal.MaxGraphEntryBytes || digest(data) != envelope.EntrySHA256 || journal.UnmarshalEntry(data, &entry) != nil {
					return fmt.Errorf("invalid canonical entry bytes")
				}
			}
		}
		if !found || entry.Index != index || entry.Epoch > c.Epoch || index+1 == c.Count && (entry.Epoch != c.Epoch || entry.Kind != c.Kind) {
			return fmt.Errorf("%s: canonical entry index/epoch/tail differs", record.Destination)
		}
		if err := state.journal.advance(record.Destination, journal.Record{Entry: entry, Sequence: envelope.Sequence}); err != nil {
			return err
		}
		if entry.Kind == journal.StepRequested || entry.Kind == journal.StepCompleted {
			state.sdkPosition++
		}
		if entry.Kind == journal.StepRequested {
			state.lastStepRequest = index
		}
		if state.checkpoint != nil {
			pointer := state.checkpoint
			if index == pointer.RequestIndex {
				state.checkpointRequest = journal.Record{Entry: entry, Sequence: envelope.Sequence}
			}
			if index == pointer.Runtime.Index {
				if err := auditCheckpointFrame(call, graph, state, journal.Record{Entry: entry, Sequence: envelope.Sequence}, record.Record.Blobs); err != nil {
					return err
				}
				state.checkpointSeen = true
			}
		}
		if entry.Kind == journal.Completed || entry.Kind == journal.Failed {
			var outcome struct {
				InvSeq     uint64 `json:"inv_seq"`
				Result     []byte `json:"result"`
				ResultRef  string `json:"result_ref"`
				ResultHash string `json:"result_hash"`
				Error      string `json:"error"`
			}
			if json.Unmarshal(entry.Payload, &outcome) != nil || outcome.InvSeq != c.Invocation || entry.Kind == journal.Completed && outcome.Error != "" || entry.Kind == journal.Failed && (outcome.Error == "" || len(outcome.Result) != 0 || outcome.ResultRef != "" || outcome.ResultHash != "") {
				return fmt.Errorf("%s: invalid canonical terminal", record.Destination)
			}
			if outcome.ResultRef != "" {
				if len(outcome.Result) != 0 || !graphAuditHash(outcome.ResultHash) {
					return fmt.Errorf("invalid canonical result pointer")
				}
				matched := false
				for _, link := range record.Record.Blobs {
					if link.Hash == outcome.ResultHash {
						data, err := graph.LoadObject(call, link.Reference.Object, graph.PayloadLimit)
						if err != nil {
							return err
						}
						if len(data) > graph.PayloadLimit || digest(data) != outcome.ResultHash {
							return fmt.Errorf("invalid canonical result bytes")
						}
						matched = true
					}
				}
				if !matched {
					return fmt.Errorf("canonical result lacks owned edge")
				}
			} else if outcome.ResultHash != "" {
				return fmt.Errorf("canonical result hash without pointer")
			}
		}
		return nil
	}
	report.References, err = CheckGraphReferences(ctx, graph)
	if err != nil {
		return report, err
	}
	destinations := make([]string, 0, len(states))
	for destination := range states {
		destinations = append(destinations, destination)
	}
	sort.Strings(destinations)
	for _, destination := range destinations {
		state := states[destination]
		if uint64(state.journal.count) != state.cursor.Count || state.cursor.Start != nil && !state.inputSeen || state.checkpoint != nil && !state.checkpointSeen {
			return report, fmt.Errorf("%s: canonical journal/input census differs", destination)
		}
		if state.cursor.Count == 0 && (state.cursor.Epoch != 0 || state.cursor.Kind != "") {
			return report, fmt.Errorf("%s: invalid empty canonical journal", destination)
		}
		_, terminal, err := state.journal.finish(destination, func() ([]byte, error) { return snapshot.ReadProjection(ctx, state.key) })
		if err != nil {
			return report, err
		}
		report.Journals++
		report.Entries += state.journal.count
		if terminal {
			report.Terminal++
		}
	}
	return report, nil
}

func auditStartSource(start journal.GraphStart, source *jetstream.RawStreamMsg) bool {
	pointer, _ := json.Marshal(struct {
		Schema string `json:"schema"`
		Token  string `json:"token"`
	}{"js-wf-canonical-start-pointer-v1", start.Token})
	parent := ""
	if start.Request.ParentInvocation != 0 {
		parent = strconv.FormatUint(start.Request.ParentInvocation, 10)
	}
	return bytes.Equal(source.Data, pointer) && source.Header.Get("Wf-Graph-Start-Token") == start.Token && source.Header.Get("Wf-Input-SHA256") == start.InputSHA256 && source.Header.Get("Wf-Input-Ref") == "" && source.Header.Get("Wf-Parent-Type") == start.Request.ParentType && source.Header.Get("Wf-Parent-ID") == start.Request.ParentID && source.Header.Get("Wf-Parent-Inv-Seq") == parent && source.Header.Get("Wf-Parent-Signal") == start.Request.SignalName
}
