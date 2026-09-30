package history

import (
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"time"

	"js-wf/client"

	"github.com/anishathalye/porcupine"
)

type signalInput struct {
	Type           string `json:"type"`
	ID             string `json:"id"`
	Name           string `json:"name"`
	PayloadHash    string `json:"payload_hash"`
	IdempotencyKey string `json:"idempotency_key"`
	InvSeq         uint64 `json:"inv_seq"`
}

type signalOutput struct {
	Status    string `json:"status"`
	SignalSeq uint64 `json:"signal_seq"`
}

type signalEntry struct {
	Key        string
	Hash       string
	Sequence   uint64
	LowerBound uint64 // exclusive bound on an unknown but committed publish
	UpperBound uint64 // exclusive bound; zero means no known upper bound
}

type signalState struct {
	MaxKnown uint64
	Entries  []signalEntry // sorted by Key for stable state comparison
}

// CheckSignals checks queue append order and idempotent retry responses for a
// WF_SIG queue across duplicate identities. Each identity's history must fit within two
// minutes: JetStream may accept its reused key after that window. Distinct
// identities may span a sustained run; stream-order checks still cover them.
// Unknown publishes branch into absent and committed possibilities.
func CheckSignals(operations []client.Operation, timeout time.Duration) (porcupine.CheckResult, error) {
	var converted []porcupine.Operation
	type identity struct {
		typ, id, name, key string
		invSeq             uint64
	}
	type window struct{ earliest, latest time.Time }
	windows := map[identity]window{}
	knownOutcomes := true
	for index, operation := range operations {
		if operation.Op != "signal" {
			continue
		}
		var input signalInput
		var output signalOutput
		if err := json.Unmarshal(operation.Args, &input); err != nil {
			return porcupine.Illegal, fmt.Errorf("operation %d args: %w", index, err)
		}
		if err := json.Unmarshal(operation.Result, &output); err != nil {
			return porcupine.Illegal, fmt.Errorf("operation %d result: %w", index, err)
		}
		if input.Type == "" || input.ID == "" || input.Name == "" || input.PayloadHash == "" || input.IdempotencyKey == "" || operation.InvokeTS.IsZero() || operation.ReturnTS.Before(operation.InvokeTS) {
			return porcupine.Illegal, fmt.Errorf("operation %d has invalid signal arguments or timestamps", index)
		}
		switch output.Status {
		case "signaled", "enqueue_unknown", "unknown", "payload_mismatch", "not_found", "not_published":
		default:
			return porcupine.Unknown, fmt.Errorf("operation %d has unsupported signal outcome %q", index, output.Status)
		}
		if output.Status == "unknown" {
			knownOutcomes = false
		}
		key := identity{input.Type, input.ID, input.Name, input.IdempotencyKey, input.InvSeq}
		span := windows[key]
		if span.earliest.IsZero() || operation.InvokeTS.Before(span.earliest) {
			span.earliest = operation.InvokeTS
		}
		if operation.ReturnTS.After(span.latest) {
			span.latest = operation.ReturnTS
		}
		windows[key] = span
		converted = append(converted, porcupine.Operation{Input: input, Output: output, Call: operation.InvokeTS.UnixNano(), Return: operation.ReturnTS.UnixNano()})
	}
	if len(converted) == 0 {
		return porcupine.Unknown, fmt.Errorf("no signal operations")
	}
	for _, span := range windows {
		if span.latest.Sub(span.earliest) >= 2*time.Minute {
			return porcupine.Unknown, fmt.Errorf("signal identity history spans the configured duplicate window")
		}
	}
	if knownOutcomes && !knownSignalOrder(converted) {
		return porcupine.Illegal, nil
	}
	model := porcupine.NondeterministicModel{
		Partition: func(history []porcupine.Operation) [][]porcupine.Operation {
			byInvocation := map[string][]porcupine.Operation{}
			for _, operation := range history {
				input := operation.Input.(signalInput)
				key := fmt.Sprintf("%s.%s:%d", input.Type, input.ID, input.InvSeq)
				if knownOutcomes {
					key += ":" + input.Name + ":" + input.IdempotencyKey
				}
				byInvocation[key] = append(byInvocation[key], operation)
			}
			keys := make([]string, 0, len(byInvocation))
			for key := range byInvocation {
				keys = append(keys, key)
			}
			sort.Strings(keys)
			parts := make([][]porcupine.Operation, 0, len(keys))
			for _, key := range keys {
				parts = append(parts, byInvocation[key])
			}
			return parts
		},
		Init:  func() []interface{} { return []interface{}{signalState{}} },
		Equal: func(a, b interface{}) bool { return reflect.DeepEqual(a, b) },
		Step:  signalStep,
	}
	return porcupine.CheckOperationsTimeout(model.ToModel(), converted, timeout), nil
}

// A known successful publish has a stable WF_SIG sequence. Distinct keys must
// never share one, and completed appends must precede later invoked appends in
// stream order. Within each key, Porcupine checks retries and mismatches.
func knownSignalOrder(operations []porcupine.Operation) bool {
	type appendRecord struct {
		owner string
		call  int64
		ret   int64
		seq   uint64
	}
	byOwner := map[string]appendRecord{}
	owners := map[string]string{}
	for _, op := range operations {
		input := op.Input.(signalInput)
		output := op.Output.(signalOutput)
		if output.Status != "signaled" && output.Status != "enqueue_unknown" {
			continue
		}
		owner := fmt.Sprintf("%s.%s:%d:%s:%s", input.Type, input.ID, input.InvSeq, input.Name, input.IdempotencyKey)
		if output.SignalSeq == 0 {
			return false
		}
		stream := fmt.Sprint(output.SignalSeq)
		if prior, ok := owners[stream]; ok && prior != owner {
			return false
		}
		owners[stream] = owner
		if prior, ok := byOwner[owner]; ok {
			if prior.seq != output.SignalSeq {
				return false
			}
			if op.Call < prior.call {
				prior.call = op.Call
			}
			if op.Return < prior.ret {
				prior.ret = op.Return
			}
			byOwner[owner] = prior
		} else {
			byOwner[owner] = appendRecord{owner, op.Call, op.Return, output.SignalSeq}
		}
	}
	appends := make([]appendRecord, 0, len(byOwner))
	for _, item := range byOwner {
		appends = append(appends, item)
	}
	for i, first := range appends {
		for _, second := range appends[i+1:] {
			if first.owner == second.owner {
				continue
			}
			if first.ret < second.call && first.seq >= second.seq || second.ret < first.call && second.seq >= first.seq {
				return false
			}
		}
	}
	return true
}

func signalStep(rawState, rawInput, rawOutput interface{}) []interface{} {
	state := rawState.(signalState)
	input := rawInput.(signalInput)
	output := rawOutput.(signalOutput)
	key := input.Name + "\x00" + input.IdempotencyKey
	index := -1
	for i, entry := range state.Entries {
		if entry.Key == key {
			index = i
			break
		}
	}
	switch output.Status {
	case "not_published":
		if output.SignalSeq != 0 {
			return nil
		}
		return []interface{}{state}
	case "not_found":
		if output.SignalSeq != 0 || input.InvSeq != 0 {
			return nil
		}
		return []interface{}{state}
	case "payload_mismatch":
		if output.SignalSeq != 0 || index < 0 || state.Entries[index].Hash == input.PayloadHash {
			return nil
		}
		return []interface{}{state}
	case "unknown":
		if output.SignalSeq != 0 || input.InvSeq == 0 {
			return nil
		}
		if index >= 0 {
			return []interface{}{state}
		}
		committed := copySignalState(state)
		committed.Entries = append(committed.Entries, signalEntry{Key: key, Hash: input.PayloadHash, LowerBound: state.MaxKnown})
		sort.Slice(committed.Entries, func(i, j int) bool { return committed.Entries[i].Key < committed.Entries[j].Key })
		return []interface{}{state, committed}
	case "signaled", "enqueue_unknown":
		seq := output.SignalSeq
		if seq == 0 || input.InvSeq == 0 {
			return nil
		}
		if index >= 0 {
			entry := state.Entries[index]
			if entry.Hash != input.PayloadHash {
				return nil
			}
			if entry.Sequence != 0 {
				if entry.Sequence == seq {
					return []interface{}{state}
				}
				return nil
			}
			if seq <= entry.LowerBound || entry.UpperBound != 0 && seq >= entry.UpperBound || sequenceUsed(state, seq) {
				return nil
			}
			bound := copySignalState(state)
			bound.Entries[index].Sequence = seq
			if seq > bound.MaxKnown {
				bound.MaxKnown = seq
			}
			return []interface{}{bound}
		}
		if seq <= state.MaxKnown || sequenceUsed(state, seq) {
			return nil
		}
		appended := copySignalState(state)
		for i := range appended.Entries {
			entry := &appended.Entries[i]
			if entry.Sequence == 0 && (entry.UpperBound == 0 || seq < entry.UpperBound) {
				entry.UpperBound = seq
				if entry.UpperBound <= entry.LowerBound+1 {
					return nil
				}
			}
		}
		appended.MaxKnown = seq
		appended.Entries = append(appended.Entries, signalEntry{Key: key, Hash: input.PayloadHash, Sequence: seq})
		sort.Slice(appended.Entries, func(i, j int) bool { return appended.Entries[i].Key < appended.Entries[j].Key })
		return []interface{}{appended}
	default:
		return nil
	}
}

func copySignalState(state signalState) signalState {
	state.Entries = append([]signalEntry(nil), state.Entries...)
	return state
}

func sequenceUsed(state signalState, sequence uint64) bool {
	for _, entry := range state.Entries {
		if entry.Sequence == sequence {
			return true
		}
	}
	return false
}
