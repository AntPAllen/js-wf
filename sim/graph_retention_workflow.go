package sim

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"time"

	"js-wf/identity"
	"js-wf/journal"
	"js-wf/retention"
	"js-wf/wf"
)

func RunGraphRetentionWorkflowSDKCut(seed int64, cut int, reuse bool, replay *Trace) (trace Trace, runErr error) {
	s := NewScheduler(seed)
	if replay != nil {
		var err error
		s, err = ReplayScheduler(*replay)
		if err != nil {
			return trace, err
		}
	}
	if err := s.SetWorkload(fmt.Sprintf("graph_retention_workflow_cut_%d_reuse_%v", cut, reuse)); err != nil {
		return trace, err
	}
	return runGraphRetentionWorkflowSDKCut(s, cut, reuse)
}

func runGraphRetentionWorkflowSDKCut(s *Scheduler, cut int, reuse bool) (trace Trace, runErr error) {
	defer func() { trace = s.Trace() }()
	ctx := context.Background()
	model := NewGraphPublicationTransport(s)
	graph, err := journal.NewGraphStore(journal.GraphConfig{Protocol: model.Protocol(), Now: func() time.Time { return time.UnixMilli(s.NowMillis()) }, PinTTL: time.Hour})
	if err != nil {
		return trace, err
	}
	port := NewPurgeTransport(s)
	port.EnableFallbackTimers()
	const typ, id = "retention-target", "source"
	create := func(result []byte) (uint64, error) {
		generation, e := port.Blobs.PublishSubject("WF_INV", identity.InvocationSubject(typ, id), nil, []byte(`7`))
		if e != nil {
			return 0, e
		}
		tail, e := graph.Begin(ctx, typ, id, generation)
		if e != nil {
			return 0, e
		}
		tail, e = graph.Append(ctx, typ, id, generation, journal.Entry{Kind: journal.Started}, tail, nil, nil)
		if e != nil {
			return 0, e
		}
		hash := sha256.Sum256(result)
		outcome := wf.Outcome{InvSeq: generation, ResultRef: "terminal-" + hex.EncodeToString(hash[:]), ResultHash: hex.EncodeToString(hash[:])}
		data, _ := json.Marshal(outcome)
		_, e = graph.Append(ctx, typ, id, generation, journal.Entry{Index: 1, Kind: journal.Completed, Payload: data}, tail, [][]byte{result}, nil)
		if e != nil {
			return 0, e
		}
		return generation, port.PutState(ctx, identity.Key(typ, id), data)
	}
	old, err := create([]byte(`42`))
	if err != nil {
		return trace, err
	}
	handler := retention.GraphHandlerWithPort(port, graph, time.Minute)
	input, _ := json.Marshal(retention.Request{Type: typ, ID: id})
	var entries []wf.Entry
	cutError := errors.New("fixture crash before SDK append")
	appendCalls := 0
	appendEntry := func(_ context.Context, kind wf.Kind, payload json.RawMessage) error {
		appendCalls++
		if appendCalls == cut {
			s.RecordTransport(TransportEvent{Operation: "retention_sdk_append_cut", Sequence: uint64(cut), Outcome: "before commit", AtMillis: s.NowMillis()})
			return cutError
		}
		entries = append(entries, wf.Entry{Index: uint64(len(entries) + 1), Kind: kind, Payload: append(json.RawMessage(nil), payload...)})
		return nil
	}
	result, err := handler(wf.NewContext(ctx, nil, appendEntry), input)
	if cut == 0 {
		if err != nil || !bytes.Equal(result, []byte(`true`)) || len(entries) != 4 {
			return trace, fmt.Errorf("clean workflow: result=%s entries=%d err=%v", result, len(entries), err)
		}
	} else if !errors.Is(err, cutError) || len(entries) != cut-1 {
		return trace, fmt.Errorf("SDK cut not reached: cut=%d entries=%d err=%v", cut, len(entries), err)
	}
	var next uint64
	var before journal.GraphRetirement
	if reuse {
		if cut < 3 {
			return trace, fmt.Errorf("reuse requires a durably recorded target")
		}
		if err := retention.PurgeGraphInvocationWithPort(ctx, port, graph, typ, id, old, time.Minute); err != nil {
			return trace, err
		}
		next, err = create([]byte(`43`))
		if err != nil || next <= old {
			return trace, fmt.Errorf("reuse failed: %d %d %v", old, next, err)
		}
		before, err = graph.InspectRetirement(ctx, typ, id)
		if err != nil {
			return trace, err
		}
	}
	// A new SDK context replays the retained entries. Its target lookup result
	// must select the OLD generation even when the invocation subject is reused.
	result, err = handler(wf.NewContext(ctx, append([]wf.Entry(nil), entries...), appendEntry), input)
	if reuse {
		if err == nil || err.Error() != journal.ErrStale.Error() {
			return trace, fmt.Errorf("replayed purge adopted replacement: result=%s err=%v", result, err)
		}
		after, e := graph.InspectRetirement(ctx, typ, id)
		if e != nil || !reflect.DeepEqual(before, after) || after.Purging || after.Retired || after.Invocation != next {
			return trace, fmt.Errorf("replayed old purge changed replacement: %+v %v", after, e)
		}
		inv, e := port.Invocation(ctx, identity.InvocationSubject(typ, id))
		if e != nil || inv.Sequence != next {
			return trace, fmt.Errorf("replacement source removed: %v", e)
		}
	} else {
		if err != nil || !bytes.Equal(result, []byte(`true`)) {
			return trace, fmt.Errorf("SDK purge retry failed: %s %v", result, err)
		}
		status, e := graph.InspectRetirement(ctx, typ, id)
		if e != nil || !status.Purging || !status.Retired || status.Invocation != old {
			return trace, fmt.Errorf("target not retired: %+v %v", status, e)
		}
	}
	if err := model.CheckReferences(); err != nil {
		return trace, err
	}
	s.RecordTransport(TransportEvent{Operation: "check_retention_sdk_replay", Sequence: old, Outcome: "bound target preserved", AtMillis: s.NowMillis()})
	return trace, s.Finish()
}
