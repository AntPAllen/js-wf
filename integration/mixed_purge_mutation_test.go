//go:build linux

package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"js-wf/client"
	"js-wf/identity"
	"js-wf/integrity"
	"js-wf/journal"
	"js-wf/retention"
	"js-wf/wf"
)

var errMixedPurgeCut = errors.New("mixed purge cut before signal purge")

type mixedPurgeCutJS struct {
	jetstream.JetStream
	cuts atomic.Int32
}

func (p *mixedPurgeCutJS) Stream(ctx context.Context, name string) (jetstream.Stream, error) {
	s, err := p.JetStream.Stream(ctx, name)
	if err != nil || name != "WF_SIG" {
		return s, err
	}
	return mixedPurgeCutStream{Stream: s, parent: p}, nil
}

type mixedPurgeCutStream struct {
	jetstream.Stream
	parent *mixedPurgeCutJS
}

func (p mixedPurgeCutStream) Purge(context.Context, ...jetstream.StreamPurgeOpt) error {
	p.parent.cuts.Add(1)
	return errMixedPurgeCut
}

func challengeMixedPurge(t *testing.T, ctx context.Context, js jetstream.JetStream, typ, id string, before []journal.Record) client.Handle {
	t.Helper()
	inv, err := js.Stream(ctx, "WF_INV")
	if err != nil {
		t.Fatal(err)
	}
	input, err := inv.GetLastMsgForSubject(ctx, identity.InvocationSubject(typ, id))
	if err != nil {
		t.Fatal(err)
	}
	proof, err := json.Marshal(input)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("MIXED_PURGE_RAW_INVOCATION %s", proof)
	state, err := js.KeyValue(ctx, "WF_STATE")
	if err != nil {
		t.Fatal(err)
	}
	prior, err := state.Get(ctx, identity.Key(typ, id))
	if err != nil {
		t.Fatal(err)
	}
	cut := &mixedPurgeCutJS{JetStream: js}
	err = retention.Purge(ctx, cut, typ, id, time.Minute)
	if !errors.Is(err, errMixedPurgeCut) || cut.cuts.Load() != 1 {
		t.Fatalf("unrelated purge cut err=%v cuts=%d", err, cut.cuts.Load())
	}
	marker, err := state.Get(ctx, "purging."+identity.Key(typ, id))
	if err != nil || string(marker.Value()) != strconv.FormatUint(input.Sequence, 10) {
		t.Fatalf("missing committed purge marker err=%v", err)
	}
	after, _, err := journal.New(js).Read(ctx, typ, id)
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatalf("purge cut changed journal err=%v", err)
	}
	current, err := state.Get(ctx, identity.Key(typ, id))
	if err != nil || current.Revision() != prior.Revision() || !bytes.Equal(current.Value(), prior.Value()) {
		t.Fatalf("purge cut changed terminal err=%v", err)
	}
	proof, err = json.Marshal(after)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("MIXED_PURGE_RETAINED_JOURNAL %s", proof)
	t.Logf("MIXED_PURGE_RETAINED_STATE revision=%d value=%s", current.Revision(), current.Value())
	retained, err := inv.GetLastMsgForSubject(ctx, identity.InvocationSubject(typ, id))
	if errors.Is(err, jetstream.ErrMsgNotFound) {
		retryErr := retention.Purge(ctx, js, typ, id, time.Minute)
		if !errors.Is(retryErr, retention.ErrNotFound) {
			t.Fatalf("unrelated purge retry err=%v", retryErr)
		}
		t.Fatal("MIXED_MUTATION_ESCAPE category=invocation_purged_first cut_before=signals invocation=absent journal=unchanged terminal=unchanged purge_marker=retained retry=invocation_not_found original_terminal=28")
	}
	if err != nil || retained.Sequence != input.Sequence || !bytes.Equal(retained.Data, input.Data) {
		t.Fatalf("purge cut invocation changed err=%v", err)
	}
	t.Logf("MIXED_PURGE_CUT before=signals invocation=retained sequence=%d journal=unchanged terminal=unchanged", retained.Sequence)
	if err := retention.Purge(ctx, js, typ, id, time.Minute); err != nil {
		t.Fatal("purge resume", err)
	}
	if err := retention.Purge(ctx, js, typ, id, time.Minute); err != nil {
		t.Fatal("purge idempotent retry", err)
	}
	_, err = inv.GetLastMsgForSubject(ctx, identity.InvocationSubject(typ, id))
	if !errors.Is(err, jetstream.ErrMsgNotFound) {
		t.Fatalf("purged invocation read err=%v", err)
	}
	remaining, _, err := journal.New(js).Read(ctx, typ, id)
	if err != nil || len(remaining) != 0 {
		t.Fatalf("purged journal entries=%d err=%v", len(remaining), err)
	}
	terminal, err := state.Get(ctx, identity.Key(typ, id))
	if err != nil {
		t.Fatal(err)
	}
	tomb, isTomb, err := retention.Decode(terminal.Value())
	if err != nil || !isTomb || tomb.InvSeq != input.Sequence {
		t.Fatalf("purge tombstone %+v err=%v", tomb, err)
	}
	_, err = state.Get(ctx, "purging."+identity.Key(typ, id))
	if !errors.Is(err, jetstream.ErrKeyNotFound) {
		t.Fatalf("purge marker read err=%v", err)
	}
	t.Logf("MIXED_PURGE_RESUMED tombstone=%s", terminal.Value())
	reused, err := client.New(js).Start(ctx, typ, id, []byte(`1`))
	if err != nil || reused.InvSeq <= input.Sequence {
		t.Fatalf("purge reuse %+v err=%v", reused, err)
	}
	return reused
}

func verifyMixedPurgeReuse(t *testing.T, ctx context.Context, js jetstream.JetStream, h client.Handle, old []journal.Record) {
	t.Helper()
	records, _, err := journal.New(js).Read(ctx, h.Type, h.ID)
	if err != nil || len(records) != 4 || records[0].Index != 0 || records[0].Kind != journal.Started || records[0].Sequence <= old[len(old)-1].Sequence {
		t.Fatalf("reused journal %+v err=%v", records, err)
	}
	var outcome wf.Outcome
	if json.Unmarshal(records[3].Payload, &outcome) != nil || outcome.InvSeq != h.InvSeq || string(outcome.Result) != "42" {
		t.Fatalf("reused outcome %+v", outcome)
	}
	report, err := integrity.Check(ctx, js)
	if err != nil || report.Invocations != 28 || report.Journals != 28 || report.Terminal != 28 {
		t.Fatalf("reused retained audit %+v err=%v", report, err)
	}
	proof, err := json.Marshal(records)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("MIXED_PURGE_REUSED_JOURNAL %s", proof)
	t.Logf("MIXED_PURGE_REJECTED terminal=28 reused_sequence=%d outcome_sequence=%d index0=true prior_prefix_absent=true", h.InvSeq, outcome.InvSeq)
}
