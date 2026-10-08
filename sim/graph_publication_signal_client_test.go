package sim

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"js-wf/client"
	"js-wf/identity"
	"js-wf/journal"
	"js-wf/wf"
)

var graphSignalClientModes = []string{"running", "uninitialized", "empty_started", "terminal", "failed", "cancelled", "bad_terminal", "forged_state", "forged_legacy_terminal", "purging", "retired", "missing_invocation_retired", "replaced_before_publish", "replaced_after_publish", "fence_before_publish", "fence_after_publish", "unknown_root", "unknown_pin", "duplicate_inline", "duplicate_external", "duplicate_wrong_hash", "duplicate_missing", "duplicate_missing_edge", "duplicate_false_inline", "duplicate_twice", "duplicate_legacy_decoy", "drop_publish", "lost_publish", "invalid_ack", "foreign_subject", "old_root_new_invocation"}

type graphSignalClientPort struct {
	*SignalTransport
	reads          int
	beforeRead     map[int]func() error
	afterPublish   func() error
	invalidAck     bool
	foreignSubject bool
}

func (p *graphSignalClientPort) LastInvocation(ctx context.Context, subject string) (*jetstream.RawStreamMsg, error) {
	p.reads++
	if hook := p.beforeRead[p.reads]; hook != nil {
		if err := hook(); err != nil {
			return nil, err
		}
	}
	return p.SignalTransport.LastInvocation(ctx, subject)
}
func (p *graphSignalClientPort) PublishSignal(ctx context.Context, msg *nats.Msg, id string) (client.SignalPublishAck, error) {
	ack, err := p.SignalTransport.PublishSignal(ctx, msg, id)
	if err != nil {
		return ack, err
	}
	if p.afterPublish != nil {
		hook := p.afterPublish
		p.afterPublish = nil
		if err = hook(); err != nil {
			return ack, err
		}
	}
	if p.invalidAck {
		ack.Sequence = 0
	}
	return ack, nil
}
func (p *graphSignalClientPort) SignalBySequence(ctx context.Context, seq uint64) (*jetstream.RawStreamMsg, error) {
	msg, err := p.SignalTransport.SignalBySequence(ctx, seq)
	if err == nil && p.foreignSubject {
		msg.Subject = "wf.sig.test.other.go"
	}
	return msg, err
}

func runGraphSignalClient(seed int64, replay *Trace) (trace Trace, runErr error) {
	s := NewScheduler(seed)
	if replay != nil {
		var e error
		s, e = ReplayScheduler(*replay)
		if e != nil {
			return trace, e
		}
	}
	if e := s.SetWorkload("graph_signal_client"); e != nil {
		return trace, e
	}
	defer func() { trace = s.Trace() }()
	mode, err := s.Choose(graphSignalClientModes)
	if err != nil {
		return trace, err
	}
	ctx, stop := context.WithTimeout(context.Background(), 5*time.Second)
	defer stop()
	model := NewGraphPublicationTransport(s)
	now := func() time.Time { return time.UnixMilli(s.NowMillis()).UTC() }
	graph, err := journal.NewGraphStore(journal.GraphConfig{Protocol: model.Protocol(), Now: now, PinTTL: 30 * time.Second, IntentTTL: time.Second})
	if err != nil {
		return trace, err
	}
	source := NewSignalTransport(s)
	port := &graphSignalClientPort{SignalTransport: source, beforeRead: map[int]func() error{}}
	resultPort := &graphResultTransport{source: source, state: NewKVTransport(s, 0)}
	c, err := client.NewWithGraphJournalPorts(source, port, resultPort, graph)
	if err != nil {
		return trace, err
	}
	const typ, id, name = "test", "signalclient", "go"
	handle, err := c.Start(ctx, typ, id, []byte(`7`))
	if err != nil {
		return trace, err
	}
	generation := handle.InvSeq
	var tail, index uint64
	appendEntry := func(kind journal.Kind, body []byte, payloads [][]byte) error {
		next, e := graph.Append(ctx, typ, id, generation, journal.Entry{Kind: kind, Index: index, Payload: body}, tail, payloads, nil)
		if e == nil {
			tail = next
			index++
		}
		return e
	}
	if mode != "uninitialized" {
		tail, err = graph.Begin(ctx, typ, id, generation)
		if err != nil {
			return trace, err
		}
	}
	if mode != "uninitialized" && mode != "empty_started" {
		if err = appendEntry(journal.Started, nil, nil); err != nil {
			return trace, err
		}
	}
	finish := func() error {
		if index == 0 {
			if err := appendEntry(journal.Started, nil, nil); err != nil {
				return err
			}
		}
		outcome := wf.Outcome{InvSeq: generation, Result: []byte(`42`)}
		kind := journal.Completed
		if mode == "failed" || mode == "cancelled" {
			kind = journal.Failed
			outcome = wf.Outcome{InvSeq: generation, Error: "failed"}
			if mode == "cancelled" {
				outcome.Error = client.ErrCancelled.Error()
			}
		}
		if mode == "bad_terminal" {
			outcome.InvSeq++
		}
		body, _ := json.Marshal(outcome)
		return appendEntry(kind, body, nil)
	}
	terminalFixture := mode == "terminal" || mode == "failed" || mode == "cancelled" || mode == "bad_terminal" || mode == "purging" || mode == "retired" || mode == "missing_invocation_retired" || mode == "unknown_pin" || mode == "old_root_new_invocation"
	if terminalFixture {
		if err = finish(); err != nil {
			return trace, err
		}
	}
	if mode == "purging" {
		if err = graph.FencePurge(ctx, typ, id, generation, tail); err != nil {
			return trace, err
		}
	}
	if mode == "retired" || mode == "missing_invocation_retired" || mode == "old_root_new_invocation" {
		if err = graph.Retire(ctx, typ, id, generation, tail); err != nil {
			return trace, err
		}
	}
	if mode == "missing_invocation_retired" {
		source.PurgeInvocation(identity.InvocationSubject(typ, id))
	}
	replace := func() error {
		source.PurgeInvocation(identity.InvocationSubject(typ, id))
		_, e := source.PublishInvocation(ctx, &nats.Msg{Subject: identity.InvocationSubject(typ, id), Data: []byte(`8`), Header: nats.Header{jetstream.ExpectedLastSubjSeqHeader: {"0"}}})
		return e
	}
	fence := func() error {
		if e := finish(); e != nil {
			return e
		}
		return graph.FencePurge(ctx, typ, id, generation, tail)
	}
	if mode == "old_root_new_invocation" {
		if err = replace(); err != nil {
			return trace, err
		}
	}
	if mode == "replaced_before_publish" {
		port.beforeRead[2] = replace
	}
	if mode == "replaced_after_publish" {
		port.afterPublish = replace
	}
	if mode == "fence_before_publish" {
		port.beforeRead[2] = fence
	}
	if mode == "fence_after_publish" {
		port.afterPublish = fence
	}
	if mode == "forged_state" {
		source.SetState(identity.Key(typ, id), []byte(`{"tombstone":true,"inv_seq":999,"purged_at":"2026-01-01T00:00:00Z","expires_at":"2026-01-02T00:00:00Z"}`))
	}
	if mode == "forged_legacy_terminal" {
		source.SetJournal(typ, id, []journal.Record{{Entry: journal.Entry{Kind: journal.Completed}}})
	}
	payload := []byte(`42`)
	duplicate := strings.HasPrefix(mode, "duplicate_") || mode == "foreign_subject"
	if duplicate {
		first, e := c.Signal(ctx, typ, id, name, payload, "once")
		if e != nil {
			return trace, e
		}
		if mode != "foreign_subject" {
			data := payload
			if mode == "duplicate_false_inline" {
				data = []byte(`43`)
			}
			event := map[string]any{"sig_seq": first, "name": name, "hash": digest(payload), "payload": data}
			var owned [][]byte
			if mode == "duplicate_external" || mode == "duplicate_missing_edge" {
				delete(event, "payload")
				event["ref"] = "signal-" + digest(payload)
				if mode == "duplicate_external" {
					owned = [][]byte{payload}
				}
			}
			body, _ := json.Marshal(event)
			if mode == "duplicate_legacy_decoy" {
				source.SetJournal(typ, id, []journal.Record{{Entry: journal.Entry{Kind: journal.SignalConsumed, Payload: body}}})
			} else if mode != "duplicate_missing" {
				if e = appendEntry(journal.SignalConsumed, body, owned); e != nil {
					return trace, e
				}
			}
			if mode == "duplicate_twice" {
				if e = appendEntry(journal.SignalConsumed, body, owned); e != nil {
					return trace, e
				}
			}
			source.PurgeSignal(first)
		} else {
			port.foreignSubject = true
		}
		if mode == "duplicate_wrong_hash" {
			payload = []byte(`43`)
		}
	}
	if mode == "unknown_root" {
		if err = model.QueueFault("read_root", DropBeforeCommit); err != nil {
			return trace, err
		}
	}
	if mode == "unknown_pin" {
		model.PauseBefore("cas_root", func() error { return model.QueueFault("read_root", DropBeforeCommit) })
		if err = model.QueueFault("cas_root", LoseAckAfterCommit); err != nil {
			return trace, err
		}
	}
	if mode == "drop_publish" {
		if err = source.QueueSignalFault(SignalDropBeforeCommit); err != nil {
			return trace, err
		}
	}
	if mode == "lost_publish" {
		if err = source.QueueSignalFault(SignalLoseAckAfterCommit); err != nil {
			return trace, err
		}
	}
	port.invalidAck = mode == "invalid_ack"
	before := len(source.Runs())
	var seq uint64
	if mode == "missing_invocation_retired" {
		seq, err = c.SignalToGeneration(ctx, typ, id, name, payload, "once", generation)
	} else {
		seq, err = c.SignalWithOptions(ctx, typ, id, name, payload, "once", client.SignalOptions{RequireRunning: !duplicate})
	}
	var wanted error
	switch mode {
	case "terminal", "failed", "cancelled":
		wanted = client.ErrNotRunning
	case "bad_terminal", "duplicate_missing_edge", "duplicate_false_inline", "duplicate_twice", "invalid_ack", "foreign_subject":
		wanted = wf.ErrCorruptJournal
	case "purging", "retired", "fence_before_publish", "fence_after_publish":
		wanted = client.ErrPurged
	case "missing_invocation_retired", "replaced_before_publish", "replaced_after_publish":
		wanted = client.ErrStaleGeneration
	case "unknown_root", "unknown_pin":
		wanted = journal.ErrUnknown
	case "duplicate_wrong_hash":
		wanted = client.ErrSignalMismatch
	case "duplicate_missing", "duplicate_legacy_decoy", "drop_publish", "lost_publish":
		wanted = client.ErrSignalUnknown
	}
	if wanted == nil {
		wakeups := before + 1
		if duplicate {
			wakeups = before
		}
		if err != nil || seq == 0 || len(source.Runs()) != wakeups {
			return trace, fmt.Errorf("accepted signal %s: seq=%d err=%v", mode, seq, err)
		}
	} else {
		if !errors.Is(err, wanted) || len(source.Runs()) != before {
			return trace, fmt.Errorf("rejected signal %s: seq=%d err=%v want=%v", mode, seq, err, wanted)
		}
		committed := mode == "fence_after_publish" || mode == "replaced_after_publish"
		if (seq != 0) != committed {
			return trace, fmt.Errorf("lost committed sequence %s: %d", mode, seq)
		}
	}
	for _, e := range s.Trace().Transport {
		if e.Operation == "state_get" || e.Operation == "last_journal" || e.Operation == "read_journal" {
			return trace, fmt.Errorf("graph signal consulted legacy authority: %s", e.Operation)
		}
	}
	if mode == "drop_publish" || mode == "lost_publish" {
		if seq, err = c.Signal(ctx, typ, id, name, payload, "once"); err != nil || seq != 1 {
			return trace, fmt.Errorf("unknown publish retry: %d %v", seq, err)
		}
	}
	status, e := graph.InspectRetirement(ctx, typ, id)
	if e != nil {
		return trace, e
	}
	if !status.Retired {
		if status.Invocation == 0 {
			tail, e = graph.Begin(ctx, typ, id, generation)
			if e != nil {
				return trace, e
			}
		}
		if status.Kind != journal.Completed && status.Kind != journal.Failed {
			if e = finish(); e != nil {
				return trace, e
			}
		}
		if e = graph.Retire(ctx, typ, id, generation, tail); e != nil {
			return trace, e
		}
	}
	if e = s.AdvanceMillis(60000); e != nil {
		return trace, e
	}
	if _, e = model.Protocol().SweepWithReaders(ctx, now()); e != nil {
		return trace, e
	}
	objects, e := model.Objects(ctx)
	if e != nil || len(objects) != 0 {
		return trace, fmt.Errorf("graph did not drain: %d %v", len(objects), e)
	}
	if e = model.CheckReferences(); e != nil {
		return trace, e
	}
	s.RecordTransport(TransportEvent{Operation: "check_graph_signal_client", Outcome: mode})
	if e = s.Finish(); e != nil {
		return trace, e
	}
	return trace, nil
}

func TestSeededGraphSignalClientReplay(t *testing.T) {
	observed := map[string]int{}
	fail := func(seed int64, trace Trace, cause error) {
		path := os.Getenv("FAULT_TRACE_OUT")
		if path == "" {
			dir, err := os.MkdirTemp("", "js-wf-graph-signal-client-failure-")
			if err != nil {
				t.Fatal(err)
			}
			path = filepath.Join(dir, "trace.json")
		}
		if err := trace.Save(path); err != nil {
			t.Fatal(err)
		}
		t.Fatalf("FAULT_SEED=%d FAULT_TRACE=%s: %v", seed, path, cause)
	}
	for seed := range seededSchedules(t) {
		generated, err := runGraphSignalClient(seed, nil)
		if err != nil {
			fail(seed, generated, err)
		}
		replayed, err := runGraphSignalClient(seed, &generated)
		if err != nil || !reflect.DeepEqual(generated, replayed) {
			fail(seed, generated, fmt.Errorf("replay differs: %v", err))
		}
		mode := generated.Decisions[0].Chosen
		observed[mode]++
		if dir := os.Getenv("SIM_GRAPH_SIGNAL_CLIENT_ROOT"); dir != "" && observed[mode] == 1 {
			if err = generated.Save(filepath.Join(dir, mode+".json")); err != nil {
				t.Fatal(err)
			}
		}
	}
	if len(observed) != len(graphSignalClientModes) {
		t.Fatalf("coverage=%v", observed)
	}
	t.Logf("graph signal client modes=%v; canonical lifecycle, owned consumption, generation rechecks and graph fixture drain", observed)
}
