package worker_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"js-wf/client"
	"js-wf/identity"
	"js-wf/internal/graphpublication"
	"js-wf/journal"
	"js-wf/sim"
	"js-wf/wf"
	"js-wf/worker"
)

type replayReleaseFailurePort struct {
	*signalBodyReadCounter
	denyRelease bool
}

func (p *replayReleaseFailurePort) CASRoot(ctx context.Context, key string, head uint64, root graphpublication.Root) (graphpublication.Root, error) {
	if p.denyRelease && len(root.Readers) == 0 {
		return graphpublication.Root{}, context.DeadlineExceeded
	}
	return p.GraphPublicationTransport.CASRoot(ctx, key, head, root)
}

func TestGraphReplaySnapshotOwnsInputsAndRejectedSignals(t *testing.T) {
	for _, mode := range []string{"consumed", "rejected", "unreadable", "forged-source", "release-unknown", "forged-terminal"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			schedule := sim.NewScheduler(1)
			model := sim.NewGraphPublicationTransport(schedule)
			counter := &signalBodyReadCounter{GraphPublicationTransport: model}
			protocol := model.Protocol()
			port := &replayReleaseFailurePort{signalBodyReadCounter: counter}
			protocol.Port = port
			store, err := journal.NewGraphStore(journal.GraphConfig{Protocol: protocol, CanonicalStarts: true, CanonicalSignals: true, Now: func() time.Time { return time.Unix(1000, 0) }})
			if err != nil {
				t.Fatal(err)
			}
			transport := sim.NewWorkerTransport(schedule, 3*time.Second)
			c, err := client.NewWithSignalPorts(transport.SignalTransport, transport.SignalTransport).WithGraphJournal(store)
			if err != nil {
				t.Fatal(err)
			}
			h, err := c.Start(ctx, "test", "export", []byte(`7`))
			if err != nil {
				t.Fatal(err)
			}
			input, err := store.ReserveSignal(ctx, journal.GraphSignalRequest{Type: h.Type, ID: h.ID, Invocation: h.InvSeq, Name: "go", Key: "key"}, []byte(`true`), false)
			if err != nil {
				t.Fatal(err)
			}
			message := &nats.Msg{Subject: "wf.sig." + h.Type + "." + h.ID + ".go", Data: input.PointerBytes(), Header: nats.Header{}}
			message.Header.Set(journal.GraphSignalTokenHeader, input.Token)
			message.Header.Set("Wf-Input-SHA256", input.InputSHA256)
			message.Header.Set("Wf-Inv-Seq", strconv.FormatUint(h.InvSeq, 10))
			sequence := transport.CommitSignal(message)
			if progress, e := store.BindNextSignal(ctx, h.Type, h.ID, h.InvSeq, sequence, transport.SignalTransport); e != nil || !progress {
				t.Fatal(progress, e)
			}
			state, err := store.InspectStart(ctx, h.Type, h.ID)
			if err != nil {
				t.Fatal(err)
			}
			started, _ := json.Marshal(map[string]any{"input_sha256": state.State.Start.InputSHA256})
			tail, err := store.Begin(ctx, h.Type, h.ID, h.InvSeq)
			if err != nil {
				t.Fatal(err)
			}
			tail, err = store.Append(ctx, h.Type, h.ID, h.InvSeq, journal.Entry{Kind: journal.Started, Payload: started}, tail, [][]byte{[]byte(`7`)}, nil)
			if err != nil {
				t.Fatal(err)
			}
			event, _ := json.Marshal(map[string]any{"sig_seq": sequence, "name": "go", "ref": "graph-signal-" + input.InputSHA256, "hash": input.InputSHA256, "canonical_signal": map[string]any{"index": 0, "token": input.Token}})
			kind := journal.SignalConsumed
			if mode == "rejected" {
				kind = journal.Failed
				event, _ = json.Marshal(wf.Outcome{InvSeq: h.InvSeq, Error: journal.ErrTooLong.Error(), LimitEntry: &wf.LimitEntry{Kind: string(journal.SignalConsumed), Payload: event}})
			}
			if mode == "forged-terminal" {
				kind = journal.Completed
				event, _ = json.Marshal(wf.Outcome{InvSeq: h.InvSeq + 1, Result: []byte(`true`)})
			}
			if _, err = store.Append(ctx, h.Type, h.ID, h.InvSeq, journal.Entry{Index: 1, Kind: kind, Payload: event}, tail, [][]byte{[]byte(`true`)}, nil); err != nil {
				t.Fatal(err)
			}
			transport.PurgeSignal(sequence)
			source, err := transport.LastInvocation(ctx, identity.InvocationSubject(h.Type, h.ID))
			if err != nil {
				t.Fatal(err)
			}
			if mode == "forged-source" {
				source.Header.Set(journal.GraphStartTokenHeader, "forged")
			}
			counter.hash = input.InputSHA256
			counter.reads = 0
			counter.deny = mode == "unreadable"
			port.denyRelease = mode == "release-unknown"
			snapshot, err := worker.ReadGraphReplaySnapshot(ctx, store, h.Type, h.ID, source)
			if mode == "unreadable" || mode == "forged-source" || mode == "release-unknown" || mode == "forged-terminal" {
				if err == nil || snapshot.Format != "" || snapshot.InputHash != "" || snapshot.Input != nil || snapshot.Records != nil || snapshot.Objects != nil {
					t.Fatal("uncertain/forged export leaked successful snapshot", err)
				}
				if mode == "unreadable" && !errors.Is(err, context.DeadlineExceeded) {
					t.Fatal(err)
				}
				return
			}
			if err != nil || snapshot.Format != wf.ReplayFormatGraphV1 || string(snapshot.Input) != "7" || len(snapshot.Records) != 2 || string(snapshot.Objects["graph-signal-"+input.InputSHA256]) != "true" || counter.reads != 1 {
				t.Fatal("owned snapshot mismatch", err, counter.reads)
			}
			if mode == "rejected" {
				if p := snapshot.PendingSignal; p == nil || p.Sequence != sequence || p.Name != "go" || p.Hash != input.InputSHA256 || p.Ref != "graph-signal-"+input.InputSHA256 {
					t.Fatal("missing rejected canonical drain", p)
				}
			} else if snapshot.PendingSignal != nil {
				t.Fatal("invented pending drain")
			}
			t.Log(fmt.Sprintf("mode=%s source deleted; owned body reads=%d", mode, counter.reads))
		})
	}
}
