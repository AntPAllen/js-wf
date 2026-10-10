package sim

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"js-wf/client"
	"js-wf/journal"
	"js-wf/lease"
	"js-wf/wf"
	"js-wf/worker"
)

var graphCancelPollModes = []string{"absent", "reserved", "bound_source_purged", "foreign_key", "foreign_generation", "root_unknown", "owned_read_unknown"}

func runGraphCancelPoll(seed int64, replay *Trace) (trace Trace, runErr error) {
	schedule := NewScheduler(seed)
	if replay != nil {
		var err error
		schedule, err = ReplayScheduler(*replay)
		if err != nil {
			return trace, err
		}
	}
	if err := schedule.SetWorkload("graph_running_cancel_poll"); err != nil {
		return trace, err
	}
	defer func() { trace = schedule.Trace() }()
	mode, err := schedule.Choose(graphCancelPollModes)
	if err != nil {
		return trace, err
	}
	ctx := context.Background()
	model := NewGraphPublicationTransport(schedule)
	now := time.Unix(1000, 0)
	graph, err := journal.NewGraphStore(journal.GraphConfig{Protocol: model.Protocol(), CanonicalStarts: true, CanonicalSignals: true, Now: func() time.Time { return now }})
	if err != nil {
		return trace, err
	}
	transport := NewWorkerTransport(schedule, 3*time.Second)
	c, err := client.NewWithSignalPorts(transport.SignalTransport, transport.SignalTransport).WithGraphJournal(graph)
	if err != nil {
		return trace, err
	}
	h, err := c.Start(ctx, "test", "cancelpoll0001", []byte(`7`))
	if err != nil {
		return trace, err
	}
	legacy := NewJournalTransport(schedule)
	entered := false
	w, err := worker.NewWithPorts("cancel-poll", map[string]worker.Handler{h.Type: func(*wf.Context, json.RawMessage) (json.RawMessage, error) { entered = true; return nil, nil }}, worker.ModeledWorkerPorts{Journal: journal.NewWithPorts(legacy, legacy), Leases: lease.NewWithKVPort(NewKVTransport(schedule, 30*time.Second)), Outcome: NewKVTransport(schedule, 0), Invocation: transport.SignalTransport, Signals: transport.SignalTransport, Client: c, CancellationPoll: transport.SignalTransport}, worker.WithGraphJournal(graph))
	if err != nil {
		return trace, err
	}
	invocation := h.InvSeq
	if mode == "reserved" || mode == "foreign_key" {
		key := "cancel"
		if mode == "foreign_key" {
			key = "foreign"
		}
		input, err := graph.ReserveSignal(ctx, journal.GraphSignalRequest{Type: h.Type, ID: h.ID, Invocation: h.InvSeq, Name: client.CancelSignalName, Key: key}, nil, false)
		if err != nil {
			return trace, err
		}
		if mode == "foreign_key" {
			msg := &nats.Msg{Subject: "wf.sig." + h.Type + "." + h.ID + "." + client.CancelSignalName, Data: input.PointerBytes(), Header: nats.Header{}}
			msg.Header.Set(journal.GraphSignalTokenHeader, input.Token)
			msg.Header.Set("Wf-Input-SHA256", input.InputSHA256)
			msg.Header.Set("Wf-Inv-Seq", strconv.FormatUint(h.InvSeq, 10))
			sequence := transport.CommitSignal(msg)
			if progress, e := graph.BindNextSignal(ctx, h.Type, h.ID, h.InvSeq, sequence, transport.SignalTransport); e != nil || !progress {
				return trace, fmt.Errorf("foreign binding=%t err=%v", progress, e)
			}
			transport.PurgeSignal(sequence)
		}
	} else if mode != "absent" {
		sequence, err := c.Cancel(ctx, h.Type, h.ID)
		if err != nil {
			return trace, err
		}
		transport.PurgeSignal(sequence)
	}
	if mode == "foreign_generation" {
		invocation++
	}
	if mode == "root_unknown" {
		if err := model.QueueFault("read_root", DropBeforeCommit); err != nil {
			return trace, err
		}
	}
	if mode == "owned_read_unknown" {
		if err := model.QueueFault("get", DropBeforeCommit); err != nil {
			return trace, err
		}
	}
	found, err := w.PollRunningCancellation(ctx, h.Type, h.ID, invocation)
	wantFound := mode == "bound_source_purged"
	wantError := mode == "foreign_generation" || mode == "root_unknown" || mode == "owned_read_unknown"
	if found != wantFound || (err != nil) != wantError || entered {
		return trace, fmt.Errorf("mode=%s found=%t error=%v entered=%t", mode, found, err, entered)
	}
	// The same durable decision must recover after an uncertain read; no legacy
	// source exists to supply a fallback. Reservations and foreign keys stay absent.
	if mode == "root_unknown" || mode == "owned_read_unknown" {
		found, err = w.PollRunningCancellation(ctx, h.Type, h.ID, h.InvSeq)
		if err != nil || !found {
			return trace, fmt.Errorf("retry mode=%s found=%t err=%v", mode, found, err)
		}
	}
	schedule.RecordTransport(TransportEvent{Operation: "check_graph_running_cancel_poll", Outcome: mode})
	if err := schedule.Finish(); err != nil {
		return trace, err
	}
	return trace, nil
}

func TestSeededGraphRunningCancelPollReplay(t *testing.T) {
	observed := map[string]int{}
	for seed := range seededSchedules(t) {
		generated, err := runGraphCancelPoll(seed, nil)
		if err != nil {
			path, e := saveSeedFailureTrace(t.Name(), seed, generated)
			t.Fatalf("seed=%d trace=%s save=%v err=%v", seed, path, e, err)
		}
		replayed, err := runGraphCancelPoll(seed, &generated)
		if err != nil || !reflect.DeepEqual(generated, replayed) {
			t.Fatalf("seed=%d replay differs: %v", seed, err)
		}
		mode := generated.Decisions[0].Chosen
		observed[mode]++
		if dir := os.Getenv("SIM_GRAPH_CANCEL_POLL_ROOT"); dir != "" && observed[mode] == 1 {
			if err := os.MkdirAll(dir, 0755); err != nil {
				t.Fatal(err)
			}
			if err := generated.Save(filepath.Join(dir, mode+".json")); err != nil {
				t.Fatal(err)
			}
		}
	}
	if len(observed) != len(graphCancelPollModes) {
		t.Fatalf("coverage=%v", observed)
	}
	t.Logf("canonical cancellation poll modes=%v; production graph lookup, source removal, unknown read recovery and exact trace replay", observed)
}
