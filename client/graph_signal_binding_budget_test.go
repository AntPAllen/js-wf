package client_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"js-wf/client"
	"js-wf/internal/retainedgraph"
	"js-wf/journal"
	"js-wf/sim"
)

// This cost model isolates the aggregate binding window. It does not model
// Raft, physical chunk timings or the precise cancellation point of the native
// PostgreSQL CLI failure. All underlying transport reads succeed; the wrapper
// projects a deterministic owned-body read cost onto the production deadline.
type signalBindingCostPort struct {
	*sim.GraphPublicationTransport
	schedule *sim.Scheduler
	hash     string
	mode     string
	armed    bool
	fired    bool
}

func (p *signalBindingCostPort) Get(ctx context.Context, link retainedgraph.Link, limit int) ([]byte, error) {
	data, err := p.GraphPublicationTransport.Get(ctx, link, limit)
	if err != nil || !p.armed || p.fired || link.Hash != p.hash {
		return data, err
	}
	p.fired = true
	if p.mode == "real-deadline" {
		<-ctx.Done()
		p.schedule.RecordTransport(sim.TransportEvent{Operation: "binding_real_deadline", Outcome: "deadline", AtMillis: p.schedule.NowMillis()})
		return nil, ctx.Err()
	}
	cost := int64(2800)
	if p.mode == "over-budget" {
		cost = 3200
	}
	if err := p.schedule.AdvanceMillis(cost); err != nil {
		return nil, err
	}
	p.schedule.RecordTransport(sim.TransportEvent{Operation: "binding_body_cost", Sequence: uint64(cost), Outcome: p.mode, AtMillis: p.schedule.NowMillis()})
	if cost > 3000 {
		return nil, context.DeadlineExceeded
	}
	if p.mode == "corrupt" {
		data[0] ^= 1
	}
	return data, nil
}

type signalBindingCostSource struct {
	*sim.SignalTransport
	port *signalBindingCostPort
}

func (p *signalBindingCostSource) NextSignal(ctx context.Context, from uint64, subject string) (*jetstream.RawStreamMsg, error) {
	if !p.port.fired {
		deadline, ok := ctx.Deadline()
		if !ok || time.Until(deadline) > 3*time.Second {
			return nil, fmt.Errorf("production binding deadline is missing or exceeds three seconds")
		}
		p.port.armed = true
	}
	return p.SignalTransport.NextSignal(ctx, from, subject)
}

func runSignalBindingBudget(seed int64, mode string, replay *sim.Trace) (trace sim.Trace, runErr error) {
	schedule := sim.NewScheduler(seed)
	if replay != nil {
		var err error
		schedule, err = sim.ReplayScheduler(*replay)
		if err != nil {
			return trace, err
		}
	}
	if err := schedule.SetWorkload("canonical_signal_binding_budget_" + mode); err != nil {
		return trace, err
	}
	defer func() { trace = schedule.Trace() }()
	ctx := context.Background()
	body := []byte("owned input whose aggregate read cost is modeled independently of its size")
	hash := sha256.Sum256(body)
	model := sim.NewGraphPublicationTransport(schedule)
	port := &signalBindingCostPort{GraphPublicationTransport: model, schedule: schedule, hash: hex.EncodeToString(hash[:]), mode: mode}
	protocol := model.Protocol()
	protocol.Port = port
	store, err := journal.NewGraphStore(journal.GraphConfig{Protocol: protocol, CanonicalStarts: true, CanonicalSignals: true, PinTTL: time.Hour, Now: func() time.Time {
		return time.Unix(1000, 0).Add(time.Duration(schedule.NowMillis()) * time.Millisecond)
	}})
	if err != nil {
		return trace, err
	}
	source := &signalBindingCostSource{SignalTransport: sim.NewSignalTransport(schedule), port: port}
	c, err := client.NewWithSignalPorts(source, source).WithGraphJournal(store)
	if err != nil {
		return trace, err
	}
	h, err := c.Start(ctx, "binding-budget", "flow", []byte(`7`))
	if err != nil {
		return trace, err
	}
	sequence, signalErr := c.Signal(ctx, h.Type, h.ID, "go", body, "key")
	if !port.fired || (mode == "fast" && signalErr != nil) || (mode != "fast" && !errors.Is(signalErr, client.ErrSignalUnknown)) {
		return trace, fmt.Errorf("binding cut not reached or wrong result: mode=%s fired=%v err=%v", mode, port.fired, signalErr)
	}
	if (mode == "over-budget" || mode == "real-deadline") && !errors.Is(signalErr, context.DeadlineExceeded) {
		return trace, fmt.Errorf("binding deadline identity lost: %v", signalErr)
	}
	r := journal.GraphSignalRequest{Type: h.Type, ID: h.ID, Invocation: h.InvSeq, Name: "go", Key: "key"}
	input, owned, found, err := store.ReadSignalInput(ctx, r)
	if err != nil || !found || !reflect.DeepEqual(owned, body) {
		return trace, fmt.Errorf("reservation not recoverable: found=%v err=%v", found, err)
	}
	_, _, bound, err := store.ReadSignalBinding(ctx, r)
	if err != nil || bound != (mode == "fast") || len(source.SignalFor(h.Type, h.ID, "go")) != 1 || len(source.Runs()) != 1+boolSignalBound(bound) {
		return trace, fmt.Errorf("unknown/corrupt binding published or source duplicated: bound=%v err=%v", bound, err)
	}
	// A subsequent call heals the modeled read cost. It uses the SAME durable
	// reservation and source identity; unknown outcome never means republish a
	// different signal or enqueue an unbound input.
	recovered, err := c.RecoverSignal(ctx, r, input.Token)
	if err != nil || recovered == 0 || sequence != 0 && sequence != recovered {
		return trace, fmt.Errorf("same-identity recovery failed: %d %d %v", sequence, recovered, err)
	}
	binding, data, found, err := store.ReadSignalBinding(ctx, r)
	if err != nil || !found || binding.Input != input || binding.Sequence != recovered || !reflect.DeepEqual(data, body) || len(source.SignalFor(h.Type, h.ID, "go")) != 1 || len(source.Runs()) != 2 {
		return trace, fmt.Errorf("recovery changed input/order or duplicated delivery: %v", err)
	}
	if err := model.CheckReferences(); err != nil {
		return trace, err
	}
	schedule.RecordTransport(sim.TransportEvent{Operation: "check_binding_budget_recovery", Sequence: recovered, Outcome: "one source, one bound input, one wakeup", AtMillis: schedule.NowMillis()})
	return trace, schedule.Finish()
}

func boolSignalBound(bound bool) int {
	if bound {
		return 1
	}
	return 0
}

func TestGraphCanonicalSignalBindingBudgetRecovery(t *testing.T) {
	for _, mode := range []string{"fast", "over-budget", "corrupt"} {
		t.Run(mode, func(t *testing.T) {
			for seed := int64(1); seed <= 16; seed++ {
				generated, err := runSignalBindingBudget(seed, mode, nil)
				if err != nil {
					t.Fatalf("seed=%d: %v", seed, err)
				}
				replayed, err := runSignalBindingBudget(seed, mode, &generated)
				if err != nil || !reflect.DeepEqual(generated, replayed) {
					t.Fatalf("seed=%d exact replay differs: %v", seed, err)
				}
			}
		})
	}
}

// Exercise the actual production context timer once. The generated/replayed
// seed matrix above stays entirely virtual and needs no wall-clock sleeps.
func TestGraphCanonicalSignalBindingActualDeadlineRecovery(t *testing.T) {
	if _, err := runSignalBindingBudget(1, "real-deadline", nil); err != nil {
		t.Fatal(err)
	}
}
