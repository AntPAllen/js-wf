package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"js-wf/client"
	"js-wf/identity"
	"js-wf/retention"
	"js-wf/sim"
	"js-wf/testcluster"
	"js-wf/worker"

	"github.com/nats-io/nats.go/jetstream"
)

type outcomeContractReport struct {
	NormalIdempotent bool
	ChangedRejected  bool
	OldPurged        bool
	OldKept          bool
	NewReplaced      bool
	NewIdempotent    bool
	RevisionAdvanced bool
}

func runOutcomeContract(ctx context.Context, port worker.OutcomePort) (outcomeContractReport, error) {
	var report outcomeContractReport
	const typ = "outcome-contract"
	normal := []byte(`{"inv_seq":2,"result":"ok"}`)
	changed := []byte(`{"inv_seq":2,"result":"changed"}`)
	if err := worker.PersistOutcomeWithPort(ctx, port, typ, "normal", 2, normal); err != nil {
		return report, err
	}
	if err := worker.PersistOutcomeWithPort(ctx, port, typ, "normal", 2, normal); err != nil {
		return report, err
	}
	entry, err := port.Get(ctx, identity.Key(typ, "normal"))
	if err != nil {
		return report, err
	}
	report.NormalIdempotent = bytes.Equal(entry.Value, normal)
	err = worker.PersistOutcomeWithPort(ctx, port, typ, "normal", 2, changed)
	report.ChangedRejected = err != nil && strings.Contains(err.Error(), "terminal result changed")
	if !report.ChangedRejected {
		return report, fmt.Errorf("changed terminal outcome: %v", err)
	}
	marker, err := json.Marshal(retention.Tombstone{Tombstone: true, InvSeq: 3, PurgedAt: time.Unix(1_700_000_000, 0).UTC(), ExpiresAt: time.Unix(1_700_000_100, 0).UTC()})
	if err != nil {
		return report, err
	}
	oldKey := identity.Key(typ, "old")
	if _, err := port.Create(ctx, oldKey, marker); err != nil {
		return report, err
	}
	oldResult := []byte(`{"inv_seq":3,"result":"late"}`)
	report.OldPurged = errors.Is(worker.PersistOutcomeWithPort(ctx, port, typ, "old", 3, oldResult), client.ErrPurged)
	old, err := port.Get(ctx, oldKey)
	if err != nil {
		return report, err
	}
	report.OldKept = bytes.Equal(old.Value, marker)
	newKey := identity.Key(typ, "new")
	before, err := port.Create(ctx, newKey, marker)
	if err != nil {
		return report, err
	}
	newResult := []byte(`{"inv_seq":4,"result":"new"}`)
	if err := worker.PersistOutcomeWithPort(ctx, port, typ, "new", 4, newResult); err != nil {
		return report, err
	}
	newEntry, err := port.Get(ctx, newKey)
	if err != nil {
		return report, err
	}
	report.NewReplaced = bytes.Equal(newEntry.Value, newResult)
	report.RevisionAdvanced = newEntry.Revision > before
	if err := worker.PersistOutcomeWithPort(ctx, port, typ, "new", 4, newResult); err != nil {
		return report, err
	}
	newEntry, err = port.Get(ctx, newKey)
	if err != nil {
		return report, err
	}
	report.NewIdempotent = bytes.Equal(newEntry.Value, newResult)
	return report, nil
}

func TestOutcomePersistenceMatchesModeledKV(t *testing.T) {
	all, _ := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	state, err := all[0].KeyValue(ctx, "WF_STATE")
	if err != nil {
		t.Fatal(err)
	}
	model := sim.NewKVTransport(sim.NewScheduler(42), 0)
	modeled, err := runOutcomeContract(ctx, model)
	if err != nil {
		t.Fatalf("model: %v", err)
	}
	real, err := runOutcomeContract(ctx, worker.NewOutcomePort(state))
	if err != nil {
		t.Fatalf("three-node KV: %v", err)
	}
	if !reflect.DeepEqual(modeled, real) || real != (outcomeContractReport{true, true, true, true, true, true, true}) {
		t.Fatalf("outcome contract model=%+v real=%+v", modeled, real)
	}
}

type holdOutcomeCreatePort struct {
	worker.OutcomePort
	proxy *testcluster.ClientProxy
	held  chan struct{}
}

func (p *holdOutcomeCreatePort) Create(ctx context.Context, key string, payload []byte) (uint64, error) {
	p.proxy.HoldResponses()
	close(p.held)
	return p.OutcomePort.Create(ctx, key, payload)
}

type holdOutcomeUpdatePort struct {
	worker.OutcomePort
	proxy *testcluster.ClientProxy
	held  chan struct{}
}

func (p *holdOutcomeUpdatePort) Update(ctx context.Context, key string, payload []byte, revision uint64) (uint64, error) {
	p.proxy.HoldResponses()
	close(p.held)
	return p.OutcomePort.Update(ctx, key, payload, revision)
}

type modelOutcomePostUpdateReadLoss struct {
	worker.OutcomePort
	model *sim.KVTransport
}

func (p modelOutcomePostUpdateReadLoss) Update(ctx context.Context, key string, payload []byte, revision uint64) (uint64, error) {
	sequence, err := p.OutcomePort.Update(ctx, key, payload, revision)
	if errors.Is(err, sim.ErrTransportLost) {
		if faultErr := p.model.QueueFault(sim.KVFault{Operation: "get", Kind: sim.KVGetTransportLost}); faultErr != nil {
			return sequence, faultErr
		}
	}
	return sequence, err
}

func TestOutcomeLostCreateAckRetryMatchesModel(t *testing.T) {
	all, cluster := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	proxy, err := testcluster.NewClientProxy(cluster.Servers[0].ClientURL())
	if err != nil {
		t.Fatal(err)
	}
	defer proxy.Close()
	nc, err := proxy.Connect()
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	proxied, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	proxiedKV, err := proxied.KeyValue(ctx, "WF_STATE")
	if err != nil {
		t.Fatal(err)
	}
	port := &holdOutcomeCreatePort{OutcomePort: worker.NewOutcomePort(proxiedKV), proxy: proxy, held: make(chan struct{})}
	const typ, id = "outcome-contract", "lost-create-ack"
	key := identity.Key(typ, id)
	payload := []byte(`{"inv_seq":7,"result":"done"}`)
	attemptCtx, stopAttempt := context.WithTimeout(ctx, 3*time.Second)
	defer stopAttempt()
	first := make(chan error, 1)
	go func() { first <- worker.PersistOutcomeWithPort(attemptCtx, port, typ, id, 7, payload) }()
	select {
	case <-port.held:
	case <-ctx.Done():
		t.Fatal("outcome create was not attempted")
	}
	observer, err := all[1].KeyValue(ctx, "WF_STATE")
	if err != nil {
		t.Fatal(err)
	}
	var committed jetstream.KeyValueEntry
	for ctx.Err() == nil {
		committed, err = observer.Get(ctx, key)
		if err == nil && bytes.Equal(committed.Value(), payload) {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if ctx.Err() != nil {
		t.Fatalf("held outcome create did not commit: %v", err)
	}
	proxy.Block()
	select {
	case err := <-first:
		if err == nil {
			t.Fatal("lost create acknowledgment unexpectedly succeeded")
		}
	case <-ctx.Done():
		t.Fatal("lost create did not return")
	}
	proxy.Heal()
	healthyKV, err := all[2].KeyValue(ctx, "WF_STATE")
	if err != nil {
		t.Fatal(err)
	}
	if err := worker.PersistOutcomeWithPort(ctx, worker.NewOutcomePort(healthyKV), typ, id, 7, payload); err != nil {
		t.Fatalf("real retry: %v", err)
	}
	current, err := observer.Get(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	if current.Revision() != committed.Revision() || !bytes.Equal(current.Value(), payload) {
		t.Fatalf("real retained result revision=%d/%d value=%s", current.Revision(), committed.Revision(), current.Value())
	}
	model := sim.NewKVTransport(sim.NewScheduler(42), 0)
	if err := model.QueueFault(sim.KVFault{Operation: "create", Kind: sim.KVLoseAckAfterCommit}); err != nil {
		t.Fatal(err)
	}
	if err := worker.PersistOutcomeWithPort(ctx, model, typ, id, 7, payload); !errors.Is(err, sim.ErrTransportLost) {
		t.Fatalf("model lost create acknowledgment: %v", err)
	}
	before, err := model.Get(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	if err := worker.PersistOutcomeWithPort(ctx, model, typ, id, 7, payload); err != nil {
		t.Fatalf("model retry: %v", err)
	}
	after, err := model.Get(ctx, key)
	if err != nil || after.Revision != before.Revision || !bytes.Equal(after.Value, payload) {
		t.Fatalf("model retained result revision=%d/%d err=%v", after.Revision, before.Revision, err)
	}
}

func TestOutcomeLostUpdateAckAndReadRetryMatchesModel(t *testing.T) {
	all, cluster := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	const typ, id = "outcome-contract", "lost-update-ack"
	key := identity.Key(typ, id)
	marker, err := json.Marshal(retention.Tombstone{Tombstone: true, InvSeq: 6, PurgedAt: time.Unix(1_700_000_000, 0).UTC(), ExpiresAt: time.Unix(1_700_000_100, 0).UTC()})
	if err != nil {
		t.Fatal(err)
	}
	newResult := []byte(`{"inv_seq":7,"result":"done"}`)
	creator, err := all[1].KeyValue(ctx, "WF_STATE")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := creator.Create(ctx, key, marker); err != nil {
		t.Fatal(err)
	}
	proxy, err := testcluster.NewClientProxy(cluster.Servers[0].ClientURL())
	if err != nil {
		t.Fatal(err)
	}
	defer proxy.Close()
	nc, err := proxy.Connect()
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	proxied, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	proxiedKV, err := proxied.KeyValue(ctx, "WF_STATE")
	if err != nil {
		t.Fatal(err)
	}
	port := &holdOutcomeUpdatePort{OutcomePort: worker.NewOutcomePort(proxiedKV), proxy: proxy, held: make(chan struct{})}
	attemptCtx, stopAttempt := context.WithTimeout(ctx, 3*time.Second)
	defer stopAttempt()
	first := make(chan error, 1)
	go func() { first <- worker.PersistOutcomeWithPort(attemptCtx, port, typ, id, 7, newResult) }()
	select {
	case <-port.held:
	case <-ctx.Done():
		t.Fatal("outcome update was not attempted")
	}
	var committed jetstream.KeyValueEntry
	for ctx.Err() == nil {
		committed, err = creator.Get(ctx, key)
		if err == nil && bytes.Equal(committed.Value(), newResult) {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if ctx.Err() != nil {
		t.Fatalf("held outcome update did not commit: %v", err)
	}
	proxy.Block()
	var firstErr error
	select {
	case firstErr = <-first:
	case <-ctx.Done():
		t.Fatal("lost update did not return")
	}
	proxy.Heal()
	healthyKV, err := all[2].KeyValue(ctx, "WF_STATE")
	if err != nil {
		t.Fatal(err)
	}
	if err := worker.PersistOutcomeWithPort(ctx, worker.NewOutcomePort(healthyKV), typ, id, 7, newResult); err != nil {
		t.Fatalf("real retry: %v", err)
	}
	current, err := creator.Get(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	if current.Revision() != committed.Revision() || !bytes.Equal(current.Value(), newResult) {
		t.Fatalf("real retained result revision=%d/%d value=%s", current.Revision(), committed.Revision(), current.Value())
	}
	model := sim.NewKVTransport(sim.NewScheduler(42), 0)
	if _, err := model.Create(ctx, key, marker); err != nil {
		t.Fatal(err)
	}
	if err := model.QueueFault(sim.KVFault{Operation: "update", Kind: sim.KVLoseAckAfterCommit}); err != nil {
		t.Fatal(err)
	}
	var modeledPort worker.OutcomePort = model
	if firstErr != nil {
		modeledPort = modelOutcomePostUpdateReadLoss{OutcomePort: model, model: model}
	}
	modeledErr := worker.PersistOutcomeWithPort(ctx, modeledPort, typ, id, 7, newResult)
	if (firstErr == nil) != (modeledErr == nil) {
		t.Fatalf("first outcome real=%v model=%v", firstErr, modeledErr)
	}
	before, err := model.Get(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	if err := worker.PersistOutcomeWithPort(ctx, model, typ, id, 7, newResult); err != nil {
		t.Fatalf("model retry: %v", err)
	}
	after, err := model.Get(ctx, key)
	if err != nil || after.Revision != before.Revision || !bytes.Equal(after.Value, newResult) {
		t.Fatalf("model retained result revision=%d/%d err=%v", after.Revision, before.Revision, err)
	}
}

var _ worker.OutcomePort = (*holdOutcomeCreatePort)(nil)
var _ worker.OutcomePort = (*holdOutcomeUpdatePort)(nil)
var _ worker.OutcomePort = (modelOutcomePostUpdateReadLoss{})
