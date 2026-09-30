package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"js-wf/lease"
	"js-wf/sim"
	"js-wf/testcluster"

	"github.com/nats-io/nats.go/jetstream"
)

type holdLeaseDeleteJS struct {
	jetstream.JetStream
	proxy *testcluster.ClientProxy
	held  chan struct{}
	once  sync.Once
}

func (h *holdLeaseDeleteJS) KeyValue(ctx context.Context, bucket string) (jetstream.KeyValue, error) {
	kv, err := h.JetStream.KeyValue(ctx, bucket)
	if err != nil {
		return nil, err
	}
	return &holdLeaseDeleteKV{KeyValue: kv, owner: h}, nil
}

type holdLeaseDeleteKV struct {
	jetstream.KeyValue
	owner *holdLeaseDeleteJS
}

type holdLeaseCreateJS struct {
	jetstream.JetStream
	proxy *testcluster.ClientProxy
	held  chan struct{}
	once  sync.Once
}

func (h *holdLeaseCreateJS) KeyValue(ctx context.Context, bucket string) (jetstream.KeyValue, error) {
	kv, err := h.JetStream.KeyValue(ctx, bucket)
	if err != nil {
		return nil, err
	}
	return &holdLeaseCreateKV{KeyValue: kv, owner: h}, nil
}

type holdLeaseCreateKV struct {
	jetstream.KeyValue
	owner *holdLeaseCreateJS
}

type holdLeaseRenewKV struct {
	jetstream.KeyValue
	proxy     *testcluster.ClientProxy
	held      chan struct{}
	mu        sync.Mutex
	count     int
	triggerAt int
}

type realLeaseClockPort struct{ kv jetstream.KeyValue }

func (p realLeaseClockPort) Create(ctx context.Context, key string, value []byte) (uint64, error) {
	return p.kv.Create(ctx, key, value)
}
func (p realLeaseClockPort) Get(ctx context.Context, key string) (lease.KVEntry, error) {
	entry, err := p.kv.Get(ctx, key)
	if err != nil {
		return lease.KVEntry{}, err
	}
	return lease.KVEntry{Value: entry.Value(), Revision: entry.Revision(), Created: entry.Created()}, nil
}
func (p realLeaseClockPort) Update(ctx context.Context, key string, value []byte, revision uint64) (uint64, error) {
	return p.kv.Update(ctx, key, value, revision)
}
func (p realLeaseClockPort) Delete(ctx context.Context, key string, revision uint64) error {
	return p.kv.Delete(ctx, key, jetstream.LastRevision(revision))
}
func (realLeaseClockPort) Now() time.Time { return time.Now() }

type offsetLeaseClockPort struct {
	lease.KVPort
	offset time.Duration
}

func (p offsetLeaseClockPort) Now() time.Time { return p.KVPort.Now().Add(p.offset) }

func TestSimLeaseClockSkewReclaimContractAgainstRealCluster(t *testing.T) {
	all, _ := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	kv, err := all[0].KeyValue(ctx, "WF_LEASE")
	if err != nil {
		t.Fatal(err)
	}
	model := sim.NewKVTransport(sim.NewScheduler(17), 30*time.Second)
	const typ, id = "lease-contract", "skewed-orphan"
	key := typ + "." + id
	orphan, _ := json.Marshal(lease.Value{Worker: "crashed"})
	realOld, err := kv.Create(ctx, key, orphan)
	if err != nil {
		t.Fatal(err)
	}
	modelOld, err := model.Create(ctx, key, orphan)
	if err != nil {
		t.Fatal(err)
	}
	realPort := realLeaseClockPort{kv: kv}
	for _, tc := range []struct {
		name   string
		offset time.Duration
		held   bool
	}{
		{name: "slow", offset: -2 * time.Second, held: true},
		{name: "fast", offset: 2 * time.Second},
	} {
		realStore := lease.NewWithKVPort(offsetLeaseClockPort{KVPort: realPort, offset: tc.offset})
		modelStore := lease.NewWithKVPort(offsetLeaseClockPort{KVPort: model, offset: tc.offset})
		realLease, realErr := realStore.Acquire(ctx, typ, id, tc.name)
		modelLease, modelErr := modelStore.Acquire(ctx, typ, id, tc.name)
		if tc.held {
			if !errors.Is(realErr, lease.ErrHeld) || !errors.Is(modelErr, lease.ErrHeld) {
				t.Fatalf("%s early reclaim: real=%v model=%v", tc.name, realErr, modelErr)
			}
			continue
		}
		if realErr != nil || modelErr != nil || realLease.Epoch() <= realOld || modelLease.Epoch() <= modelOld {
			t.Fatalf("%s reclaim: real_epoch=%v real_err=%v model_epoch=%v model_err=%v old=%d/%d", tc.name, realLease, realErr, modelLease, modelErr, realOld, modelOld)
		}
	}
	for name, port := range map[string]lease.KVPort{"real": realPort, "model": model} {
		entry, err := port.Get(ctx, key)
		if err != nil {
			t.Fatalf("%s retained lease: %v", name, err)
		}
		var value lease.Value
		if err := json.Unmarshal(entry.Value, &value); err != nil || value.Worker != "fast" || value.Epoch == 0 || entry.Revision <= value.Epoch {
			t.Fatalf("%s retained lease=%+v revision=%d err=%v", name, value, entry.Revision, err)
		}
	}
}

func (h *holdLeaseRenewKV) Update(ctx context.Context, key string, value []byte, revision uint64) (uint64, error) {
	h.mu.Lock()
	h.count++
	triggerAt := h.triggerAt
	if triggerAt == 0 {
		triggerAt = 2
	}
	hit := h.count == triggerAt
	h.mu.Unlock()
	if hit {
		h.proxy.HoldResponses()
		close(h.held)
	}
	return h.KeyValue.Update(ctx, key, value, revision)
}

func (h *holdLeaseCreateKV) Create(ctx context.Context, key string, value []byte, opts ...jetstream.KVCreateOpt) (uint64, error) {
	h.owner.once.Do(func() {
		h.owner.proxy.HoldResponses()
		close(h.owner.held)
	})
	return h.KeyValue.Create(ctx, key, value, opts...)
}

func (h *holdLeaseDeleteKV) Delete(ctx context.Context, key string, opts ...jetstream.KVDeleteOpt) error {
	h.owner.once.Do(func() {
		h.owner.proxy.HoldResponses()
		close(h.owner.held)
	})
	return h.KeyValue.Delete(ctx, key, opts...)
}

func TestSimLeaseRevisionContractAgainstRealCluster(t *testing.T) {
	all, _ := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	real, err := lease.New(ctx, all[0])
	if err != nil {
		t.Fatal(err)
	}
	model := sim.NewKVTransport(sim.NewScheduler(1), 30*time.Second)
	simulated := lease.NewWithKVPort(model)
	const typ, id = "lease-contract", "same-id"
	realFirst, realErr := real.Acquire(ctx, typ, id, "first")
	modelFirst, modelErr := simulated.Acquire(ctx, typ, id, "first")
	if realErr != nil || modelErr != nil {
		t.Fatalf("first acquisition: real=%v model=%v", realErr, modelErr)
	}
	if realFirst.Epoch() != modelFirst.Epoch() {
		t.Fatalf("first epoch: real=%d err=%v model=%d err=%v", realFirst.Epoch(), realErr, modelFirst.Epoch(), modelErr)
	}
	if _, err := real.Acquire(ctx, typ, id, "second"); !errors.Is(err, lease.ErrHeld) {
		t.Fatalf("real held lease: %v", err)
	}
	if _, err := simulated.Acquire(ctx, typ, id, "second"); !errors.Is(err, lease.ErrHeld) {
		t.Fatalf("modeled held lease: %v", err)
	}
	if err := realFirst.Renew(ctx); err != nil {
		t.Fatal(err)
	}
	if err := modelFirst.Renew(ctx); err != nil {
		t.Fatal(err)
	}
	if err := realFirst.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if err := modelFirst.Release(ctx); err != nil {
		t.Fatal(err)
	}
	realNext, realErr := real.Acquire(ctx, typ, id, "second")
	modelNext, modelErr := simulated.Acquire(ctx, typ, id, "second")
	if realErr != nil || modelErr != nil {
		t.Fatalf("successor acquisition: real=%v model=%v", realErr, modelErr)
	}
	if realNext.Epoch() <= realFirst.Epoch() || modelNext.Epoch() <= modelFirst.Epoch() {
		t.Fatalf("successor epoch: real=%d err=%v model=%d err=%v", realNext.Epoch(), realErr, modelNext.Epoch(), modelErr)
	}
	if err := realFirst.Cleanup(ctx); !errors.Is(err, lease.ErrLost) {
		t.Fatalf("real stale cleanup: %v", err)
	}
	if err := modelFirst.Cleanup(ctx); !errors.Is(err, lease.ErrLost) {
		t.Fatalf("modeled stale cleanup: %v", err)
	}
	kv, err := all[1].KeyValue(ctx, "WF_LEASE")
	if err != nil {
		t.Fatal(err)
	}
	realEntry, err := kv.Get(ctx, typ+"."+id)
	if err != nil {
		t.Fatal(err)
	}
	modelEntry, err := model.Get(ctx, typ+"."+id)
	if err != nil {
		t.Fatal(err)
	}
	var realValue, modelValue lease.Value
	if json.Unmarshal(realEntry.Value(), &realValue) != nil || json.Unmarshal(modelEntry.Value, &modelValue) != nil || realValue.Worker != "second" || modelValue.Worker != "second" || realValue.Epoch != realNext.Epoch() || modelValue.Epoch != modelNext.Epoch() {
		t.Fatalf("retained lease: real=%+v model=%+v", realValue, modelValue)
	}
	if _, err := kv.Update(ctx, typ+"."+id, []byte(`other`), realFirst.Epoch()); !errors.Is(err, jetstream.ErrKeyRevisionMismatch) {
		t.Fatalf("real stale update: %v", err)
	}
	if _, err := model.Update(ctx, typ+"."+id, []byte(`other`), modelFirst.Epoch()); !errors.Is(err, jetstream.ErrKeyRevisionMismatch) {
		t.Fatalf("modeled stale update: %v", err)
	}
}

func TestLeaseNetworkLostReleaseAckPreservesSuccessor(t *testing.T) {
	all, cluster := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
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
	wrapped := &holdLeaseDeleteJS{JetStream: proxied, proxy: proxy, held: make(chan struct{})}
	oldStore, err := lease.New(ctx, wrapped)
	if err != nil {
		t.Fatal(err)
	}
	const typ, id = "lease-contract", "lost-release-ack"
	old, err := oldStore.Acquire(ctx, typ, id, "old")
	if err != nil {
		t.Fatal(err)
	}
	releaseCtx, stopRelease := context.WithTimeout(ctx, 3*time.Second)
	defer stopRelease()
	released := make(chan error, 1)
	go func() { released <- old.Release(releaseCtx) }()
	select {
	case <-wrapped.held:
	case <-ctx.Done():
		t.Fatal("lease deletion was not attempted")
	}
	observer, err := all[1].KeyValue(ctx, "WF_LEASE")
	if err != nil {
		t.Fatal(err)
	}
	for ctx.Err() == nil {
		_, err := observer.Get(ctx, typ+"."+id)
		if errors.Is(err, jetstream.ErrKeyNotFound) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	if ctx.Err() != nil {
		t.Fatal("held release did not commit")
	}
	proxy.Block()
	select {
	case err := <-released:
		if !errors.Is(err, lease.ErrLost) {
			t.Fatalf("lost release acknowledgment: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("release did not return after connection cut")
	}
	proxy.Heal()
	successorStore, err := lease.New(ctx, all[2])
	if err != nil {
		t.Fatal(err)
	}
	successor, err := successorStore.Acquire(ctx, typ, id, "successor")
	if err != nil {
		t.Fatalf("successor acquisition: %v", err)
	}
	if successor.Epoch() <= old.Epoch() {
		t.Fatalf("successor acquisition: lease=%+v err=%v", successor, err)
	}
	if err := old.Cleanup(ctx); !errors.Is(err, lease.ErrLost) {
		t.Fatalf("cleanup after lost release ack: %v", err)
	}
	entry, err := observer.Get(ctx, typ+"."+id)
	if err != nil {
		t.Fatal(err)
	}
	var value lease.Value
	if err := json.Unmarshal(entry.Value(), &value); err != nil || value.Worker != "successor" || value.Epoch != successor.Epoch() {
		t.Fatalf("successor changed after cleanup: value=%+v err=%v", value, err)
	}
}

func TestSimLeaseExpiryContractAgainstRealCluster(t *testing.T) {
	all, _ := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	const ttl = 2 * time.Second
	kv, err := all[0].CreateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket: "WF_LEASE_SIM_TTL", History: 1, TTL: ttl,
		LimitMarkerTTL: time.Second, Storage: jetstream.FileStorage, Replicas: 3,
	})
	if err != nil {
		t.Fatal(err)
	}
	real := lease.NewWithKeyValue(kv)
	schedule := sim.NewScheduler(7)
	model := sim.NewKVTransport(schedule, ttl)
	simulated := lease.NewWithKVPort(model)
	const typ, id = "lease-contract", "ttl"
	realOld, err := real.Acquire(ctx, typ, id, "old")
	if err != nil {
		t.Fatal(err)
	}
	modelOld, err := simulated.Acquire(ctx, typ, id, "old")
	if err != nil {
		t.Fatal(err)
	}
	for ctx.Err() == nil {
		_, err := kv.Get(ctx, typ+"."+id)
		if errors.Is(err, jetstream.ErrKeyNotFound) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		time.Sleep(25 * time.Millisecond)
	}
	if ctx.Err() != nil {
		t.Fatal("real short-TTL lease did not expire")
	}
	if err := schedule.AdvanceMillis(ttl.Milliseconds() + 1); err != nil {
		t.Fatal(err)
	}
	realNext, realErr := real.Acquire(ctx, typ, id, "next")
	modelNext, modelErr := simulated.Acquire(ctx, typ, id, "next")
	if realErr != nil || modelErr != nil {
		t.Fatalf("successor after expiry: real=%v model=%v", realErr, modelErr)
	}
	if realNext.Epoch() <= realOld.Epoch() || modelNext.Epoch() <= modelOld.Epoch() {
		t.Fatalf("expiry did not advance fencing epoch: real %d→%d model %d→%d", realOld.Epoch(), realNext.Epoch(), modelOld.Epoch(), modelNext.Epoch())
	}
	if err := realOld.Renew(ctx); !errors.Is(err, lease.ErrLost) {
		t.Fatalf("real expired renewal: %v", err)
	}
	if err := modelOld.Renew(ctx); !errors.Is(err, lease.ErrLost) {
		t.Fatalf("modeled expired renewal: %v", err)
	}
	if err := realOld.Cleanup(ctx); !errors.Is(err, lease.ErrLost) {
		t.Fatalf("real expired cleanup: %v", err)
	}
	if err := modelOld.Cleanup(ctx); !errors.Is(err, lease.ErrLost) {
		t.Fatalf("modeled expired cleanup: %v", err)
	}
}

func TestLeaseExpiryRaceNormalizesCASConflicts(t *testing.T) {
	all, _ := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	const ttl = 2 * time.Second
	kv, err := all[0].CreateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket: "WF_LEASE_EXPIRY_RACE", History: 1, TTL: ttl,
		LimitMarkerTTL: time.Second, Storage: jetstream.FileStorage, Replicas: 3,
	})
	if err != nil {
		t.Fatal(err)
	}
	const typ, id = "lease-contract", "expiry-race"
	old, err := lease.NewWithKeyValue(kv).Acquire(ctx, typ, id, "old")
	if err != nil {
		t.Fatal(err)
	}
	for ctx.Err() == nil {
		_, err = kv.Get(ctx, typ+"."+id)
		if errors.Is(err, jetstream.ErrKeyNotFound) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		time.Sleep(25 * time.Millisecond)
	}
	if ctx.Err() != nil {
		t.Fatal("lease did not expire")
	}
	var peers [3]jetstream.KeyValue
	peers[0] = kv
	for i := 1; i < len(peers); i++ {
		peers[i], err = all[i].KeyValue(ctx, "WF_LEASE_EXPIRY_RACE")
		if err != nil {
			t.Fatal(err)
		}
	}
	const contenders = 32
	var started sync.WaitGroup
	var done sync.WaitGroup
	started.Add(contenders)
	done.Add(contenders)
	begin := make(chan struct{})
	results := make([]struct {
		epoch uint64
		err   error
	}, contenders)
	for i := range results {
		go func(i int) {
			defer done.Done()
			started.Done()
			<-begin
			winner, acquireErr := lease.NewWithKeyValue(peers[i%len(peers)]).Acquire(ctx, typ, id, fmt.Sprintf("contender-%02d", i))
			results[i].err = acquireErr
			if winner != nil {
				results[i].epoch = winner.Epoch()
			}
		}(i)
	}
	started.Wait()
	close(begin)
	done.Wait()
	var winners, held int
	for i, result := range results {
		switch {
		case result.err == nil && result.epoch > old.Epoch():
			winners++
		case errors.Is(result.err, lease.ErrHeld):
			held++
		default:
			t.Errorf("contender %d: epoch=%d err=%v", i, result.epoch, result.err)
		}
	}
	if winners != 1 || held != contenders-1 {
		t.Fatalf("expiry race: winners=%d held=%d want=1/%d", winners, held, contenders-1)
	}
}

func TestLeaseNetworkLostCreateAckReclaimsUninitializedEpoch(t *testing.T) {
	all, cluster := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
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
	wrapped := &holdLeaseCreateJS{JetStream: proxied, proxy: proxy, held: make(chan struct{})}
	oldStore, err := lease.New(ctx, wrapped)
	if err != nil {
		t.Fatal(err)
	}
	const typ, id = "lease-contract", "lost-create-ack"
	createCtx, stopCreate := context.WithTimeout(ctx, 3*time.Second)
	defer stopCreate()
	created := make(chan error, 1)
	go func() { _, err := oldStore.Acquire(createCtx, typ, id, "crashed"); created <- err }()
	select {
	case <-wrapped.held:
	case <-ctx.Done():
		t.Fatal("lease creation was not attempted")
	}
	observer, err := all[1].KeyValue(ctx, "WF_LEASE")
	if err != nil {
		t.Fatal(err)
	}
	var first jetstream.KeyValueEntry
	for ctx.Err() == nil {
		first, err = observer.Get(ctx, typ+"."+id)
		if err == nil {
			break
		}
		if !errors.Is(err, jetstream.ErrKeyNotFound) {
			t.Fatal(err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	if first == nil {
		t.Fatal("held create did not commit")
	}
	var uninitialized lease.Value
	if err := json.Unmarshal(first.Value(), &uninitialized); err != nil || uninitialized.Worker != "crashed" || uninitialized.Epoch != 0 {
		t.Fatalf("committed pre-initialization lease: value=%+v err=%v", uninitialized, err)
	}
	proxy.Block()
	select {
	case err := <-created:
		if err == nil {
			t.Fatal("lost create acknowledgment returned success")
		}
	case <-ctx.Done():
		t.Fatal("create did not return after connection cut")
	}
	proxy.Heal()
	if wait := time.Until(first.Created().Add(1100 * time.Millisecond)); wait > 0 {
		time.Sleep(wait)
	}
	survivorStore, err := lease.New(ctx, all[2])
	if err != nil {
		t.Fatal(err)
	}
	deadline := first.Created().Add(3 * time.Second)
	var survivor *lease.Lease
	for {
		survivor, err = survivorStore.Acquire(ctx, typ, id, "survivor")
		if !errors.Is(err, lease.ErrHeld) || time.Now().After(deadline) {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(25 * time.Millisecond):
		}
	}
	if err != nil {
		if errors.Is(err, lease.ErrHeld) {
			entry, getErr := observer.Get(ctx, typ+"."+id)
			if getErr == nil {
				var held lease.Value
				_ = json.Unmarshal(entry.Value(), &held)
				t.Fatalf("reclaim stayed held: value=%+v revision=%d age=%s first_age=%s", held, entry.Revision(), time.Since(entry.Created()), time.Since(first.Created()))
			}
			t.Fatalf("reclaim stayed held: observer=%v first_age=%s", getErr, time.Since(first.Created()))
		}
		t.Fatal(err)
	}
	if survivor.Epoch() <= first.Revision() {
		t.Fatalf("reclaimed fencing epoch %d did not pass orphan revision %d", survivor.Epoch(), first.Revision())
	}
	var current lease.Value
	observed := time.Now().Add(3 * time.Second)
	for {
		entry, getErr := observer.Get(ctx, typ+"."+id)
		if getErr != nil {
			t.Fatal(getErr)
		}
		if err := json.Unmarshal(entry.Value(), &current); err != nil {
			t.Fatal(err)
		}
		if current.Worker == "survivor" && current.Epoch == survivor.Epoch() {
			break
		}
		if time.Now().After(observed) {
			t.Fatalf("reclaimed lease value=%+v, want survivor epoch %d", current, survivor.Epoch())
		}
		time.Sleep(25 * time.Millisecond)
	}
}

func TestLeaseNetworkLostRenewAckCleanup(t *testing.T) {
	all, cluster := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
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
	kv, err := proxied.KeyValue(ctx, "WF_LEASE")
	if err != nil {
		t.Fatal(err)
	}
	wrapped := &holdLeaseRenewKV{KeyValue: kv, proxy: proxy, held: make(chan struct{})}
	store := lease.NewWithKeyValue(wrapped)
	const typ, id = "lease-contract", "lost-renew-ack"
	old, err := store.Acquire(ctx, typ, id, "old")
	if err != nil {
		t.Fatal(err)
	}
	observer, err := all[1].KeyValue(ctx, "WF_LEASE")
	if err != nil {
		t.Fatal(err)
	}
	initial, err := observer.Get(ctx, typ+"."+id)
	if err != nil {
		t.Fatal(err)
	}
	renewCtx, stopRenew := context.WithTimeout(ctx, 3*time.Second)
	defer stopRenew()
	renewed := make(chan error, 1)
	go func() { renewed <- old.Renew(renewCtx) }()
	select {
	case <-wrapped.held:
	case <-ctx.Done():
		t.Fatal("lease renewal was not attempted")
	}
	var committed jetstream.KeyValueEntry
	for ctx.Err() == nil {
		committed, err = observer.Get(ctx, typ+"."+id)
		if err != nil {
			t.Fatal(err)
		}
		if committed.Revision() > initial.Revision() {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if ctx.Err() != nil {
		t.Fatal("held renewal did not commit")
	}
	var retained lease.Value
	if err := json.Unmarshal(committed.Value(), &retained); err != nil || retained.Worker != "old" || retained.Epoch != old.Epoch() {
		t.Fatalf("committed renewal value=%+v err=%v", retained, err)
	}
	proxy.Block()
	select {
	case err := <-renewed:
		if !errors.Is(err, lease.ErrLost) {
			t.Fatalf("lost renewal acknowledgment: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("renewal did not return after connection cut")
	}
	proxy.Heal()
	if err := old.Cleanup(ctx); err != nil {
		t.Fatalf("cleanup of uncertain renewal: %v", err)
	}
	successorStore, err := lease.New(ctx, all[2])
	if err != nil {
		t.Fatal(err)
	}
	successor, err := successorStore.Acquire(ctx, typ, id, "successor")
	if err != nil || successor.Epoch() <= old.Epoch() {
		t.Fatalf("successor acquisition: lease=%+v err=%v", successor, err)
	}
	if err := old.Cleanup(ctx); !errors.Is(err, lease.ErrLost) {
		t.Fatalf("stale cleanup after successor: %v", err)
	}
	entry, err := observer.Get(ctx, typ+"."+id)
	if err != nil {
		t.Fatal(err)
	}
	var value lease.Value
	if err := json.Unmarshal(entry.Value(), &value); err != nil || value.Worker != "successor" || value.Epoch != successor.Epoch() {
		t.Fatalf("successor changed after cleanup: value=%+v err=%v", value, err)
	}
}
