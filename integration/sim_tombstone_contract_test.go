package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"js-wf/identity"
	"js-wf/retention"
	"js-wf/sim"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func TestSimTombstoneSweepContractAgainstRealCluster(t *testing.T) {
	all, _ := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	const typ = "tombcontract"
	now := time.Now().UTC()
	realState, err := all[0].KeyValue(ctx, "WF_STATE")
	if err != nil {
		t.Fatal(err)
	}
	schedule := sim.NewScheduler(42)
	modelInv := sim.NewStartTransport(schedule)
	modelState := sim.NewKVTransport(schedule, 0)
	modelPort := sim.TombstoneSweepTransport{Invocations: modelInv, State: modelState}
	type candidate struct {
		id             string
		kind           string
		wantExpired    bool
		wantEligible   bool
		wantStateAlive bool
	}
	cases := []candidate{
		{id: "absent", kind: "absent", wantExpired: true, wantEligible: true},
		{id: "held", kind: "held", wantExpired: true, wantStateAlive: true},
		{id: "reused", kind: "reused", wantExpired: true, wantEligible: true},
		{id: "future", kind: "future", wantStateAlive: true},
	}
	for _, tc := range cases {
		subject := identity.InvocationSubject(typ, tc.id)
		var realGeneration, modelGeneration uint64 = 1, 1
		if tc.kind == "reused" {
			old := identity.InvocationSubject(typ, "retired-old")
			ack, err := all[0].Publish(ctx, old, []byte(`null`))
			if err != nil {
				t.Fatal(err)
			}
			realGeneration = ack.Sequence
			modelGeneration, err = modelInv.PublishInvocation(ctx, &nats.Msg{Subject: old, Data: []byte(`null`)})
			if err != nil {
				t.Fatal(err)
			}
		}
		if tc.kind == "held" || tc.kind == "reused" {
			ack, err := all[0].Publish(ctx, subject, []byte(`null`))
			if err != nil {
				t.Fatal(err)
			}
			modelSequence, err := modelInv.PublishInvocation(ctx, &nats.Msg{Subject: subject, Data: []byte(`null`)})
			if err != nil {
				t.Fatal(err)
			}
			if tc.kind == "held" {
				realGeneration, modelGeneration = ack.Sequence, modelSequence
			} else if ack.Sequence <= realGeneration || modelSequence <= modelGeneration {
				t.Fatal("reused invocation did not advance its stream sequence")
			}
		}
		expires := now.Add(-time.Minute)
		if tc.kind == "future" {
			expires = now.Add(time.Minute)
		}
		encode := func(generation uint64) []byte {
			value, err := json.Marshal(retention.Tombstone{Tombstone: true, InvSeq: generation, PurgedAt: now.Add(-time.Hour), ExpiresAt: expires})
			if err != nil {
				t.Fatal(err)
			}
			return value
		}
		key := identity.Key(typ, tc.id)
		if _, err := realState.Put(ctx, key, encode(realGeneration)); err != nil {
			t.Fatal(err)
		}
		modelValue := encode(modelGeneration)
		revision, err := modelState.Create(ctx, key, modelValue)
		if err != nil {
			t.Fatal(err)
		}
		expired, eligible, deleted, err := retention.SweepCandidate(ctx, modelPort, key, modelValue, revision, now, false)
		if err != nil || expired != tc.wantExpired || eligible != tc.wantEligible || deleted != tc.wantEligible {
			t.Fatalf("model %s expired=%v eligible=%v deleted=%v err=%v", tc.id, expired, eligible, deleted, err)
		}
		_, err = modelState.Get(ctx, key)
		if alive := err == nil; alive != tc.wantStateAlive {
			t.Fatalf("model %s alive=%v err=%v", tc.id, alive, err)
		}
	}
	realResult, err := retention.SweepTombstones(ctx, all[0], now)
	if err != nil || realResult.Expired != 3 || realResult.Deleted != 2 {
		t.Fatalf("real sweep=%+v err=%v", realResult, err)
	}
	for _, tc := range cases {
		_, err := realState.Get(ctx, identity.Key(typ, tc.id))
		if alive := err == nil; alive != tc.wantStateAlive || err != nil && !errors.Is(err, jetstream.ErrKeyNotFound) {
			t.Fatalf("real %s alive=%v err=%v", tc.id, alive, err)
		}
	}
}
