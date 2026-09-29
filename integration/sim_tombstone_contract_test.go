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
		wantStateAlive bool
	}
	cases := []candidate{
		{id: "absent", kind: "absent"},
		{id: "held", kind: "held", wantStateAlive: true},
		{id: "reused", kind: "reused"},
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
		_, err := modelState.Create(ctx, key, encode(modelGeneration))
		if err != nil {
			t.Fatal(err)
		}
	}
	modelResult, err := retention.SweepTombstonesWithPort(ctx, modelPort, now)
	if err != nil || modelResult.Expired != 3 || modelResult.Deleted != 2 {
		t.Fatalf("model sweep=%+v err=%v", modelResult, err)
	}
	realResult, err := retention.SweepTombstones(ctx, all[0], now)
	if err != nil || realResult != modelResult {
		t.Fatalf("real sweep=%+v model sweep=%+v err=%v", realResult, modelResult, err)
	}
	for _, tc := range cases {
		_, modelErr := modelState.Get(ctx, identity.Key(typ, tc.id))
		_, err := realState.Get(ctx, identity.Key(typ, tc.id))
		if alive := err == nil; alive != tc.wantStateAlive || alive != (modelErr == nil) || err != nil && !errors.Is(err, jetstream.ErrKeyNotFound) {
			t.Fatalf("%s real=%v model=%v", tc.id, err, modelErr)
		}
	}
}
