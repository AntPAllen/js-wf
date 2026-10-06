//go:build linux

package integrity

import (
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

func restoredCapacityInventoryForTest(t *testing.T) []capacityConsumerInventory {
	t.Helper()
	j := capacityConsumerInventory{Stream: "WF_JRN", Count: 2}
	for name, stamp := range capacityRestoredCursorCreated {
		created, err := time.Parse(time.RFC3339Nano, stamp)
		if err != nil {
			t.Fatal(err)
		}
		j.Names = append(j.Names, name)
		j.Consumers = append(j.Consumers, &jetstream.ConsumerInfo{Name: name, Stream: "WF_JRN", Created: created, NumPending: 4800000, Config: jetstream.ConsumerConfig{Name: name, MemoryStorage: true, Replicas: 5, AckPolicy: jetstream.AckNonePolicy, DeliverPolicy: jetstream.DeliverByStartSequencePolicy, OptStartSeq: 1, InactiveThreshold: 30 * time.Second}})
	}
	return []capacityConsumerInventory{{Stream: "WF_INV", Count: 0}, j}
}

func TestRestoredCapacityPreparationRejectsUnverifiedCursors(t *testing.T) {
	good := restoredCapacityInventoryForTest(t)
	names, err := validateRestoredCapacityCursors(good)
	if err != nil || len(names) != 2 {
		t.Fatalf("verified identities: %v / %v", names, err)
	}
	cases := map[string]func([]capacityConsumerInventory){
		"extra INV":          func(x []capacityConsumerInventory) { x[0].Count = 1 },
		"unavailable list":   func(x []capacityConsumerInventory) { x[1].Error = "deadline" },
		"unavailable names":  func(x []capacityConsumerInventory) { x[1].NamesError = "deadline" },
		"unknown assignment": func(x []capacityConsumerInventory) { x[1].Names[0] = "wf-audit-unrelated" },
		"duplicate names":    func(x []capacityConsumerInventory) { x[1].Names[0] = x[1].Names[1] },
		"duplicate infos":    func(x []capacityConsumerInventory) { x[1].Consumers[0] = x[1].Consumers[1] },
		"missing Info":       func(x []capacityConsumerInventory) { x[1].Consumers[0] = nil },
		"different creation": func(x []capacityConsumerInventory) {
			x[1].Consumers[0].Created = x[1].Consumers[0].Created.Add(time.Nanosecond)
		},
		"different stream":   func(x []capacityConsumerInventory) { x[1].Consumers[0].Stream = "WF_INV" },
		"file consumer":      func(x []capacityConsumerInventory) { x[1].Consumers[0].Config.MemoryStorage = false },
		"different replicas": func(x []capacityConsumerInventory) { x[1].Consumers[0].Config.Replicas = 1 },
		"ack consumer":       func(x []capacityConsumerInventory) { x[1].Consumers[0].Config.AckPolicy = jetstream.AckExplicitPolicy },
		"different start":    func(x []capacityConsumerInventory) { x[1].Consumers[0].Config.OptStartSeq = 2 },
		"filter":             func(x []capacityConsumerInventory) { x[1].Consumers[0].Config.FilterSubject = "wf.jrn.other.*" },
		"position changed":   func(x []capacityConsumerInventory) { x[1].Consumers[0].Delivered.Stream = 1 },
		"different pending":  func(x []capacityConsumerInventory) { x[1].Consumers[0].NumPending-- },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			inventory := restoredCapacityInventoryForTest(t)
			mutate(inventory)
			if names, err := validateRestoredCapacityCursors(inventory); err == nil || len(names) != 0 {
				t.Fatalf("unverified deletion set: %v / %v", names, err)
			}
		})
	}
}
