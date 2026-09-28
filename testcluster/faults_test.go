package testcluster

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

func TestFaultScheduleReproducible(t *testing.T) {
	seed := int64(173)
	t.Logf("FAULT_SEED=%d", seed)
	a, err := Generate(seed, 100, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	b, err := Generate(seed, 100, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	first := filepath.Join(t.TempDir(), "first.json")
	second := filepath.Join(t.TempDir(), "second.json")
	if err := a.Save(first); err != nil {
		t.Fatal(err)
	}
	if err := b.Save(second); err != nil {
		t.Fatal(err)
	}
	firstBytes, err := os.ReadFile(first)
	if err != nil {
		t.Fatal(err)
	}
	secondBytes, err := os.ReadFile(second)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(firstBytes, secondBytes) {
		t.Fatal("same seed produced different schedules")
	}
	loaded, err := LoadFaultSchedule(first)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(a, loaded) {
		t.Fatal("loaded schedule changed")
	}
}

func TestFaultReplayOrder(t *testing.T) {
	s := FaultSchedule{Seed: 1, Events: []FaultEvent{{Op: KillNode, A: 0}, {Op: PauseNode, A: 1}}}
	var seen []FaultOp
	if err := s.Run(context.Background(), func(e FaultEvent) error { seen = append(seen, e.Op); return nil }); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(seen, []FaultOp{KillNode, PauseNode}) {
		t.Fatalf("events=%v", seen)
	}
}
