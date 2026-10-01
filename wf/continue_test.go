package wf

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
)

type continuationFixture struct {
	records             []Entry
	epochs              map[uint64]uint64
	objects             map[string][]byte
	puts, gets, anchors int
	fault               string
	fired               bool
	epoch               uint64
}

func newContinuationFixture() *continuationFixture {
	return &continuationFixture{epochs: map[uint64]uint64{}, objects: map[string][]byte{}}
}
func (f *continuationFixture) context(epoch uint64) *Context {
	f.epoch = epoch
	c := NewContext(context.Background(), append([]Entry(nil), f.records...), func(_ context.Context, kind Kind, payload json.RawMessage) error {
		if f.fault == "request_drop" && kind == StepRequested && !f.fired {
			f.fired = true
			return context.DeadlineExceeded
		}
		if f.fault == "completion_drop" && kind == StepCompleted && !f.fired {
			f.fired = true
			return context.DeadlineExceeded
		}
		index := uint64(len(f.records) + 1)
		f.records = append(f.records, Entry{Index: index, Kind: kind, Payload: bytes.Clone(payload)})
		f.epochs[index] = f.epoch
		if !f.fired && (f.fault == "request_ack_lost" && kind == StepRequested || f.fault == "completion_ack_lost" && kind == StepCompleted) {
			f.fired = true
			return context.DeadlineExceeded
		}
		return nil
	})
	c.SetChildSupport("parent", "continue", 17, nil)
	c.SetContinuationSupport(func(stage string) bool { return stage == "next_v1" || stage == "other_v1" }, func(completed uint64, recorded bool) (ContinuationAnchor, error) {
		f.anchors++
		if completed != 0 {
			return ContinuationAnchor{Index: completed, Epoch: f.epochs[completed]}, nil
		}
		index := uint64(len(f.records) + 2)
		if recorded {
			index--
		}
		return ContinuationAnchor{Index: index, Epoch: f.epoch}, nil
	})
	c.SetResultStore(func(_ context.Context, raw []byte) (string, error) {
		f.puts++
		hash := sha256.Sum256(raw)
		name := "step-result-" + hex.EncodeToString(hash[:])
		f.objects[name] = bytes.Clone(raw)
		if f.fault == "blob_ack_lost" && !f.fired {
			f.fired = true
			return "", context.DeadlineExceeded
		}
		return name, nil
	}, func(_ context.Context, name string) ([]byte, error) {
		f.gets++
		if f.fault == "blob_get_timeout" && !f.fired {
			f.fired = true
			return nil, context.DeadlineExceeded
		}
		data, ok := f.objects[name]
		if !ok {
			return nil, fmt.Errorf("missing %s", name)
		}
		return bytes.Clone(data), nil
	})
	return c
}

func TestContinueCapturesStateAndEndsDelivery(t *testing.T) {
	f := newContinuationFixture()
	c := f.context(51)
	if err := c.SetState("total", 23); err != nil {
		t.Fatal(err)
	}
	if err := Continue(c, "next_v1", map[string]int{"total": 23}); !errors.Is(err, ErrContinuation) {
		t.Fatal(err)
	}
	point, ok := c.Continuation()
	if !ok || point.Index != 4 || point.Epoch != 51 || point.StepPosition != 4 || point.Stage != "next_v1" || f.puts != 1 || f.gets != 1 || len(f.records) != 4 {
		t.Fatalf("point=%+v puts=%d gets=%d entries=%d", point, f.puts, f.gets, len(f.records))
	}
	if !errors.Is(c.CheckComplete(), ErrContinuation) || c.WaitingOn() != "continuation:next_v1" {
		t.Fatal("continuation became normal completion")
	}
	if err := Continue(c, "next_v1", nil); !errors.Is(err, ErrContinuation) {
		t.Fatal(err)
	}
	if _, err := Run(c, "after-checkpoint", nil, func(context.Context) (int, error) { t.Fatal("post-checkpoint effect executed"); return 0, nil }); !errors.Is(err, ErrSuspended) {
		t.Fatal(err)
	}
	if len(f.records) != 4 || f.puts != 1 {
		t.Fatal("checkpoint delivery continued")
	}
	// A replacement owner replays the old completion using its recorded epoch,
	// rather than rebuilding the frame with its newly acquired epoch.
	replay := f.context(99)
	if err := replay.SetState("total", 23); err != nil {
		t.Fatal(err)
	}
	if err := Continue(replay, "next_v1", map[string]int{"total": 23}); !errors.Is(err, ErrContinuation) {
		t.Fatal(err)
	}
	replayed, ok := replay.Continuation()
	if !ok || replayed != point || f.puts != 1 || f.gets != 2 || len(f.records) != 4 {
		t.Fatalf("replay=%+v puts=%d gets=%d", replayed, f.puts, f.gets)
	}
}

func TestContinueRepairsPublicationCutsWithoutDuplicateEntries(t *testing.T) {
	for _, fault := range []string{"request_drop", "request_ack_lost", "blob_ack_lost", "blob_get_timeout", "completion_drop", "completion_ack_lost"} {
		t.Run(fault, func(t *testing.T) {
			f := newContinuationFixture()
			f.fault = fault
			c := f.context(51)
			first := Continue(c, "next_v1", 23)
			if !errors.Is(first, context.DeadlineExceeded) {
				t.Fatalf("fault wasn't injected: %v", first)
			}
			if _, ok := c.Continuation(); ok {
				t.Fatal("unknown publication advertised a continuation")
			}
			if err := c.CheckComplete(); !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("unknown write could complete: %v", err)
			}
			if _, err := Run(c, "ignored-error", nil, func(context.Context) (int, error) { t.Fatal("effect after failed checkpoint"); return 0, nil }); err == nil {
				t.Fatal("failed context kept running")
			}
			repair := f.context(99)
			if err := Continue(repair, "next_v1", 23); !errors.Is(err, ErrContinuation) {
				t.Fatal(err)
			}
			point, ok := repair.Continuation()
			if !ok || len(f.records) != 2 || f.records[0].Kind != StepRequested || f.records[1].Kind != StepCompleted || point.Index != 2 || point.StepPosition != 2 {
				t.Fatalf("repair point=%+v entries=%+v", point, f.records)
			}
			wantEpoch := uint64(99)
			if fault == "completion_ack_lost" {
				wantEpoch = 51
			}
			if point.Epoch != wantEpoch {
				t.Fatalf("epoch=%d want=%d", point.Epoch, wantEpoch)
			}
			puts := f.puts
			again := f.context(101)
			if err := Continue(again, "next_v1", 23); !errors.Is(err, ErrContinuation) {
				t.Fatal(err)
			}
			last, _ := again.Continuation()
			if last != point || len(f.records) != 2 || f.puts != puts {
				t.Fatal("replay rewrote checkpoint")
			}
		})
	}
}

func TestContinueRejectsChangedLocalsAndStagesBeforeObjectReads(t *testing.T) {
	f := newContinuationFixture()
	c := f.context(51)
	if err := Continue(c, "next_v1", 23); !errors.Is(err, ErrContinuation) {
		t.Fatal(err)
	}
	for _, tt := range []struct {
		stage string
		data  any
	}{{"next_v1", 24}, {"other_v1", 23}} {
		replay := f.context(99)
		reads, anchors, puts := f.gets, f.anchors, f.puts
		if err := Continue(replay, tt.stage, tt.data); !errors.Is(err, ErrNonDeterministic) {
			t.Fatal(err)
		}
		if f.gets != reads || f.anchors != anchors || f.puts != puts || len(f.records) != 2 {
			t.Fatal("divergent continuation did I/O")
		}
	}
}

func TestContinueRejectsUnregisteredAndInvalidBoundaries(t *testing.T) {
	f := newContinuationFixture()
	c := f.context(51)
	if err := Continue(c, "unknown_v1", nil); !errors.Is(err, ErrUnknownContinuation) {
		t.Fatal(err)
	}
	if err := Continue(c, "next_v1", map[string]any{"context": c}); !errors.Is(err, ErrInvalidCheckpoint) {
		t.Fatal(err)
	}
	timer, err := c.Timer("live", 0)
	if err != nil {
		t.Fatal(err)
	}
	before := len(f.records)
	if err := Continue(c, "next_v1", nil); !errors.Is(err, ErrCheckpointBoundary) {
		t.Fatal(err)
	}
	if len(f.records) != before || f.puts != 0 {
		t.Fatal("invalid boundary published")
	}
	if err := timer.Cancel(); err != nil {
		t.Fatal(err)
	}
	if err := Continue(c, "next_v1", nil); !errors.Is(err, ErrContinuation) {
		t.Fatal(err)
	}
}

type continuationMarshalCounter struct{ calls *int }

func (v continuationMarshalCounter) MarshalJSON() ([]byte, error) {
	*v.calls++
	return []byte(fmt.Sprintf(`{"call":%d}`, *v.calls)), nil
}
func TestContinueMarshalsLocalsOnceAndProtectsStoreInput(t *testing.T) {
	f := newContinuationFixture()
	c := f.context(51)
	calls := 0
	if err := Continue(c, "next_v1", continuationMarshalCounter{calls: &calls}); !errors.Is(err, ErrContinuation) || calls != 1 {
		t.Fatalf("calls=%d err=%v", calls, err)
	}
	f = newContinuationFixture()
	c = f.context(51)
	// A buggy transport mutates its input and returns that mutated object. It
	// must not mutate the frame retained for verification and publish success.
	c.SetResultStore(func(_ context.Context, raw []byte) (string, error) {
		hash := sha256.Sum256(raw)
		name := "step-result-" + hex.EncodeToString(hash[:])
		raw[0] = '['
		f.objects[name] = raw
		return name, nil
	}, func(_ context.Context, name string) ([]byte, error) { return f.objects[name], nil })
	if err := Continue(c, "next_v1", 23); !errors.Is(err, ErrInvalidCheckpoint) {
		t.Fatal(err)
	}
	if len(f.records) != 1 {
		t.Fatal("corrupt frame completed")
	}
}
