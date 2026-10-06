//go:build linux

package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"

	"js-wf/journal"
)

type latencyReductionFixture struct {
	records                                  []journal.Record
	times                                    []time.Time
	start, deadline, child, signal, terminal time.Time
}

func latencyReductionCase(t *testing.T) latencyReductionFixture {
	t.Helper()
	at := time.Date(2026, 10, 6, 0, 0, 0, 123456789, time.UTC)
	request, err := json.Marshal(map[string]any{"kind": "timer", "fire_at": at.Add(10 * time.Millisecond)})
	if err != nil {
		t.Fatal(err)
	}
	f := latencyReductionFixture{start: at, deadline: at.Add(2 * time.Minute), child: at.Add(20 * time.Millisecond), signal: at.Add(27 * time.Millisecond), terminal: at.Add(40 * time.Millisecond)}
	f.records = []journal.Record{
		{Entry: journal.Entry{Kind: journal.Started}},
		{Entry: journal.Entry{Kind: journal.StepRequested, Payload: request}},
		{Entry: journal.Entry{Kind: journal.StepCompleted}},
		{Entry: journal.Entry{Kind: journal.StepRequested, Payload: json.RawMessage(`{"kind":"call","child_type":"child","child_id":"x"}`)}},
		{Entry: journal.Entry{Kind: journal.StepCompleted}},
		{Entry: journal.Entry{Kind: journal.SignalConsumed, Payload: json.RawMessage(`{"sig_seq":7}`)}},
		{Entry: journal.Entry{Kind: journal.Completed}},
	}
	for _, ms := range []int{0, 2, 15, 17, 25, 30, 40} {
		f.times = append(f.times, at.Add(time.Duration(ms)*time.Millisecond))
	}
	return f
}

func (f latencyReductionFixture) reduce(ctx context.Context, offset time.Duration) ([]matrixLatencySample, error) {
	return matrixReduceInvocationLatencies(ctx, "parent", "p", f.start, f.deadline, offset, f.records, f.times,
		func(typ, id string) (time.Time, error) {
			if typ != "child" || id != "x" {
				return time.Time{}, errors.New("incorrect child lookup")
			}
			return f.child, nil
		},
		func(seq uint64) (time.Time, error) {
			if seq != 7 {
				return time.Time{}, errors.New("incorrect signal lookup")
			}
			return f.signal, nil
		},
		func() (time.Time, error) { return f.terminal, nil })
}

func TestMatrixLatencyReductionCausalSamplesAndClockNormalization(t *testing.T) {
	f := latencyReductionCase(t)
	for _, offset := range []time.Duration{0, 60 * time.Second, -60 * time.Second} {
		got, err := f.reduce(context.Background(), offset)
		if err != nil {
			t.Fatal(err)
		}
		want := []matrixLatencySample{}
		for i, event := range []string{"start", "timer_due", "child_completed", "signal_sent", "terminal"} {
			enabled := []int{0, 10, 20, 27, 27}[i]
			observed := []int{0, 15, 25, 30, 40}[i]
			want = append(want, matrixLatencySample{Type: "parent", ID: "p", Event: event, Enabled: f.start.Add(time.Duration(enabled)*time.Millisecond - offset), Observed: f.start.Add(time.Duration(observed)*time.Millisecond - offset), Delay: time.Duration(observed-enabled) * time.Millisecond, ServerClockOffset: offset})
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("offset %s: got=%+v want=%+v", offset, got, want)
		}
	}
	if f.times[0] != f.start {
		t.Fatal("reducer modified raw timestamps")
	}
}

func TestMatrixLatencyReductionRejectsIncompleteAndImpossibleEvidence(t *testing.T) {
	cases := map[string]func(*latencyReductionFixture){
		"timestamp census":       func(f *latencyReductionFixture) { f.times = f.times[:len(f.times)-1] },
		"missing start progress": func(f *latencyReductionFixture) { f.start = f.start.Add(time.Second) },
		"timer before due":       func(f *latencyReductionFixture) { f.times[2] = f.start.Add(9 * time.Millisecond) },
		"missing timer completion": func(f *latencyReductionFixture) {
			f.records[2].Kind = journal.Suspended
			f.records[4].Kind = journal.Suspended
		},
		"invalid request":          func(f *latencyReductionFixture) { f.records[1].Payload = json.RawMessage(`broken`) },
		"invalid signal":           func(f *latencyReductionFixture) { f.records[5].Payload = json.RawMessage(`broken`) },
		"signal before publish":    func(f *latencyReductionFixture) { f.times[5] = f.signal.Add(-time.Nanosecond) },
		"terminal after deadline":  func(f *latencyReductionFixture) { f.deadline = f.terminal.Add(-time.Nanosecond) },
		"terminal before enabling": func(f *latencyReductionFixture) { f.terminal = f.signal.Add(-time.Nanosecond) },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			f := latencyReductionCase(t)
			mutate(&f)
			samples, err := f.reduce(context.Background(), 0)
			if err == nil || samples != nil {
				t.Fatalf("invalid evidence escaped: samples=%+v err=%v", samples, err)
			}
		})
	}
}

func TestMatrixLatencyReductionDiscardsLookupFailureAndCanceledCompletion(t *testing.T) {
	f := latencyReductionCase(t)
	failure := errors.New("missing retained timestamp")
	for _, where := range []string{"child", "signal", "terminal"} {
		child := func(string, string) (time.Time, error) { return f.child, nil }
		signal := func(uint64) (time.Time, error) { return f.signal, nil }
		terminal := func() (time.Time, error) { return f.terminal, nil }
		switch where {
		case "child":
			child = func(string, string) (time.Time, error) { return time.Time{}, failure }
		case "signal":
			signal = func(uint64) (time.Time, error) { return time.Time{}, failure }
		case "terminal":
			terminal = func() (time.Time, error) { return time.Time{}, failure }
		}
		samples, err := matrixReduceInvocationLatencies(context.Background(), "parent", "p", f.start, f.deadline, 0, f.records, f.times, child, signal, terminal)
		if !errors.Is(err, failure) || samples != nil {
			t.Fatalf("%s failure escaped: %+v %v", where, samples, err)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	samples, err := matrixReduceInvocationLatencies(ctx, "parent", "p", f.start, f.deadline, 0, f.records, f.times, func(string, string) (time.Time, error) { return f.child, nil }, func(uint64) (time.Time, error) { return f.signal, nil }, func() (time.Time, error) { cancel(); return f.terminal, nil })
	if !errors.Is(err, context.Canceled) || samples != nil {
		t.Fatalf("canceled terminal accepted: %+v %v", samples, err)
	}
	samples, err = f.reduce(ctx, 0)
	if !errors.Is(err, context.Canceled) || samples != nil {
		t.Fatalf("pre-canceled reduction accepted: %+v %v", samples, err)
	}
}
