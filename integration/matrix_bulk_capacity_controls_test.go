//go:build linux

package integration_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"js-wf/journal"
)

func TestMatrixCapacityOracleRejectsInvalidNativeInputs(t *testing.T) {
	at := time.Unix(1700000000, 0).UTC()
	for _, test := range []struct {
		name   string
		change func(*jetstream.RawStreamMsg)
	}{
		{"missing_time", func(m *jetstream.RawStreamMsg) { m.Time = time.Time{} }},
		{"wrong_type", func(m *jetstream.RawStreamMsg) { m.Subject = "wf.inv.other.capacity-000000" }},
		{"outside_population", func(m *jetstream.RawStreamMsg) { m.Subject = "wf.inv.audit.capacity-400000" }},
		{"wrong_id", func(m *jetstream.RawStreamMsg) { m.Subject = "wf.inv.audit.other" }},
	} {
		t.Run(test.name, func(t *testing.T) {
			o := matrixCapacityOracle{BySubject: map[string]*matrixCapacityTimes{}}
			m := &jetstream.RawStreamMsg{Subject: "wf.inv.audit.capacity-000000", Time: at}
			test.change(m)
			if err := o.invocation(m); err == nil {
				t.Fatal("invalid invocation accepted")
			}
		})
	}
	makeOracle := func() matrixCapacityOracle {
		o := matrixCapacityOracle{BySubject: map[string]*matrixCapacityTimes{}}
		if err := o.invocation(&jetstream.RawStreamMsg{Subject: "wf.inv.audit.capacity-000000", Time: at}); err != nil {
			t.Fatal(err)
		}
		return o
	}
	o := makeOracle()
	if err := o.invocation(&jetstream.RawStreamMsg{Subject: "wf.inv.audit.capacity-000000", Time: at}); err == nil {
		t.Fatal("duplicate invocation accepted")
	}
	entry := func(index uint64) journal.Entry {
		e := journal.Entry{Index: index, Epoch: 1, Kind: journal.StepCompleted}
		switch {
		case index == 0:
			e.Kind = journal.Started
		case index == 11:
			e.Kind = journal.Completed
			e.Payload = json.RawMessage(`"ok"`)
		case index%2 == 1:
			e.Kind = journal.StepRequested
			e.Payload = json.RawMessage(`{"kind":"activity"}`)
		}
		return e
	}
	appendEntry := func(o *matrixCapacityOracle, e journal.Entry, subject string, at time.Time) error {
		data, err := json.Marshal(e)
		if err != nil {
			t.Fatal(err)
		}
		return o.entry(&jetstream.RawStreamMsg{Subject: subject, Time: at, Data: data})
	}
	for _, test := range []struct {
		name   string
		index  uint64
		change func(*journal.Entry, *string, *time.Time)
	}{
		{"index_gap", 0, func(e *journal.Entry, _ *string, _ *time.Time) { e.Index = 1 }},
		{"wrong_epoch", 0, func(e *journal.Entry, _ *string, _ *time.Time) { e.Epoch = 2 }},
		{"wrong_kind", 0, func(e *journal.Entry, _ *string, _ *time.Time) { e.Kind = journal.StepCompleted }},
		{"unknown_subject", 0, func(_ *journal.Entry, s *string, _ *time.Time) { *s = "wf.jrn.audit.capacity-000001" }},
		{"start_before_invocation", 0, func(_ *journal.Entry, _ *string, ts *time.Time) { *ts = at.Add(-time.Second) }},
		{"missing_activity_kind", 1, func(e *journal.Entry, _ *string, _ *time.Time) { e.Payload = json.RawMessage(`{}`) }},
		{"unexpected_timer", 1, func(e *journal.Entry, _ *string, _ *time.Time) { e.Payload = json.RawMessage(`{"kind":"timer"}`) }},
		{"unexpected_child", 1, func(e *journal.Entry, _ *string, _ *time.Time) {
			e.Payload = json.RawMessage(`{"kind":"activity","child_id":"child"}`)
		}},
		{"wrong_terminal", 11, func(e *journal.Entry, _ *string, _ *time.Time) { e.Payload = json.RawMessage(`"bad"`) }},
	} {
		t.Run(test.name, func(t *testing.T) {
			o := makeOracle()
			for n := uint64(0); n < test.index; n++ {
				if err := appendEntry(&o, entry(n), "wf.jrn.audit.capacity-000000", at.Add(time.Duration(n+1)*time.Millisecond)); err != nil {
					t.Fatal(err)
				}
			}
			e := entry(test.index)
			subject := "wf.jrn.audit.capacity-000000"
			ts := at.Add(time.Duration(test.index+1) * time.Millisecond)
			test.change(&e, &subject, &ts)
			if err := appendEntry(&o, e, subject, ts); err == nil {
				t.Fatal("invalid journal accepted")
			}
		})
	}
	o = makeOracle()
	if _, err := o.Ordered[0].samples(at.Add(time.Second)); err == nil {
		t.Fatal("partial journal produced samples")
	}
	for i := uint64(0); i < 12; i++ {
		if err := appendEntry(&o, entry(i), "wf.jrn.audit.capacity-000000", at.Add(time.Duration(i+1)*time.Millisecond)); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := o.Ordered[0].samples(at); err == nil {
		t.Fatal("late terminal produced samples")
	}
	samples, err := o.Ordered[0].samples(at.Add(time.Second))
	if err != nil || len(samples) != 2 || samples[0].Delay != time.Millisecond || samples[1].Delay != 12*time.Millisecond {
		t.Fatalf("known timestamp oracle samples=%+v err=%v", samples, err)
	}
	if err := appendEntry(&o, entry(12), "wf.jrn.audit.capacity-000000", at.Add(time.Second)); err == nil {
		t.Fatal("extra journal entry accepted")
	}
}
