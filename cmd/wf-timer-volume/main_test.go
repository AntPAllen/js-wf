package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"js-wf/identity"
	"js-wf/provision"
)

func TestMillionDeadlineSpanDoesNotOverflow(t *testing.T) {
	base := time.Unix(0, 0).UTC()
	const count = 1000000
	horizon := 24 * time.Hour
	previous := base.Add(-time.Nanosecond)
	for i := 0; i < count; i++ {
		next := due(base, horizon, count, i)
		if !next.After(previous) || next.Before(base) || next.After(base.Add(horizon)) {
			t.Fatalf("deadline %d=%s previous=%s", i, next, previous)
		}
		previous = next
	}
	if got := due(base, horizon, count, count-1); !got.Equal(base.Add(horizon)) {
		t.Fatalf("last deadline=%s", got)
	}
	if got := due(base, horizon, count, 0); !got.Equal(base) {
		t.Fatalf("first deadline=%s", got)
	}
}

type observedMsg struct {
	jetstream.Msg
	data    []byte
	subject string
	header  nats.Header
	meta    jetstream.MsgMetadata
}

func (m observedMsg) Data() []byte                              { return m.data }
func (m observedMsg) Subject() string                           { return m.subject }
func (m observedMsg) Headers() nats.Header                      { return m.header }
func (m observedMsg) Metadata() (*jetstream.MsgMetadata, error) { return &m.meta, nil }

func TestDeliveryCheckerRejectsCorruptionAndDuplicateEmission(t *testing.T) {
	cfg := config{Count: 3, Horizon: 6 * time.Second}
	base := time.Now().Add(-10 * time.Second)
	partition := identity.Partition("volume", "0", provision.Partitions)
	valid := observedMsg{data: []byte("volume.0"), subject: identity.RunSubject("volume", "0", provision.Partitions), header: nats.Header{identity.TimerInvSeqHeader: []string{"1"}, identity.TimerStepHeader: []string{"0"}}, meta: jetstream.MsgMetadata{Timestamp: base.Add(time.Second), Stream: "WF_RUN", Consumer: fmt.Sprintf("volume-%d", partition), Sequence: jetstream.SequencePair{Stream: 100}}}
	newCampaign := func() *campaign {
		f, err := os.Create(filepath.Join(t.TempDir(), "receipts.bin"))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { f.Close() })
		return &campaign{cfg: cfg, base: base, seen: make([]observation, 3), ledger: f}
	}
	c := newCampaign()
	if err := c.observe(valid, partition); err != nil {
		t.Fatal(err)
	}
	if err := c.observe(valid, partition); err != nil {
		t.Fatalf("same stream sequence redelivery: %v", err)
	}
	if c.rep.Received != 1 || c.rep.Redeliveries != 1 {
		t.Fatalf("receipt accounting: %+v", c.rep)
	}
	duplicate := valid
	duplicate.meta.Sequence.Stream++
	if err := c.observe(duplicate, partition); err == nil || !strings.Contains(err.Error(), "emitted twice") {
		t.Fatalf("duplicate emission accepted: %v", err)
	}
	for _, test := range []struct {
		name   string
		mutate func(*observedMsg)
	}{
		{"early", func(m *observedMsg) { m.meta.Timestamp = base.Add(-time.Nanosecond) }},
		{"wrong_target", func(m *observedMsg) { m.subject = "wf.run.999" }},
		{"out_of_range", func(m *observedMsg) { m.data = []byte("volume.3") }},
		{"noncanonical", func(m *observedMsg) { m.data = []byte("volume.00") }},
		{"wrong_generation", func(m *observedMsg) {
			m.header = nats.Header{identity.TimerInvSeqHeader: []string{"99"}, identity.TimerStepHeader: []string{"0"}}
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			bad := valid
			test.mutate(&bad)
			if err := newCampaign().observe(bad, partition); err == nil {
				t.Fatal("corrupt delivery accepted")
			}
		})
	}
}
