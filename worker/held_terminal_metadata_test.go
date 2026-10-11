package worker_test

import (
	"context"
	"encoding/json"
	"js-wf/client"
	"js-wf/identity"
	"js-wf/journal"
	"js-wf/lease"
	"js-wf/provision"
	"js-wf/sim"
	"js-wf/wf"
	"js-wf/worker"
	"testing"
	"time"
)

func terminalAdmissionCases() map[string]string {
	cases := map[string]string{
		"inline":                       `{"inv_seq":1,"result":"NDI="}`,
		"user_null":                    `{"inv_seq":1,"result":"bnVsbA=="}`,
		"opaque_user_duplicates":       `{"inv_seq":1,"result":"eyJ1c2VyIjoxLCJ1c2VyIjoyfQ=="}`,
		"explicit_empty_error":         `{"inv_seq":1,"result":"NDI=","error":""}`,
		"duplicate_invocation":         `{"inv_seq":9,"inv_seq":1,"result":"NDI="}`,
		"alias_invocation":             `{"Inv_seq":9,"inv_seq":1,"result":"NDI="}`,
		"escaped_duplicate_invocation": `{"\u0069nv_seq":9,"inv_seq":1,"result":"NDI="}`,
		"duplicate_result":             `{"inv_seq":1,"result":"Nw==","result":"NDI="}`,
		"alias_result":                 `{"inv_seq":1,"Result":"Nw==","result":"NDI="}`,
		"duplicate_error":              `{"inv_seq":1,"error":"bad","error":"","result":"NDI="}`,
		"alias_error":                  `{"inv_seq":1,"Error":"bad","error":"","result":"NDI="}`,
		"unknown_field":                `{"inv_seq":1,"foreign":true,"result":"NDI="}`,
		"null_limit_kind":              `{"inv_seq":1,"result":"NDI=","limit_entry":{"kind":null,"payload":{}}}`,
		"null_limit_payload":           `{"inv_seq":1,"result":"NDI=","limit_entry":{"kind":"Suspended","payload":null}}`,
		"duplicate_limit_kind":         `{"inv_seq":1,"result":"NDI=","limit_entry":{"kind":"Failed","kind":"Suspended","payload":{}}}`,
	}
	for _, field := range []string{"result_ref", "result_hash", "error", "limit_request", "limit_entry"} {
		cases["null_"+field] = `{"inv_seq":1,"result":"NDI=","` + field + `":null}`
	}
	cases["null_result"] = `{"inv_seq":1,"result":null}`
	return cases
}
func validTerminalAdmissionCase(name string) bool {
	return name == "inline" || name == "user_null" || name == "opaque_user_duplicates" || name == "explicit_empty_error"
}

func TestLegacyHeldTerminalMetadataAdmission(t *testing.T) {
	for name, raw := range terminalAdmissionCases() {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			schedule := sim.NewScheduler(1)
			transport := sim.NewWorkerTransport(schedule, 3*time.Second)
			c := client.NewWithSignalPorts(transport.SignalTransport, transport.SignalTransport)
			h, err := c.Start(ctx, "test", "terminal", []byte(`7`))
			if err != nil {
				t.Fatal(err)
			}
			if h.InvSeq != 1 {
				t.Fatal("fixture generation", h.InvSeq)
			}
			leases := lease.NewWithKVPort(sim.NewKVTransport(schedule, 30*time.Second))
			held, err := leases.Acquire(ctx, h.Type, h.ID, "other")
			if err != nil {
				t.Fatal(err)
			}
			defer held.Release(ctx)
			outcomes := sim.NewKVTransport(schedule, 0)
			if _, err = outcomes.Create(ctx, identity.Key(h.Type, h.ID), []byte(raw)); err != nil {
				t.Fatal(err)
			}
			legacy := sim.NewJournalTransport(schedule)
			handlers, acks, naks := 0, 0, 0
			w, err := worker.NewWithPorts("contender", map[string]worker.Handler{h.Type: func(*wf.Context, json.RawMessage) (json.RawMessage, error) { handlers++; return nil, nil }}, worker.ModeledWorkerPorts{Journal: journal.NewWithPorts(legacy, legacy), Leases: leases, Outcome: outcomes, Invocation: transport.SignalTransport, Signals: transport.SignalTransport, Client: c, HeartbeatTicks: make(chan time.Time)}, worker.WithDispatchObserver(func(e worker.DispatchEvent) {
				if e.Stage == "ack" {
					acks++
				}
				if e.Stage == "nak" {
					naks++
				}
			}))
			if err != nil {
				t.Fatal(err)
			}
			runCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()
			transport.Dispatch.StopWhenDrained(cancel)
			transport.Dispatch.StopAfterNextNak(cancel)
			if err = w.RunPartitionWithTransport(runCtx, identity.Partition(h.Type, h.ID, provision.Partitions), transport.Dispatch); err != nil {
				t.Fatal(err)
			}
			wantAck, wantNak := 0, 1
			if validTerminalAdmissionCase(name) {
				wantAck, wantNak = 1, 0
			}
			if handlers != 0 || acks != wantAck || naks != wantNak {
				t.Fatalf("held terminal: handlers=%d acks=%d want=%d naks=%d want=%d", handlers, acks, wantAck, naks, wantNak)
			}
			state, err := outcomes.Get(ctx, identity.Key(h.Type, h.ID))
			if err != nil || string(state.Value) != raw {
				t.Fatal("held delivery changed state", err)
			}
		})
	}
}
