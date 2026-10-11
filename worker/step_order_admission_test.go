package worker_test

import (
	"js-wf/journal"
	"testing"
)

func TestWorkerStepOrderBeforeHandler(t *testing.T) {
	cases := []struct {
		name, level string
		valid       bool
		mutate      func([]journal.Entry) []journal.Entry
	}{
		{"balanced", "healthy", true, func(e []journal.Entry) []journal.Entry { return e }},
		{"suspended_between_pair", "healthy", true, func(e []journal.Entry) []journal.Entry {
			return append(e[:2], journal.Entry{Kind: journal.Suspended, Payload: []byte(`{"waiting_on":"step"}`)}, e[2])
		}},
		{"pending_request", "pending_request", true, func(e []journal.Entry) []journal.Entry { return e[:2] }},
		{"orphan_completion", "healthy", false, func(e []journal.Entry) []journal.Entry { return []journal.Entry{e[0], e[2]} }},
		{"overlapping_requests", "healthy", false, func(e []journal.Entry) []journal.Entry { return []journal.Entry{e[0], e[1], e[1]} }},
		{"completion_then_request", "healthy", false, func(e []journal.Entry) []journal.Entry { return []journal.Entry{e[0], e[2], e[1]} }},
		{"duplicate_completion", "healthy", false, func(e []journal.Entry) []journal.Entry { return append(e, e[2]) }},
		{"completed_pending", "terminal", false, func(e []journal.Entry) []journal.Entry {
			return append(e[:2], journal.Entry{Kind: journal.Completed, Payload: []byte(`{"inv_seq":1,"result":"NDI="}`)})
		}},
		{"balanced_terminal", "terminal", true, func(e []journal.Entry) []journal.Entry {
			return append(e, journal.Entry{Kind: journal.Completed, Payload: []byte(`{"inv_seq":1,"result":"NDI="}`)})
		}},
		{"failed_pending", "failed_pending", true, func(e []journal.Entry) []journal.Entry {
			return append(e[:2], journal.Entry{Kind: journal.Failed, Payload: []byte(`{"inv_seq":1,"error":"stopped"}`)})
		}},
	}
	for _, backend := range []string{"legacy", "canonical"} {
		for _, c := range cases {
			t.Run(backend+"/"+c.name, func(t *testing.T) {
				runWorkerStepAdmission(t, backend, c.level, "valid", func(entries []journal.Entry) []journal.Entry {
					entries = c.mutate(entries)
					for i := range entries {
						entries[i].Index = uint64(i)
					}
					return entries
				}, c.valid)
			})
		}
	}
}
