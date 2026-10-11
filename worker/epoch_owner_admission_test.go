package worker_test

import (
	"js-wf/journal"
	"testing"
)

func TestWorkerEpochOwnerBeforeHandler(t *testing.T) {
	cases := []struct {
		name    string
		epochs  [3]uint64
		workers [3]string
		valid   bool
	}{
		{"one_owner", [3]uint64{1, 1, 1}, [3]string{"a", "a", "a"}, true},
		{"successor_owner", [3]uint64{1, 2, 2}, [3]string{"a", "b", "b"}, true},
		{"same_owner_next_epoch", [3]uint64{1, 2, 2}, [3]string{"a", "a", "a"}, true},
		{"omitted_middle", [3]uint64{1, 1, 1}, [3]string{"a", "", "a"}, true},
		{"all_omitted", [3]uint64{1, 1, 1}, [3]string{}, true},
		{"zero_epoch_legacy", [3]uint64{}, [3]string{"a", "b", "c"}, true},
		{"request_owner_conflict", [3]uint64{1, 1, 1}, [3]string{"a", "b", "b"}, false},
		{"completion_owner_conflict", [3]uint64{1, 1, 1}, [3]string{"a", "a", "b"}, false},
		{"conflict_after_omitted", [3]uint64{1, 1, 1}, [3]string{"a", "", "b"}, false},
		{"conflict_without_started_owner", [3]uint64{1, 1, 1}, [3]string{"", "a", "b"}, false},
		{"successor_epoch_conflict", [3]uint64{1, 2, 2}, [3]string{"a", "b", "c"}, false},
	}
	for _, backend := range []string{"legacy", "canonical"} {
		for _, c := range cases {
			t.Run(backend+"/"+c.name, func(t *testing.T) {
				runWorkerStepAdmission(t, backend, "healthy", "valid", func(entries []journal.Entry) []journal.Entry {
					for i := range entries {
						entries[i].Epoch = c.epochs[i]
						entries[i].WorkerID = c.workers[i]
					}
					return entries
				}, c.valid)
			})
		}
	}
}
