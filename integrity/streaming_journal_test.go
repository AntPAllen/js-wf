package integrity

import (
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"strings"
	"testing"

	"js-wf/journal"
)

func compareStreamingJournal(t *testing.T, records []journal.Record, value []byte, missing bool) {
	t.Helper()
	const subject = "wf.jrn.test.stream"
	missingErr := errors.New("missing retained state")
	reads := 0
	lookup := func() ([]byte, error) {
		reads++
		if missing {
			return nil, missingErr
		}
		return value, nil
	}
	wantCount, wantTerminal, wantErr := checkJournalRecords(subject, records, lookup)
	wantReads := reads
	reads = 0
	var state journalAudit
	for _, record := range records {
		state.add(subject, record)
	}
	count, terminal, err := state.finish(subject, lookup)
	message := func(err error) string {
		if err == nil {
			return ""
		}
		return err.Error()
	}
	if count != wantCount || terminal != wantTerminal || message(err) != message(wantErr) || reads != wantReads || errors.Is(err, missingErr) != errors.Is(wantErr, missingErr) {
		t.Fatalf("count=%d/%d terminal=%v/%v reads=%d/%d error=%v/%v records=%+v", count, wantCount, terminal, wantTerminal, reads, wantReads, err, wantErr, records)
	}
}

func TestStreamingJournalDifferentialHistories(t *testing.T) {
	kinds := []journal.Kind{journal.Started, journal.StepRequested, journal.StepCompleted, journal.Suspended, journal.SignalConsumed, journal.Attempt, journal.Completed, journal.Failed, "unknown"}
	for seed := int64(1); seed <= 1000; seed++ {
		rng := rand.New(rand.NewSource(seed))
		var records []journal.Record
		var epoch, sequence uint64
		signal, attempts := 0, 0
		appendEntry := func(kind journal.Kind, payload json.RawMessage) {
			epoch += uint64(rng.Intn(2))
			sequence += uint64(1 + rng.Intn(5))
			worker := fmt.Sprintf("worker-%d", epoch)
			if rng.Intn(3) == 0 {
				worker = ""
			}
			records = append(records, journal.Record{Entry: journal.Entry{Index: uint64(len(records)), Epoch: epoch, WorkerID: worker, Kind: kind, Payload: payload}, Sequence: sequence})
		}
		appendEntry(journal.Started, nil)
		for step := 0; step < 1+rng.Intn(12); step++ {
			appendEntry(journal.StepRequested, []byte(`{"kind":"step"}`))
			if rng.Intn(2) == 0 {
				attempts++
				appendEntry(journal.Attempt, []byte(fmt.Sprintf(`{"count":%d,"error":"panic"}`, attempts)))
			}
			if rng.Intn(2) == 0 {
				appendEntry(journal.Suspended, []byte(`{"waiting_on":"step"}`))
			}
			appendEntry(journal.StepCompleted, []byte(`{"result":"ok"}`))
			if rng.Intn(2) == 0 {
				signal++
				appendEntry(journal.SignalConsumed, []byte(fmt.Sprintf(`{"sig_seq":%d}`, signal)))
			}
		}
		appendEntry(journal.Completed, []byte(`"ok"`))
		if _, _, err := checkJournalRecords("wf.jrn.test.stream", records, func() ([]byte, error) { return []byte(`"ok"`), nil }); err != nil {
			t.Fatalf("valid generator seed=%d failed: %v", seed, err)
		}
		compareStreamingJournal(t, records, []byte(`"ok"`), false)
		compareStreamingJournal(t, records, []byte(`"different"`), false)
		compareStreamingJournal(t, records, nil, true)
		for cut := range records {
			compareStreamingJournal(t, records[:cut], nil, false)
		}
		for mutation := 0; mutation < 12; mutation++ {
			changed := append([]journal.Record(nil), records...)
			at := rng.Intn(len(changed))
			switch mutation {
			case 0:
				changed[at].Sequence = 0
			case 1:
				if at > 0 {
					changed[at].Sequence = changed[at-1].Sequence
				}
			case 2:
				changed[at].Index += 1
			case 3:
				changed[at].Epoch = 0
			case 4:
				changed[at].WorkerID = "other-worker"
			case 5:
				changed[at].Kind = kinds[rng.Intn(len(kinds))]
			case 6:
				changed[at].Payload = []byte(`invalid`)
			case 7:
				changed = append(changed, changed[len(changed)-1])
				changed[len(changed)-1].Index++
				changed[len(changed)-1].Sequence++
			case 8:
				changed[at].Kind = journal.SignalConsumed
				changed[at].Payload = []byte(`{"sig_seq":0}`)
			case 9:
				changed[at].Kind = journal.Attempt
				changed[at].Payload = []byte(`{"count":0}`)
			case 10:
				changed[at].Kind = journal.Failed
			case 11:
				changed[at].Kind = journal.StepRequested
			}
			compareStreamingJournal(t, changed, []byte(`"ok"`), false)
		}
		// Arbitrary histories exercise combinations outside the valid generator.
		for i := range records {
			records[i].Kind = kinds[rng.Intn(len(kinds))]
			records[i].Epoch = uint64(rng.Intn(4))
			records[i].WorkerID = []string{"", "worker-a", "worker-b"}[rng.Intn(3)]
		}
		compareStreamingJournal(t, records, []byte(`"ok"`), false)
	}
}

func TestStreamingJournalContinuationAndEpochOwnership(t *testing.T) {
	hash := strings.Repeat("a", 64)
	entries := []journal.Entry{
		{Kind: journal.Started},
		{Index: 1, Epoch: 1, WorkerID: "worker-a", Kind: journal.StepRequested, Payload: []byte(`{"kind":"checkpoint","name":"next_v1"}`)},
		{Index: 2, Epoch: 1, Kind: journal.StepCompleted, Payload: []byte(`{"result_ref":"step-result-` + hash + `","result_hash":"` + hash + `"}`)},
		{Index: 3, Epoch: 1, WorkerID: "worker-a", Kind: journal.Suspended, Payload: []byte(`{"waiting_on":"continuation:next_v1"}`)},
		{Index: 4, Epoch: 2, Kind: journal.Completed, Payload: []byte(`"ok"`)},
	}
	var records []journal.Record
	for i, e := range entries {
		records = append(records, journal.Record{Entry: e, Sequence: uint64(i + 1)})
	}
	compareStreamingJournal(t, records, []byte(`"ok"`), false)
	records[3].WorkerID = "worker-b" // blank owner on the completion cannot erase worker-a
	compareStreamingJournal(t, records, []byte(`"ok"`), false)
	records[3].WorkerID = "worker-a"
	records[3].Payload = []byte(`{"waiting_on":"continuation:wrong"}`)
	compareStreamingJournal(t, records, []byte(`"ok"`), false)
}

func TestStreamingJournalLongPrefixDoesNotAllocatePerEntryOrEpoch(t *testing.T) {
	const steps = 100000
	allocations := testing.AllocsPerRun(3, func() {
		var state journalAudit
		state.add("test", journal.Record{Entry: journal.Entry{Kind: journal.Started}, Sequence: 1})
		for step := uint64(0); step < steps; step++ {
			state.add("test", journal.Record{Entry: journal.Entry{Index: 1 + 2*step, Epoch: step + 1, WorkerID: "worker", Kind: journal.StepRequested}, Sequence: 2 + 2*step})
			state.add("test", journal.Record{Entry: journal.Entry{Index: 2 + 2*step, Epoch: step + 1, WorkerID: "worker", Kind: journal.StepCompleted}, Sequence: 3 + 2*step})
		}
		count, terminal, err := state.finish("test", func() ([]byte, error) { t.Fatal("terminal read on pending prefix"); return nil, nil })
		if err != nil || terminal || count != 1+2*steps {
			t.Fatalf("count=%d terminal=%v err=%v", count, terminal, err)
		}
	})
	if allocations > 1 {
		t.Fatalf("long prefix allocated %.0f objects", allocations)
	}
}
