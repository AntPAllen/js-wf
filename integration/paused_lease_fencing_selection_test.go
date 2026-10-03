//go:build linux

package integration_test

import (
	"encoding/json"
	"js-wf/worker"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestPausedLeaseFencingSelection(t *testing.T) {
	path := filepath.Join(t.TempDir(), "fencing.jsonl")
	at := time.Now().UTC()
	leases := []matrixPausedLease{{Key: "test.paused", Epoch: 7}}
	records := []matrixProcessFencingRecord{
		{PID: 123, Sequence: 1, Event: worker.FencingEvent{Worker: "owner", Type: "test", ID: "other", Epoch: 7, At: at}},
		{PID: 123, Sequence: 2, Event: worker.FencingEvent{Worker: "owner", Type: "test", ID: "paused", Epoch: 8, At: at}},
		{PID: 123, Sequence: 3, Event: worker.FencingEvent{Worker: "owner", Type: "test", ID: "paused", Epoch: 7, At: at.Add(-time.Second)}},
		{PID: 123, Sequence: 4, Event: worker.FencingEvent{Worker: "owner", Type: "test", ID: "paused", Epoch: 7, At: at}},
	}
	write := func(records []matrixProcessFencingRecord, tail string) {
		t.Helper()
		var data []byte
		for _, r := range records {
			b, err := json.Marshal(r)
			if err != nil {
				t.Fatal(err)
			}
			data = append(data, append(b, '\n')...)
		}
		if err := os.WriteFile(path, append(data, tail...), 0600); err != nil {
			t.Fatal(err)
		}
	}
	write(records[:3], "")
	if n, err := matrixPausedLeaseFencingEvents(path, 123, "owner", at, leases); err != nil || n != 0 {
		t.Fatalf("unrelated records admitted: count=%d err=%v", n, err)
	}
	write(records, `{"unfinished":`)
	if n, err := matrixPausedLeaseFencingEvents(path, 123, "owner", at, leases); err != nil || n != 1 {
		t.Fatalf("retained paused lease: count=%d err=%v", n, err)
	}
	records[3].PID = 124
	write(records, "")
	if _, err := matrixPausedLeaseFencingEvents(path, 123, "owner", at, leases); err == nil {
		t.Fatal("foreign PID admitted")
	}
	records[3].PID = 123
	records[3].Sequence = 5
	write(records, "")
	if _, err := matrixPausedLeaseFencingEvents(path, 123, "owner", at, leases); err == nil {
		t.Fatal("noncontiguous records admitted")
	}
}
