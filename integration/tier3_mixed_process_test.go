//go:build linux

package integration_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"syscall"
	"testing"

	"js-wf/worker"
)

type tier3ProxySpec struct {
	Worker    string `json:"worker_id"`
	PID       int    `json:"pid"`
	Slot      int    `json:"slot"`
	ProxyURL  string `json:"proxy_url"`
	ServerURL string `json:"server_url"`
}

type tier3ProcessEvidence struct {
	ExitSuccess         bool            `json:"exit_success"`
	ExitSignal          int             `json:"exit_signal"`
	Worker              string          `json:"worker_id"`
	PID                 int             `json:"pid"`
	Generation          int             `json:"generation"`
	DispatchRecords     int             `json:"dispatch_records"`
	FencingRecords      int             `json:"fencing_records"`
	PartialDispatchTail bool            `json:"partial_dispatch_tail"`
	PartialFencingTail  bool            `json:"partial_fencing_tail"`
	FinalMetrics        *worker.Metrics `json:"final_metrics,omitempty"`
}

// Interrupted tails are retained in the original file, never counted as a
// complete event. Missing final metrics mean no graceful counter cross-check.
func readTier3ProcessRecords[T any](path string) ([]T, bool, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, false, err
	}
	end := bytes.LastIndexByte(data, '\n') + 1
	partial := end != len(data)
	var records []T
	for _, line := range bytes.Split(data[:end], []byte{'\n'}) {
		if len(line) == 0 {
			continue
		}
		var record T
		if err := json.Unmarshal(line, &record); err != nil {
			return nil, partial, fmt.Errorf("%s: %w", path, err)
		}
		records = append(records, record)
	}
	return records, partial, nil
}

func readTier3ProcessEvidence(p *matrixProcessWorker) (tier3ProcessEvidence, []worker.DispatchEvent, []worker.FencingEvent, error) {
	proof := tier3ProcessEvidence{Worker: p.id, PID: p.cmd.Process.Pid, Generation: p.generation}
	if p.cmd.ProcessState == nil {
		return proof, nil, nil, fmt.Errorf("process still live: %s", p.id)
	}
	proof.ExitSuccess = p.cmd.ProcessState.Success()
	if status, ok := p.cmd.ProcessState.Sys().(syscall.WaitStatus); ok && status.Signaled() {
		proof.ExitSignal = int(status.Signal())
	}

	steps, partial, err := readTier3ProcessRecords[worker.DispatchEvent](p.base + "-dispatch.jsonl")
	if err != nil {
		return proof, nil, nil, err
	}
	proof.DispatchRecords, proof.PartialDispatchTail = len(steps), partial
	records, partial, err := readTier3ProcessRecords[matrixProcessFencingRecord](p.base + "-fencing.jsonl")
	if err != nil {
		return proof, nil, nil, err
	}
	proof.FencingRecords, proof.PartialFencingTail = len(records), partial
	var fences []worker.FencingEvent
	for i, r := range records {
		if r.PID != proof.PID || r.Event.Worker != p.id || r.Sequence != uint64(i+1) {
			return proof, nil, nil, fmt.Errorf("process fencing identity/sequence: %s", p.id)
		}
		fences = append(fences, r.Event)
	}
	for _, e := range steps {
		if e.Worker != p.id {
			return proof, nil, nil, fmt.Errorf("process dispatch identity: %s", p.id)
		}
	}
	data, err := os.ReadFile(p.base + "-metrics.json")
	if err == nil {
		var final matrixProcessMetrics
		if err = json.Unmarshal(data, &final); err != nil {
			return proof, nil, nil, err
		}
		if !proof.ExitSuccess || final.PID != proof.PID || final.Worker != p.id || final.Metrics.FencingEvents != uint64(len(records)) || proof.PartialDispatchTail || proof.PartialFencingTail {
			return proof, nil, nil, fmt.Errorf("graceful process evidence mismatch: %s", p.id)
		}
		proof.FinalMetrics = &final.Metrics
	} else if !os.IsNotExist(err) {
		return proof, nil, nil, err
	}
	return proof, steps, fences, nil
}

func TestTier3ProcessRecordReaderPreservesInterruptedTail(t *testing.T) {
	path := t.TempDir() + "/events.jsonl"
	data := []byte("{\"pid\":7,\"sequence\":1}\n{\"pid\":")
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	records, partial, err := readTier3ProcessRecords[matrixProcessFencingRecord](path)
	if err != nil || !partial || len(records) != 1 || records[0].PID != 7 {
		t.Fatalf("records=%+v partial=%v err=%v", records, partial, err)
	}
	unchanged, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(data, unchanged) {
		t.Fatalf("interrupted original changed: %s err=%v", unchanged, err)
	}
	if err := os.WriteFile(path, []byte("{broken}\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, _, err := readTier3ProcessRecords[matrixProcessFencingRecord](path); err == nil {
		t.Fatal("accepted malformed complete record")
	}
}
