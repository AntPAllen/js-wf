package main

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func verifierFixture() (report, []byte) {
	base := time.Unix(1700000000, 0).UTC()
	rep := report{Status: "passed", GoVersion: "go1.27.1", ServerVersion: "2.15.0", Storage: "file", Replicas: 3, Partitions: 64, Count: 3, Published: 3, Received: 3, Horizon: "6s", FirstDue: base, LastDue: base.Add(6 * time.Second), P99Limit: "1s", MaxLateLimit: "2s", P99LateSeconds: 0.001, MaxLateSeconds: 0.001}
	for i := 0; i < 2; i++ {
		at := base.Add(time.Duration(i+1) * 2 * time.Second)
		rep.Restarts = append(rep.Restarts, restart{Started: at, Healed: at.Add(time.Millisecond), ReceivedBefore: i + 1, Killed: []killedProcess{{PID: 1 + 3*i, Signal: "SIGKILL"}, {PID: 2 + 3*i, Signal: "SIGKILL"}, {PID: 3 + 3*i, Signal: "SIGKILL"}}})
	}
	data := make([]byte, 48)
	for i := 0; i < 3; i++ {
		binary.LittleEndian.PutUint64(data[16*i:], uint64(i+100))
		binary.LittleEndian.PutUint64(data[16*i+8:], uint64(base.Add(time.Duration(i)*3*time.Second+time.Millisecond).UnixNano()))
	}
	return rep, data
}
func TestOfflineVerifierRejectsFalsePasses(t *testing.T) {
	for _, name := range []string{"valid", "running", "missing", "duplicate", "early", "wrong_hash", "wrong_percentile", "missing_kill", "undrained", "unobserved_drain", "partial_drain", "retained_source_drain", "truncated"} {
		t.Run(name, func(t *testing.T) {
			rep, data := verifierFixture()
			switch name {
			case "running":
				rep.Status = "running"
			case "missing":
				binary.LittleEndian.PutUint64(data, 0)
			case "duplicate":
				binary.LittleEndian.PutUint64(data[16:], 100)
			case "early":
				binary.LittleEndian.PutUint64(data[8:], uint64(rep.FirstDue.Add(-time.Nanosecond).UnixNano()))
			case "wrong_percentile":
				rep.P99LateSeconds = 0
			case "missing_kill":
				rep.Restarts[1].Killed = nil
			case "undrained":
				rep.FinalMessages = 1
			case "unobserved_drain":
				rep.LastDrainAudit = &drainAudit{}
			case "partial_drain":
				messages, pending := uint64(0), 0
				rep.LastDrainAudit = &drainAudit{Messages: &messages, Pending: &pending, ConsumersChecked: 63}
			case "retained_source_drain":
				messages, pending := uint64(141), 0
				rep.LastDrainAudit = &drainAudit{Messages: &messages, Pending: &pending, ConsumersChecked: 64}
			case "truncated":
				data = data[:47]
			}
			sum := sha256.Sum256(data)
			rep.ObservationsSHA256 = hex.EncodeToString(sum[:])
			if name == "wrong_hash" {
				rep.ObservationsSHA256 = "wrong"
			}
			root := t.TempDir()
			reportBytes, err := json.Marshal(rep)
			if err != nil {
				t.Fatal(err)
			}
			if err = os.WriteFile(filepath.Join(root, "report.json"), reportBytes, 0644); err != nil {
				t.Fatal(err)
			}
			if err = os.WriteFile(filepath.Join(root, "observations.bin"), data, 0644); err != nil {
				t.Fatal(err)
			}
			err = verifyReport(root, true)
			if name == "valid" {
				if err != nil {
					t.Fatal(err)
				}
				if err = verifyReport(root, false); err == nil {
					t.Fatal("short smoke accepted as release evidence")
				}
			} else if err == nil {
				t.Fatal("false pass accepted")
			}
		})
	}
}

func TestMillionReleaseVerifierCannotDowngradeLedgerEvidence(t *testing.T) {
	root := t.TempDir()
	rep, _ := verifierFixture()
	rep.Count = 1000000
	rep.Published, rep.Received = rep.Count, rep.Count
	rep.Horizon = "24h"
	rep.LastDue = rep.FirstDue.Add(24 * time.Hour)
	rep.Revision = strings.Repeat("a", 40)
	rep.SourceModified = "false"
	rep.ReceiptLedger = receiptLedgerVersion
	for i := range rep.Restarts {
		rep.Restarts[i].Started = rep.FirstDue.Add(24 * time.Hour * time.Duration(i+1) / 3)
		rep.Restarts[i].Healed = rep.Restarts[i].Started.Add(time.Millisecond)
	}
	archive := make([]byte, 16*rep.Count)
	ledger := make([]byte, receiptSlotSize*rep.Count)
	for i := 0; i < rep.Count; i++ {
		deadline := due(rep.FirstDue, 24*time.Hour, rep.Count, i)
		sequence := uint64(i + 100)
		at := uint64(deadline.Add(time.Millisecond).UnixNano())
		binary.LittleEndian.PutUint64(archive[16*i:], sequence)
		binary.LittleEndian.PutUint64(archive[16*i+8:], at)
		slot := ledger[receiptSlotSize*i : receiptSlotSize*(i+1)]
		binary.LittleEndian.PutUint64(slot[:8], sequence)
		binary.LittleEndian.PutUint64(slot[8:16], at)
		binary.LittleEndian.PutUint64(slot[16:24], uint64(deadline.UnixNano()))
		hash := sha256.Sum256(slot[:24])
		copy(slot[24:], hash[:16])
	}
	hash := sha256.Sum256(archive)
	rep.ObservationsSHA256 = hex.EncodeToString(hash[:])
	for name, data := range map[string][]byte{"observations.bin": archive, "receipts.bin": ledger} {
		if err := os.WriteFile(filepath.Join(root, name), data, 0644); err != nil {
			t.Fatal(err)
		}
	}
	save := func() {
		t.Helper()
		data, err := json.Marshal(rep)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(root, "report.json"), data, 0644); err != nil {
			t.Fatal(err)
		}
	}
	save()
	if err := verifyReport(root, false); err != nil {
		t.Fatalf("valid million-record artifact rejected: %v", err)
	}
	// Deleting the declaration must not bypass ledger checking, even when the
	// archive, all million deadlines, hashes and release configuration still pass.
	rep.ReceiptLedger = ""
	ledger[8] ^= 1
	if err := os.WriteFile(filepath.Join(root, "receipts.bin"), ledger, 0644); err != nil {
		t.Fatal(err)
	}
	save()
	if err := verifyReport(root, false); err == nil || !strings.Contains(err.Error(), "requires durable receipt ledger") {
		t.Fatalf("downgraded release artifact accepted: %v", err)
	}
	rep.ReceiptLedger = receiptLedgerVersion
	save()
	if err := verifyReport(root, false); err == nil || !strings.Contains(err.Error(), "checksum mismatch") {
		t.Fatalf("corrupt declared ledger accepted: %v", err)
	}
}
