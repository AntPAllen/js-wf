package main

import (
	"encoding/json"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"js-wf/identity"
	"js-wf/provision"
)

type failedReceiptFile struct {
	writes int
	syncs  int
	short  bool
}

func (f *failedReceiptFile) WriteAt(b []byte, offset int64) (int, error) {
	f.writes++
	if f.short {
		return len(b) - 1, nil
	}
	return len(b), nil
}
func (f *failedReceiptFile) Sync() error { f.syncs++; return errors.New("disk sync failed") }

func receiptMessage(base time.Time) (observedMsg, uint32) {
	p := identity.Partition("volume", "0", provision.Partitions)
	return observedMsg{data: []byte("volume.0"), subject: identity.RunSubject("volume", "0", provision.Partitions), header: nats.Header{identity.TimerInvSeqHeader: {"1"}, identity.TimerStepHeader: {"0"}}, meta: jetstream.MsgMetadata{Timestamp: base.Add(time.Second), Sequence: jetstream.SequencePair{Stream: 100}}}, p
}

func TestReceiptFailurePreventsAcknowledgableObservation(t *testing.T) {
	for _, short := range []bool{false, true} {
		f := &failedReceiptFile{short: short}
		c := campaign{cfg: config{Count: 3, Horizon: 6 * time.Second}, base: time.Now().Add(-10 * time.Second), seen: make([]observation, 3), ledger: f}
		msg, p := receiptMessage(c.base)
		err := c.observe(msg, p)
		if err == nil || c.rep.Received != 0 || c.seen[0].Sequence != 0 {
			t.Fatalf("failed persistence accepted: err=%v seen=%+v", err, c.seen)
		}
		if short && !errors.Is(err, io.ErrShortWrite) {
			t.Fatal(err)
		}
		// A failed write is sticky: subsequent messages cannot become ACK eligible.
		if err := c.observe(msg, p); err == nil || f.writes != 1 {
			t.Fatalf("write retried after unknown durability: %v writes=%d", err, f.writes)
		}
	}
}

func TestDurableReceiptSurvivesReopenAndDetectsDamage(t *testing.T) {
	root := t.TempDir()
	base := time.Now().Add(-10 * time.Second).UTC()
	rep := report{Count: 3, FirstDue: base, LastDue: base.Add(6 * time.Second), ReceiptLedger: receiptLedgerVersion}
	path := filepath.Join(root, "receipts.bin")
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.Truncate(3 * receiptSlotSize); err != nil {
		t.Fatal(err)
	}
	c := campaign{cfg: config{Count: 3, Horizon: 6 * time.Second}, base: base, seen: make([]observation, 3), ledger: f}
	msg, p := receiptMessage(base)
	if err := c.observe(msg, p); err != nil {
		t.Fatal(err)
	}
	first := c.seen[0]
	if err := c.observe(msg, p); err != nil {
		t.Fatal(err)
	}
	if c.seen[0] != first {
		t.Fatal("redelivery replaced first receipt")
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	seen, err := readReceiptLedger(root, rep, 6*time.Second)
	if err != nil || seen[0].Sequence != first.Sequence || !seen[0].At.Equal(first.At) || seen[1].Sequence != 0 {
		t.Fatalf("reopen lost receipt: %v %+v", err, seen)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	data[8] ^= 1
	if err := os.WriteFile(path, data, 0644); err != nil {
		t.Fatal(err)
	}
	if _, err := readReceiptLedger(root, rep, 6*time.Second); err == nil || !strings.Contains(err.Error(), "checksum") {
		t.Fatalf("corrupt slot accepted: %v", err)
	}
}

func TestReceiptLedgerProcessKillHelper(t *testing.T) {
	root := os.Getenv("WF_RECEIPT_KILL_HELPER")
	if root == "" {
		t.Skip("receipt process-kill helper")
	}
	base := time.Now().Add(-10 * time.Second).UTC()
	f, err := os.OpenFile(filepath.Join(root, "receipts.bin"), os.O_CREATE|os.O_EXCL|os.O_RDWR, 0644)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.Truncate(3 * receiptSlotSize); err != nil {
		t.Fatal(err)
	}
	c := campaign{cfg: config{Root: root, Count: 3, Horizon: 6 * time.Second}, base: base, seen: make([]observation, 3), ledger: f,
		rep: report{Status: "running", Count: 3, Horizon: "6s", FirstDue: base, LastDue: base.Add(6 * time.Second), ReceiptLedger: receiptLedgerVersion}}
	if err := c.save(); err != nil {
		t.Fatal(err)
	}
	msg, p := receiptMessage(base)
	if err := c.observe(msg, p); err != nil {
		t.Fatal(err)
	}
	// No final archive, report checkpoint or deferred close can run after this.
	if err := syscall.Kill(os.Getpid(), syscall.SIGKILL); err != nil {
		t.Fatal(err)
	}
	select {}
}

func TestReceiptRecoveryAfterActualProcessKill(t *testing.T) {
	root := t.TempDir()
	child := exec.Command(os.Args[0], "-test.run=^TestReceiptLedgerProcessKillHelper$")
	child.Env = append(os.Environ(), "WF_RECEIPT_KILL_HELPER="+root)
	output, err := child.CombinedOutput()
	var exit *exec.ExitError
	if !errors.As(err, &exit) {
		t.Fatalf("child was not killed: %v %s", err, output)
	}
	status, ok := exit.Sys().(syscall.WaitStatus)
	if !ok || !status.Signaled() || status.Signal() != syscall.SIGKILL {
		t.Fatalf("child exit=%v %s", err, output)
	}
	if _, err := os.Stat(filepath.Join(root, "observations.bin")); !os.IsNotExist(err) {
		t.Fatalf("unexpected final archive: %v", err)
	}
	if err := recoverObservations(root); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(root, "receipt-recovery", "report.json"))
	if err != nil {
		t.Fatal(err)
	}
	var rep report
	if err := json.Unmarshal(data, &rep); err != nil {
		t.Fatal(err)
	}
	if rep.Received != 1 || rep.Status != "interrupted" || rep.Error == "" {
		t.Fatalf("bad recovery report: %+v", rep)
	}
	if err := verifyReport(filepath.Join(root, "receipt-recovery"), true); err == nil {
		t.Fatal("partial receipts accepted as completed campaign")
	}
}
