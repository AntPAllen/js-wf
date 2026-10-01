package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"
)

// A fixed slot per timer retains the first receipt before DoubleAck can remove
// the message. The checksum detects an interrupted or corrupted slot write.
// Zero slots denote timers not yet observed; recovery never marks a run passed.
const receiptSlotSize = 40
const receiptLedgerVersion = "indexed-sha256-128-v1"

type receiptFile interface {
	WriteAt([]byte, int64) (int, error)
	Sync() error
}

func (c *campaign) persistReceipt(index int, sequence uint64, at, server time.Time) error {
	if c.ledger == nil {
		return fmt.Errorf("receipt ledger is not open")
	}
	if c.ledgerErr != nil {
		return c.ledgerErr
	}
	var slot [receiptSlotSize]byte
	binary.LittleEndian.PutUint64(slot[0:8], sequence)
	binary.LittleEndian.PutUint64(slot[8:16], uint64(at.UnixNano()))
	binary.LittleEndian.PutUint64(slot[16:24], uint64(server.UnixNano()))
	sum := sha256.Sum256(slot[:24])
	copy(slot[24:], sum[:16])
	n, err := c.ledger.WriteAt(slot[:], int64(index)*receiptSlotSize)
	if err == nil && n != len(slot) {
		err = io.ErrShortWrite
	}
	if err == nil {
		err = c.ledger.Sync()
	}
	if err != nil {
		c.ledgerErr = fmt.Errorf("persist timer %d before acknowledgment: %w", index, err)
		return c.ledgerErr
	}
	return nil
}

func readReceiptLedger(root string, rep report, horizon time.Duration) ([]observation, error) {
	if rep.ReceiptLedger != receiptLedgerVersion || rep.Count < 3 || rep.Count > 10000000 || rep.FirstDue.IsZero() || horizon <= 0 || !rep.LastDue.Equal(rep.FirstDue.Add(horizon)) {
		return nil, fmt.Errorf("incompatible receipt ledger identity")
	}
	f, err := os.Open(filepath.Join(root, "receipts.bin"))
	if err != nil {
		return nil, err
	}
	defer f.Close()
	stat, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if stat.Size() != int64(rep.Count)*receiptSlotSize {
		return nil, fmt.Errorf("receipt ledger size mismatch")
	}
	seen := make([]observation, rep.Count)
	sequences := make(map[uint64]bool)
	var slot [receiptSlotSize]byte
	var zero [receiptSlotSize]byte
	for i := range seen {
		if _, err := io.ReadFull(f, slot[:]); err != nil {
			return nil, err
		}
		if bytes.Equal(slot[:], zero[:]) {
			continue
		}
		sum := sha256.Sum256(slot[:24])
		if !bytes.Equal(slot[24:], sum[:16]) {
			return nil, fmt.Errorf("receipt %d checksum mismatch; interrupted or corrupt write", i)
		}
		sequence := binary.LittleEndian.Uint64(slot[:8])
		at := time.Unix(0, int64(binary.LittleEndian.Uint64(slot[8:16])))
		server := time.Unix(0, int64(binary.LittleEndian.Uint64(slot[16:24])))
		deadline := due(rep.FirstDue, horizon, rep.Count, i)
		if sequence == 0 || sequences[sequence] || at.Before(deadline) || server.Before(deadline) {
			return nil, fmt.Errorf("receipt %d has invalid sequence or early delivery", i)
		}
		sequences[sequence] = true
		seen[i] = observation{Sequence: sequence, At: at}
	}
	return seen, nil
}

// Sync the contents and the rename. Atomic rename alone does not make a
// checkpoint durable across a host crash.
func durableReplace(path string, data []byte) (err error) {
	f, err := os.OpenFile(path+".tmp", os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	if _, err = f.Write(data); err == nil {
		err = f.Sync()
	}
	closeErr := f.Close()
	if err == nil {
		err = closeErr
	}
	if err != nil {
		return err
	}
	if err = os.Rename(path+".tmp", path); err != nil {
		return err
	}
	return syncDirectory(filepath.Dir(path))
}

func syncDirectory(path string) error {
	dir, err := os.Open(path)
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}

func recoverObservations(root string) error {
	data, err := os.ReadFile(filepath.Join(root, "report.json"))
	if err != nil {
		return err
	}
	var rep report
	if err := json.Unmarshal(data, &rep); err != nil {
		return err
	}
	horizon, err := time.ParseDuration(rep.Horizon)
	if err != nil {
		return err
	}
	seen, err := readReceiptLedger(root, rep, horizon)
	if err != nil {
		return err
	}
	output := filepath.Join(root, "receipt-recovery")
	if err := os.Mkdir(output, 0755); err != nil {
		return err
	}
	if err := syncDirectory(root); err != nil {
		return err
	}
	rep.Status = "interrupted"
	rep.Error = "offline receipt recovery; original campaign completion and final queue audit are unproven"
	rep.Received = 0
	for _, receipt := range seen {
		if receipt.Sequence != 0 {
			rep.Received++
		}
	}
	c := campaign{cfg: config{Root: output, Count: rep.Count, Horizon: horizon}, rep: rep, base: rep.FirstDue, seen: seen}
	if err := c.saveObservations(); err != nil {
		return err
	}
	if err := c.save(); err != nil {
		return err
	}
	fmt.Printf("recovered %d/%d receipts to %s; campaign remains interrupted\n", rep.Received, rep.Count, output)
	return nil
}
