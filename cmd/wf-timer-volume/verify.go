package main

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"time"
)

func verifyReport(root string, allowSmoke bool) error {
	data, err := os.ReadFile(filepath.Join(root, "report.json"))
	if err != nil {
		return err
	}
	var rep report
	if err = json.Unmarshal(data, &rep); err != nil {
		return err
	}
	if rep.ServerCandidateSHA256 != "" {
		if !allowSmoke {
			return fmt.Errorf("diagnostic server candidate does not qualify the release server profile")
		}
		binary, err := os.ReadFile(filepath.Join(root, "nats-server-candidate"))
		if err != nil {
			return err
		}
		digest := sha256.Sum256(binary)
		if hex.EncodeToString(digest[:]) != rep.ServerCandidateSHA256 {
			return fmt.Errorf("retained server candidate digest differs")
		}
	}
	horizon, err := time.ParseDuration(rep.Horizon)
	if err != nil {
		return err
	}
	p99Limit, err := time.ParseDuration(rep.P99Limit)
	if err != nil {
		return err
	}
	maxLate, err := time.ParseDuration(rep.MaxLateLimit)
	if err != nil {
		return err
	}
	if rep.Status != "passed" || rep.Error != "" || rep.Count < 3 || rep.Published != rep.Count || rep.Received != rep.Count || rep.Storage != "file" || rep.Replicas != 3 || rep.Partitions != 64 || rep.FinalMessages != 0 || rep.FinalAckPending != 0 || !rep.LastDue.Equal(rep.FirstDue.Add(horizon)) || len(rep.Restarts) != 2 {
		return fmt.Errorf("incomplete or incompatible campaign report")
	}
	if rep.LastDrainAudit != nil && !rep.LastDrainAudit.complete(rep.Partitions) {
		return fmt.Errorf("incomplete final drain metadata")
	}
	if rep.LastPhysicalDrainAudit != nil && !rep.LastPhysicalDrainAudit.complete(rep.Replicas, rep.Partitions) {
		return fmt.Errorf("incomplete physical replica drain")
	}
	if !allowSmoke && (rep.Count != 1000000 || horizon != 24*time.Hour || p99Limit > 2*time.Second || maxLate > 30*time.Second || rep.SourceModified != "false" || len(rep.Revision) != 40) {
		return fmt.Errorf("report does not meet million-message/24-hour release scope")
	}
	if !allowSmoke && rep.ReceiptLedger != receiptLedgerVersion {
		return fmt.Errorf("release report requires durable receipt ledger")
	}
	if !allowSmoke && (rep.LastDrainAudit == nil || rep.LastPhysicalDrainAudit == nil) {
		return fmt.Errorf("release report requires physical replica drain evidence")
	}
	if p99Limit <= 0 || maxLate < p99Limit {
		return fmt.Errorf("invalid lateness limits")
	}
	for i, event := range rep.Restarts {
		if event.Started.Before(rep.FirstDue.Add(horizon*time.Duration(i+1)/3)) || event.Healed.Before(event.Started) || event.Healed.IsZero() || event.ReceivedBefore <= 0 || event.ReceivedBefore >= rep.Count || len(event.Killed) != 3 {
			return fmt.Errorf("restart %d incomplete", i+1)
		}
		pids := map[int]bool{}
		for _, killed := range event.Killed {
			if killed.PID <= 0 || pids[killed.PID] || killed.Signal != "SIGKILL" {
				return fmt.Errorf("restart %d missing distinct SIGKILL exits", i+1)
			}
			pids[killed.PID] = true
		}
		if i > 0 && (event.Started.Before(rep.Restarts[i-1].Healed) || event.ReceivedBefore <= rep.Restarts[i-1].ReceivedBefore) {
			return fmt.Errorf("restart order/progress invalid")
		}
	}
	data, err = os.ReadFile(filepath.Join(root, "observations.bin"))
	if err != nil {
		return err
	}
	if len(data) != 16*rep.Count {
		return fmt.Errorf("observation count mismatch")
	}
	hash := sha256.Sum256(data)
	if hex.EncodeToString(hash[:]) != rep.ObservationsSHA256 {
		return fmt.Errorf("observation hash mismatch")
	}
	sequences := make(map[uint64]bool, rep.Count)
	late := make([]float64, rep.Count)
	for i := 0; i < rep.Count; i++ {
		sequence := binary.LittleEndian.Uint64(data[16*i:])
		at := int64(binary.LittleEndian.Uint64(data[16*i+8:]))
		if sequence == 0 || sequences[sequence] {
			return fmt.Errorf("missing/duplicate stream sequence at timer %d", i)
		}
		sequences[sequence] = true
		deadline := due(rep.FirstDue, horizon, rep.Count, i)
		if at < deadline.UnixNano() {
			return fmt.Errorf("timer %d received early", i)
		}
		late[i] = time.Unix(0, at).Sub(deadline).Seconds()
	}
	if rep.ReceiptLedger != "" {
		receipts, err := readReceiptLedger(root, rep, horizon)
		if err != nil {
			return err
		}
		for i, receipt := range receipts {
			if receipt.Sequence != binary.LittleEndian.Uint64(data[16*i:]) || receipt.At.UnixNano() != int64(binary.LittleEndian.Uint64(data[16*i+8:])) {
				return fmt.Errorf("timer %d archive differs from durable receipt", i)
			}
		}
	}
	sort.Float64s(late)
	p99 := late[(99*len(late)+99)/100-1]
	maximum := late[len(late)-1]
	if p99 > p99Limit.Seconds() || maximum > maxLate.Seconds() {
		return fmt.Errorf("observation lateness gate failed: p99=%g max=%g", p99, maximum)
	}
	if math.IsNaN(rep.P99LateSeconds) || math.IsNaN(rep.MaxLateSeconds) || math.Abs(p99-rep.P99LateSeconds) > 1e-9 || math.Abs(maximum-rep.MaxLateSeconds) > 1e-9 {
		return fmt.Errorf("reported lateness does not match observations")
	}
	return nil
}
