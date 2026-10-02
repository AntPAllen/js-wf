//go:build linux

package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"js-wf/identity"
	"js-wf/integrity"
	"js-wf/journal"
)

// Both contenders pass production's pre-read before either publish arrives.
// The intact server CAS accepts one matching result; the mutant accepts two
// physical entries at the same logical index. Detection uses retained bytes
// and the raw-state checker before any attempted recovery of that corruption.
func challengeMixedCAS(t *testing.T, ctx context.Context, nodes []jetstream.JetStream, survivor int, typ, id string, prefix []journal.Record) {
	t.Helper()
	pair := [2]jetstream.JetStream{nodes[survivor], nodes[(survivor+1)%3]}
	var streams [2]jetstream.Stream
	for i, js := range pair {
		var err error
		for until := time.Now().Add(20 * time.Second); time.Now().Before(until) && ctx.Err() == nil; {
			attempt, stop := context.WithTimeout(ctx, 2*time.Second)
			streams[i], err = js.Stream(attempt, "WF_JRN")
			stop()
			if err == nil {
				break
			}
			time.Sleep(50 * time.Millisecond)
		}
		if err != nil {
			t.Fatal("CAS stream readiness", err)
		}
	}
	records, expected, err := journal.New(pair[0]).Read(ctx, typ, id)
	if err != nil || len(records) != len(prefix) {
		t.Fatalf("CAS pending prefix %+v %v", records, err)
	}
	entry := journal.Entry{Kind: journal.StepCompleted, Index: prefix[len(prefix)-1].Index + 1, Epoch: prefix[len(prefix)-1].Epoch, WorkerID: prefix[len(prefix)-1].WorkerID, Payload: []byte(`{"result":42}`)}
	arrived := make(chan struct{}, 2)
	release := make(chan struct{})
	results := make(chan casRoundOutcome, 2)
	for i, js := range pair {
		gate := &casPublishGate{JetStream: js, subject: identity.JournalSubject(typ, id), stream: streams[i], arrived: arrived, release: release}
		go func() {
			seq, err := journal.New(gate).Append(ctx, typ, id, entry, expected)
			results <- casRoundOutcome{seq: seq, err: err}
		}()
	}
	for i := 0; i < 2; i++ {
		select {
		case <-arrived:
		case <-ctx.Done():
			close(release)
			t.Fatal("CAS publish gate admission", ctx.Err())
		}
	}
	t.Logf("MIXED_CAS_ADMISSION contenders=2 expected=%d index=%d epoch=%d", expected, entry.Index, entry.Epoch)
	close(release)
	wins, stales := 0, 0
	var winning []uint64
	for i := 0; i < 2; i++ {
		result := <-results
		if result.err == nil {
			wins++
			winning = append(winning, result.seq)
		} else if errors.Is(result.err, journal.ErrStale) {
			stales++
		} else {
			t.Fatal("unrelated CAS append error", result.err)
		}
	}
	if wins == 1 && stales == 1 {
		return
	}
	if wins != 2 || stales != 0 || winning[0] == winning[1] {
		t.Fatalf("unexpected CAS outcomes wins=%d stales=%d seqs=%v", wins, stales, winning)
	}
	for _, seq := range winning {
		raw, err := streams[0].GetMsg(ctx, seq)
		if err != nil {
			t.Fatal("acknowledged CAS write missing", err)
		}
		var retained journal.Entry
		if err := journal.UnmarshalEntry(raw.Data, &retained); err != nil || raw.Subject != identity.JournalSubject(typ, id) || retained.Index != entry.Index || retained.Epoch != entry.Epoch || retained.Kind != journal.StepCompleted {
			t.Fatalf("CAS retained write seq=%d entry=%+v err=%v", seq, retained, err)
		}
		t.Logf("MIXED_CAS_RETAINED sequence=%d same_index=%d epoch=%d", seq, retained.Index, retained.Epoch)
		proof, err := json.Marshal(raw)
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("MIXED_CAS_RAW_RECEIPT %s", proof)
	}
	_, readErr := func() ([]journal.Record, error) { r, _, err := journal.New(pair[0]).Read(ctx, typ, id); return r, err }()
	if !errors.Is(readErr, journal.ErrGap) {
		t.Fatal("duplicate index did not corrupt journal read", readErr)
	}
	_, checkErr := integrity.Check(ctx, pair[0])
	if checkErr == nil || !strings.Contains(checkErr.Error(), fmt.Sprintf("%s: index %d at position %d", identity.JournalSubject(typ, id), entry.Index, entry.Index+1)) {
		t.Fatal("raw-state checker did not report the target duplicate index", checkErr)
	}
	t.Logf("MIXED_CAS_CHECKER_REJECTED error=%v", checkErr)
	t.Fatalf("MIXED_MUTATION_ESCAPE category=missing_cas acknowledged_winners=2 stale_rejections=0 same_index=%d retained_duplicates=2 checker_rejected=true", entry.Index)
}
