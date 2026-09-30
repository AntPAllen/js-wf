package integrity

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"js-wf/journal"
)

func TestAuditJournalsBoundedReadsAndTerminalMutations(t *testing.T) {
	subjects := make([]string, 33)
	for i := range subjects {
		subjects[i] = fmt.Sprintf("wf.jrn.test.id-%02d", i)
	}
	for _, mutation := range []string{"", "changed-terminal", "missing-terminal", "late-index"} {
		t.Run(mutation, func(t *testing.T) {
			ctx, done := context.WithTimeout(context.Background(), time.Second)
			defer done()
			var active, peak, calls atomic.Int32
			report, err := auditJournals(ctx, subjects, func(ctx context.Context, subject string) (int, bool, error) {
				calls.Add(1)
				current := active.Add(1)
				defer active.Add(-1)
				for prior := peak.Load(); prior < current && !peak.CompareAndSwap(prior, current); prior = peak.Load() {
				}
				select {
				case <-ctx.Done():
					return 0, false, ctx.Err()
				case <-time.After(50 * time.Millisecond):
				}
				records := []journal.Record{
					{Entry: journal.Entry{Epoch: 1, Index: 0, Kind: journal.Started}, Sequence: 1},
					{Entry: journal.Entry{Epoch: 1, Index: 1, Kind: journal.Completed, Payload: []byte(`42`)}, Sequence: 2},
				}
				if mutation == "late-index" && subject == subjects[32] {
					records[1].Index = 9
				}
				return checkJournalRecords(subject, records, func() ([]byte, error) {
					if subject == subjects[17] {
						switch mutation {
						case "changed-terminal":
							return []byte(`43`), nil
						case "missing-terminal":
							return nil, fmt.Errorf("missing terminal value")
						}
					}
					return []byte(`42`), nil
				})
			})
			if peak.Load() > 16 || peak.Load() < 2 {
				t.Fatalf("concurrent readers=%d", peak.Load())
			}
			if mutation == "" {
				if err != nil || report != (Report{Journals: 33, Entries: 66, Terminal: 33}) || calls.Load() != 33 {
					t.Fatalf("report=%+v calls=%d err=%v", report, calls.Load(), err)
				}
			} else {
				wantSubject := subjects[17]
				if mutation == "late-index" {
					wantSubject = subjects[32]
				}
				if err == nil || !strings.Contains(err.Error(), wantSubject) {
					t.Fatalf("mutation=%s report=%+v err=%v", mutation, report, err)
				}
			}
		})
	}
}

func TestAuditJournalsReportsFirstSortedFailure(t *testing.T) {
	subjects := []string{"wf.jrn.test.first", "wf.jrn.test.second"}
	report, err := auditJournals(context.Background(), subjects, func(_ context.Context, subject string) (int, bool, error) {
		if subject == subjects[0] {
			time.Sleep(20 * time.Millisecond)
		}
		return 0, false, fmt.Errorf("%s: deliberate mutation", subject)
	})
	if err == nil || !strings.Contains(err.Error(), subjects[0]) || report.Journals != 1 {
		t.Fatalf("report=%+v err=%v", report, err)
	}
}
