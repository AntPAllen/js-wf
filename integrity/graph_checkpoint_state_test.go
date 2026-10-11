package integrity

import (
	"context"
	"fmt"
	"testing"

	"js-wf/journal"
)

func TestRawGraphCheckpointSDKStateHistory(t *testing.T) {
	for _, enc := range []journal.Encoding{journal.JSON, journal.ProtobufV1} {
		for _, mode := range []string{"history-valid", "history-external-valid", "history-read-absent-valid", "history-write-error-valid", "history-null-valid"} {
			t.Run(fmt.Sprintf("%s/%s", enc, mode), func(t *testing.T) {
				s, _ := rawJournalCheckpointFixture(t, enc, true, nil, false, false, mode)
				r, err := CheckGraphJournals(context.Background(), s)
				if err != nil || r.Entries != 6 || r.Terminal != 1 {
					t.Fatal(r, err)
				}
			})
		}
	}
	for _, mode := range []string{"history-frame-value", "history-frame-missing", "history-frame-extra", "history-write-hash", "history-write-missing", "history-write-error", "history-read-fabricated", "history-result-null", "history-result-unknown", "history-external-unowned", "history-external-wrong"} {
		t.Run(mode, func(t *testing.T) {
			s, _ := rawJournalCheckpointFixture(t, journal.JSON, true, nil, false, false, mode)
			if _, err := CheckGraphReferences(context.Background(), s.Graph); err != nil {
				t.Fatal("control is not reference-valid", err)
			}
			if _, err := CheckGraphJournals(context.Background(), s); err == nil {
				t.Fatal("SDK state corruption accepted")
			}
		})
	}
}
