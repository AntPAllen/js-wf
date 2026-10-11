package integrity

import (
	"context"
	"testing"

	"js-wf/journal"
)

func TestRawGraphEveryCheckpointHistory(t *testing.T) {
	for _, encoding := range []journal.Encoding{journal.JSON, journal.ProtobufV1} {
		for _, control := range []string{"historical-valid", "historical-state", "historical-anchor", "historical-locals", "historical-cursor", "historical-timer", "historical-metadata"} {
			t.Run(string(encoding)+"/"+control, func(t *testing.T) {
				s, _ := rawJournalCheckpointFixture(t, encoding, true, nil, false, false, control)
				if _, err := CheckGraphReferences(context.Background(), s.Graph); err != nil {
					t.Fatal("mutation did not preserve physical references", err)
				}
				report, err := CheckGraphJournals(context.Background(), s)
				if control == "historical-valid" {
					if err != nil || report.Checkpoints != 2 {
						t.Fatal("checkpoint census", report, err)
					}
				} else if err == nil {
					t.Fatal("older corrupt frame hidden by valid latest checkpoint")
				}
			})
		}
	}
}
