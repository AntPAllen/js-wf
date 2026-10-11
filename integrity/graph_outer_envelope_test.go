package integrity

import (
	"context"
	"js-wf/journal"
	"testing"
)

func TestRawGraphCanonicalEnvelope(t *testing.T) {
	for _, encoding := range []journal.Encoding{journal.JSON, journal.ProtobufV1} {
		for _, control := range []string{"outer-valid", "outer-whitespace", "outer-unknown", "outer-alias", "outer-duplicate", "outer-escaped-duplicate", "outer-generation", "outer-sequence", "outer-hash"} {
			t.Run(string(encoding)+"/"+control, func(t *testing.T) {
				s, _ := rawJournalCheckpointFixture(t, encoding, false, nil, false, false, control)
				if _, err := CheckGraphReferences(context.Background(), s.Graph); err != nil {
					t.Fatal("invalid physical references", err)
				}
				_, err := CheckGraphJournals(context.Background(), s)
				valid := control == "outer-valid" || control == "outer-whitespace"
				if valid && err != nil {
					t.Fatal(err)
				}
				if !valid && err == nil {
					t.Fatal("corrupt canonical envelope accepted")
				}
			})
		}
	}
}
