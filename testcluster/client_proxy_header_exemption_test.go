package testcluster

import (
	"fmt"
	"testing"
)

func TestPublicationExemptionOnlyExaminesHeaders(t *testing.T) {
	marker := "Wf-Authority-Read-Witness"
	headers := "NATS/1.0\r\n" + marker + ": 1\r\n\r\n"
	for _, tc := range []struct {
		name, packet string
		want         bool
	}{
		{"actual-header", fmt.Sprintf("HPUB test _reply %d %d\r\n%sbody\r\n", len(headers), len(headers)+4, headers), true},
		{"payload-only", fmt.Sprintf("HPUB test _reply 12 %d\r\nNATS/1.0\r\n\r\n%s: 1\r\n", 12+len(marker)+3, marker), false},
		{"plain-pub-payload", "PUB test 40\r\n" + headers + "\r\n", false},
		{"wrong-value", "HPUB test 40 40\r\nNATS/1.0\r\n" + marker + ": 2\r\n\r\n", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := publicationHasHeader([]byte(tc.packet), marker, "1"); got != tc.want {
				t.Fatal(got, tc.want)
			}
		})
	}
}
