package reconcile

import (
	"encoding/json"
	"js-wf/retention"
	"testing"
	"time"
)

func TestTerminalProjectionAuditPurgeAndMalformedBounds(t *testing.T) {
	for _, inv := range []uint64{6, 7, 8} {
		value, _ := json.Marshal(retention.Tombstone{Tombstone: true, InvSeq: inv, PurgedAt: time.Unix(100, 0), ExpiresAt: time.Unix(200, 0)})
		got, err := TerminalProjectionAuditable(value, 7)
		if err != nil || got != (inv < 7) {
			t.Fatal(inv, got, err)
		}
	}
	for _, value := range []string{`{"inv_seq":7,"result":"NDI="}`, `{"inv_seq":9,"error":"corrupt"}`, `null`} {
		if got, err := TerminalProjectionAuditable([]byte(value), 7); err != nil || !got {
			t.Fatal(value, got, err)
		}
	}
	for _, value := range []string{`{`, `{"tombstone":true}`, `{"tombstone":"unknown"}`} {
		if got, err := TerminalProjectionAuditable([]byte(value), 7); err == nil || got {
			t.Fatal(value, got, err)
		}
	}
	if _, err := TerminalProjectionAuditable([]byte(`{}`), 0); err == nil {
		t.Fatal("zero generation audited")
	}
}
