package journal_test

import (
	"fmt"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"js-wf/journal"
	"strings"
	"testing"
)

func TestGraphStartMatchesInvocationHeaders(t *testing.T) {
	for _, parent := range []bool{false, true} {
		controls := []string{"valid", "transport", "input-ref-empty", "input-ref", "pointer", "nil", "sequence", "subject"}
		keys := []string{"Wf-Graph-Start-Token", "Wf-Input-SHA256"}
		parentKeys := []string{"Wf-Parent-Type", "Wf-Parent-ID", "Wf-Parent-Inv-Seq", "Wf-Parent-Signal"}
		if parent {
			keys = append(keys, parentKeys...)
		} else {
			for _, key := range parentKeys {
				controls = append(controls, key+"/unexpected-empty", key+"/unexpected-alias")
			}
		}
		for _, key := range keys {
			for _, mode := range []string{"duplicate", "conflicting", "empty-list", "alias", "missing"} {
				controls = append(controls, key+"/"+mode)
			}
		}
		for _, control := range controls {
			t.Run(fmt.Sprintf("parent=%t/%s", parent, control), func(t *testing.T) {
				start := journal.GraphStart{Schema: "js-wf-canonical-start-v1", Token: "start", Request: journal.GraphStartRequest{Type: "flow", ID: "root"}, InputSHA256: strings.Repeat("a", 64), InputSize: 5}
				h := nats.Header{"Wf-Graph-Start-Token": []string{"start"}, "Wf-Input-SHA256": []string{start.InputSHA256}}
				if parent {
					start.Request.ParentType = "flow"
					start.Request.ParentID = "parent"
					start.Request.ParentInvocation = 3
					start.Request.SignalName = "child_0"
					h.Set("Wf-Parent-Type", "flow")
					h.Set("Wf-Parent-ID", "parent")
					h.Set("Wf-Parent-Inv-Seq", "3")
					h.Set("Wf-Parent-Signal", "child_0")
				}
				msg := &jetstream.RawStreamMsg{Subject: "wf.inv.flow.root", Sequence: 10, Data: start.PointerBytes(), Header: h}
				switch control {
				case "transport":
					h.Set("Nats-Expected-Last-Subject-Sequence", "0")
					h.Set("X-Trace", "opaque")
				case "input-ref-empty":
					h.Set("Wf-Input-Ref", "")
				case "input-ref":
					h.Set("Wf-Input-Ref", "external")
				case "pointer":
					msg.Data = append(msg.Data, ' ')
				case "nil":
					msg = nil
				case "sequence":
					msg.Sequence = 0
				case "subject":
					msg.Subject = "wf.inv.flow.other"
				default:
					parts := strings.Split(control, "/")
					if len(parts) == 2 {
						key, mode := parts[0], parts[1]
						v := h[key]
						switch mode {
						case "duplicate":
							h[key] = append(v, v[0])
						case "conflicting":
							h[key] = append(v, "conflict")
						case "empty-list":
							h[key] = nil
						case "alias":
							delete(h, key)
							h[strings.ToLower(key)] = v
						case "missing":
							delete(h, key)
						case "unexpected-empty":
							h[key] = []string{""}
						case "unexpected-alias":
							h[strings.ToLower(key)] = []string{""}
						}
					}
				}
				want := control == "valid" || control == "transport"
				if got := start.MatchesInvocation(msg); got != want {
					t.Fatalf("source match=%t want=%t", got, want)
				}
			})
		}
	}
}
