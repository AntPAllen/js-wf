package stepwire

import (
	"encoding/json"
	"testing"
)

func TestNullRuntimeMetadataAndOpaqueResults(t *testing.T) {
	for _, level := range []string{"request", "case", "completion"} {
		fields := map[string]any{"kind": "", "name": "", "input_hash": "", "duration_nanos": int64(0), "fire_at": "0001-01-01T00:00:00Z", "clock_domain": "", "timer_step": uint64(0), "timer_name": "", "child_type": "", "child_id": "", "cases": []any{}}
		if level == "case" {
			fields = map[string]any{"kind": "", "name": "", "timer_step": uint64(0), "fire_at": "0001-01-01T00:00:00Z", "clock_domain": "", "child_type": "", "child_id": ""}
		}
		if level == "completion" {
			fields = map[string]any{"case_index": 0, "result_ref": "", "result_hash": "", "error": "", "error_kind": "", "signal_seq": uint64(0), "selected": "", "cancelled": false, "checkpoint_metadata_ref": "", "checkpoint_metadata_hash": ""}
		}
		for field, value := range fields {
			for _, valid := range []bool{false, true} {
				name := "null"
				if valid {
					name = "zero"
				}
				t.Run(level+"/"+field+"/"+name, func(t *testing.T) {
					selected := value
					if !valid {
						selected = nil
					}
					body := map[string]any{field: selected}
					if level == "case" {
						body = map[string]any{"kind": "select_many", "cases": []any{body}}
					}
					raw, _ := json.Marshal(body)
					var err error
					if level == "completion" {
						var completion Completion
						err = DecodeCompletion(raw, &completion)
					} else {
						var request Request
						err = Decode(raw, &request)
					}
					if (err == nil) != valid {
						t.Fatalf("valid=%t raw=%s error=%v", valid, raw, err)
					}
				})
			}
		}
	}
	t.Run("null-case", func(t *testing.T) {
		var request Request
		if err := Decode([]byte(`{"kind":"select_many","cases":[null]}`), &request); err == nil {
			t.Fatal("null case accepted")
		}
	})
	for _, raw := range []string{`{"result":null}`, `{"result":{"error":null,"kind":null,"x":1,"x":2}}`, `{}`} {
		t.Run("opaque/"+raw, func(t *testing.T) {
			var completion Completion
			if err := DecodeCompletion([]byte(raw), &completion); err != nil {
				t.Fatal(err)
			}
		})
	}
}
