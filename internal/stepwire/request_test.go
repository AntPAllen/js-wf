package stepwire

import "testing"

func TestRequestVariantsRemainReadable(t *testing.T) {
	for _, raw := range []string{
		`{"name":"legacy","input_hash":"hash"}`,
		`{"kind":"run","name":"step","input_hash":"hash"}`,
		`{"kind":"checkpoint","name":"next","input_hash":"hash"}`,
		`{"kind":"call","name":"result","input_hash":"hash","child_type":"child","child_id":"id"}`,
		`{"kind":"timer","name":"sleep","duration_nanos":10,"fire_at":"2026-10-10T00:00:00Z","clock_domain":"clock","timer_step":2,"timer_name":"sleep"}`,
		`{"kind":"select_many","cases":[{"kind":"timer","name":"sleep","timer_step":2,"fire_at":"2026-10-10T00:00:00Z","clock_domain":"clock"},{"kind":"promise","name":"result","child_type":"child","child_id":"id"}]}`,
	} {
		var request Request
		if err := Decode([]byte(raw), &request); err != nil {
			t.Fatalf("valid declaration %s: %v", raw, err)
		}
	}
}
