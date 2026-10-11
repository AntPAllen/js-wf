package integrity

import "encoding/json"

// Own complete terminal wire shape. User result bytes and retained rejected
// request/payload bytes are opaque; boundary semantics are a separate check.
type auditedGraphOutcome struct {
	InvSeq       uint64          `json:"inv_seq"`
	Result       []byte          `json:"result"`
	ResultRef    string          `json:"result_ref"`
	ResultHash   string          `json:"result_hash"`
	Error        string          `json:"error"`
	LimitRequest json.RawMessage `json:"limit_request"`
	LimitEntry   *struct {
		Kind    string          `json:"kind"`
		Payload json.RawMessage `json:"payload"`
	} `json:"limit_entry"`
}
