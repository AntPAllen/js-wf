package wf

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
)

// Outcome is the terminal value stored in both the journal and WF_STATE.
type Outcome struct {
	InvSeq     uint64 `json:"inv_seq,omitempty"`
	Result     []byte `json:"result,omitempty"`
	ResultRef  string `json:"result_ref,omitempty"`
	ResultHash string `json:"result_hash,omitempty"`
	Error      string `json:"error,omitempty"`
}

const MaxInlineTerminal = 600 * 1024

// ResultBytes resolves and verifies an Object Store-backed terminal result.
func (o Outcome) ResultBytes(ctx context.Context, load func(context.Context, string) ([]byte, error)) ([]byte, error) {
	if o.ResultRef == "" {
		if o.ResultHash != "" {
			return nil, ErrCorruptJournal
		}
		return o.Result, nil
	}
	if load == nil || o.ResultHash == "" || len(o.Result) != 0 {
		return nil, ErrCorruptJournal
	}
	data, err := load(ctx, o.ResultRef)
	if err != nil {
		return nil, fmt.Errorf("load terminal result: %w", err)
	}
	digest := sha256.Sum256(data)
	if hex.EncodeToString(digest[:]) != o.ResultHash {
		return nil, ErrCorruptJournal
	}
	return data, nil
}
