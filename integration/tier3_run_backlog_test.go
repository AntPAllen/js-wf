//go:build linux

package integration_test

import (
	"context"
	"encoding/json"
	"os"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// A failed drain remains failed. This independent, read-only snapshot runs
// after its deadline and cannot extend or satisfy the drainage gate.
func captureMatrixRunBacklog(path string, run jetstream.Stream, last *jetstream.StreamInfo) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	type observation struct {
		Sequence    uint64                  `json:"sequence"`
		Before      time.Time               `json:"before"`
		After       time.Time               `json:"after"`
		Message     *jetstream.RawStreamMsg `json:"message,omitempty"`
		DataBytes   int                     `json:"data_bytes,omitempty"`
		DataOmitted bool                    `json:"data_omitted,omitempty"`
		Error       string                  `json:"error,omitempty"`
	}
	proof := struct {
		Info         *jetstream.StreamInfo `json:"last_observed_info"`
		Reads        []observation         `json:"reads"`
		Truncated    bool                  `json:"truncated"`
		GatePassed   bool                  `json:"drain_gate_passed"`
		ContextError string                `json:"context_error,omitempty"`
	}{Info: last}
	if last != nil && last.State.Msgs > 0 {
		for seq := last.State.FirstSeq; seq <= last.State.LastSeq; seq++ {
			if len(proof.Reads) == 512 || ctx.Err() != nil {
				proof.Truncated = true
				break
			}
			before := time.Now().UTC()
			attempt, stop := context.WithTimeout(ctx, 2*time.Second)
			msg, err := run.GetMsg(attempt, seq)
			stop()
			read := observation{Sequence: seq, Before: before, After: time.Now().UTC(), Message: msg}
			if msg != nil {
				read.DataBytes = len(msg.Data)
				if len(msg.Data) > 4096 {
					copyMessage := *msg
					copyMessage.Data = nil
					read.Message = &copyMessage
					read.DataOmitted = true
				}
			}
			if err != nil {
				read.Error = err.Error()
			}
			proof.Reads = append(proof.Reads, read)
			if seq == last.State.LastSeq {
				break // Avoid overflow at the maximum stream sequence.
			}
		}
	}
	if ctx.Err() != nil {
		proof.ContextError = ctx.Err().Error()
	}
	data, err := json.MarshalIndent(proof, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0644)
}
