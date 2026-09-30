package client

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// Operation is one observed SDK call. Arguments and successful results contain
// hashes instead of user payloads. Error strings are recorded verbatim.
type Operation struct {
	InvokeTS time.Time       `json:"invoke_ts"`
	ReturnTS time.Time       `json:"return_ts"`
	Op       string          `json:"op"`
	Args     json.RawMessage `json:"args"`
	Result   json.RawMessage `json:"result"`
	Error    string          `json:"error,omitempty"`
}

type Observer interface {
	Record(Operation)
}

func NewObserved(js jetstream.JetStream, observer Observer) *Client {
	return &Client{js: js, startPort: &jetStreamStartPort{js: js}, observer: observer}
}

func (c *Client) observe(start time.Time, op string, args, result any, err error) {
	if c.observer == nil {
		return
	}
	argsJSON, _ := json.Marshal(args)
	resultJSON, _ := json.Marshal(result)
	record := Operation{InvokeTS: start, ReturnTS: time.Now(), Op: op, Args: argsJSON, Result: resultJSON}
	if err != nil {
		record.Error = err.Error()
	}
	c.observer.Record(record)
}

func hashBytes(data []byte) string {
	digest := sha256.Sum256(data)
	return hex.EncodeToString(digest[:])
}

func startStatus(err error) string {
	switch {
	case err == nil:
		return "started"
	case errors.Is(err, ErrAlreadyStarted):
		return "already_started"
	case errors.Is(err, ErrInputMismatch):
		return "input_mismatch"
	case errors.Is(err, ErrEnqueueUnknown):
		return "enqueue_unknown"
	case errors.Is(err, ErrStartUnknown):
		return "unknown"
	default:
		return "error"
	}
}

func signalStatus(err error) string {
	switch {
	case err == nil:
		return "signaled"
	case errors.Is(err, ErrEnqueueUnknown):
		return "enqueue_unknown"
	case errors.Is(err, ErrSignalUnknown):
		return "unknown"
	case errors.Is(err, ErrNotFound):
		return "not_found"
	case errors.Is(err, ErrSignalMismatch):
		return "payload_mismatch"
	default:
		return "error"
	}
}

func awaitStatus(err error, terminalFailed bool) string {
	switch {
	case err == nil:
		return "completed"
	case terminalFailed:
		return "failed"
	case errors.Is(err, ErrPurged):
		return "purged"
	case errors.Is(err, ErrNotFound):
		return "not_found"
	default:
		return "error"
	}
}

// A generic failure before PublishSignal cannot have appended a queue entry.
// Keep recognized outcomes and every post-publication failure unchanged.
func observedSignalStatus(err error, publishAttempted bool) string {
	status := signalStatus(err)
	if status == "error" && !publishAttempted {
		return "not_published"
	}
	return status
}
