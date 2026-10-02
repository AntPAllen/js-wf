package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"
)

// Nil counts mean unobserved, rather than an empty queue. Retained native
// schedule sources are physical messages even when delivery consumers are idle.
type drainAudit struct {
	At               time.Time `json:"at"`
	Messages         *uint64   `json:"stream_messages"`
	Pending          *int      `json:"consumer_pending"`
	ConsumersChecked uint32    `json:"consumers_checked"`
	Error            string    `json:"error,omitempty"`
}

type drainPort struct {
	stream   func(context.Context) (uint64, error)
	consumer func(context.Context, uint32) (int, error)
}

func inspectDrain(ctx context.Context, port drainPort, partitions uint32) drainAudit {
	audit := drainAudit{At: time.Now().UTC()}
	messages, err := port.stream(ctx)
	if err != nil {
		audit.Error = fmt.Sprintf("stream metadata: %v", err)
		return audit
	}
	audit.Messages = &messages
	if messages != 0 {
		return audit
	}
	pending := 0
	for p := uint32(0); p < partitions; p++ {
		n, err := port.consumer(ctx, p)
		if err != nil {
			audit.Error = fmt.Sprintf("consumer %d metadata: %v", p, err)
			return audit
		}
		pending += n
		audit.ConsumersChecked++
	}
	audit.Pending = &pending
	return audit
}

func (a drainAudit) complete(partitions uint32) bool {
	return a.Error == "" && a.Messages != nil && *a.Messages == 0 &&
		a.Pending != nil && *a.Pending == 0 && a.ConsumersChecked == partitions
}

func appendDrainAudit(path string, audit any) error {
	data, err := json.Marshal(audit)
	if err != nil {
		return err
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	_, err = f.Write(append(data, '\n'))
	if err == nil {
		err = f.Sync()
	}
	closeErr := f.Close()
	if err != nil {
		return err
	}
	return closeErr
}
