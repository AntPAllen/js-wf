package client

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"js-wf/journal"
	"js-wf/wf"
)

// NewWithGraphJournal resolves large terminal results through the experimental
// graph journal. Start and signal publication still use legacy storage; rollout,
// importer and retention integration remain required before production GC.
func NewWithGraphJournal(js jetstream.JetStream, store *journal.GraphStore) (*Client, error) {
	if js == nil || store == nil {
		return nil, fmt.Errorf("invalid graph client configuration")
	}
	c := New(js)
	c.graphJournal = store
	return c, nil
}

func (c *Client) graphTerminalResult(ctx context.Context, typ, id string, outcome wf.Outcome) (data []byte, err error) {
	view, err := c.graphJournal.Open(ctx, typ, id, outcome.InvSeq)
	if err != nil {
		return nil, err
	}
	defer func() {
		cleanup, stop := context.WithTimeout(context.WithoutCancel(ctx), 3*time.Second)
		defer stop()
		closeErr := view.Close(cleanup)
		if err == nil && closeErr != nil {
			data = nil
			err = closeErr
		}
	}()
	if view.Count() == 0 {
		return nil, wf.ErrCorruptJournal
	}
	record, err := view.Read(ctx, view.Count()-1)
	if err != nil {
		return nil, err
	}
	var terminal wf.Outcome
	if record.Kind != journal.Completed || json.Unmarshal(record.Payload, &terminal) != nil || terminal.InvSeq != outcome.InvSeq || terminal.ResultRef != outcome.ResultRef || terminal.ResultHash != outcome.ResultHash || terminal.Error != "" || len(terminal.Result) != 0 {
		return nil, wf.ErrCorruptJournal
	}
	return outcome.ResultBytes(ctx, func(ctx context.Context, _ string) ([]byte, error) {
		for _, link := range append(record.Blobs, record.EntryBlob) {
			if link.Hash == terminal.ResultHash {
				return view.Payload(ctx, record.Index, link, c.graphJournal.PayloadReadLimit())
			}
		}
		return nil, wf.ErrCorruptJournal
	})
}
