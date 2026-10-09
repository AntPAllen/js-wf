package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"

	"github.com/nats-io/nats.go/jetstream"
	"js-wf/identity"
	"js-wf/journal"
)

func selectClientGraph(authority, prefix, bucket string, replicas int, encoding journal.Encoding) (*journal.NativeGraphConfig, error) {
	if authority == "" && prefix == "" && bucket == "" {
		return nil, nil
	}
	if authority == "" || prefix == "" || bucket == "" {
		return nil, fmt.Errorf("graph runtime requires graph-authority-stream, graph-authority-prefix and graph-object-bucket together")
	}
	cfg := journal.NativeGraphConfig{AuthorityStream: authority, AuthorityPrefix: prefix, ObjectBucket: bucket, ExpectedReplicas: replicas, Encoding: encoding, CanonicalStarts: true, CanonicalSignals: true}
	if _, err := journal.NativeGraphStreamConfigs(cfg, replicas); err != nil {
		return nil, err
	}
	return &cfg, nil
}

func readCLIPayload(literal, file string) ([]byte, error) {
	data := []byte(literal)
	if file != "" {
		f, err := os.Open(file)
		if err != nil {
			return nil, err
		}
		defer f.Close()
		data, err = io.ReadAll(io.LimitReader(f, 64<<20+1))
		if err != nil {
			return nil, err
		}
		if len(data) > 64<<20 {
			return nil, fmt.Errorf("JSON payload file exceeds 64 MiB")
		}
	}
	if !json.Valid(data) {
		return nil, fmt.Errorf("start/signal payload must be valid JSON")
	}
	return data, nil
}

// Export only a pinned matching generation. Compatibility state and WF_JRN
// cannot authorize graph history; this does not resolve payloads for replay.
func readGraphCLIHistory(ctx context.Context, js jetstream.JetStream, graph *journal.GraphStore, typ, id string) (invocation uint64, records []journal.Record, err error) {
	if err = identity.Validate(typ, id); err != nil {
		return
	}
	inv, e := js.Stream(ctx, "WF_INV")
	if e != nil {
		err = e
		return
	}
	input, e := inv.GetLastMsgForSubject(ctx, identity.InvocationSubject(typ, id))
	if e != nil {
		err = e
		return
	}
	invocation = input.Sequence
	view, e := graph.OpenExisting(ctx, typ, id, invocation)
	if e != nil {
		err = e
		return
	}
	if view == nil {
		err = journal.ErrStale
		return
	}
	defer func() {
		closeErr := view.Close(ctx)
		if err == nil && closeErr != nil {
			records = nil
			err = closeErr
		}
	}()
	if err = view.ValidateStartInvocation(ctx, input); err != nil {
		return
	}
	records = make([]journal.Record, 0, view.Count())
	for index := uint64(0); index < view.Count(); index++ {
		record, e := view.Read(ctx, index)
		if e != nil {
			records = nil
			err = e
			return
		}
		records = append(records, record.Record)
	}
	return
}
