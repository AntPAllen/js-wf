package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strconv"

	"fmt"
	"github.com/nats-io/nats.go"
	"io"
	"js-wf/worker"
	"os"

	"github.com/nats-io/nats.go/jetstream"
	"js-wf/identity"
	"js-wf/internal/graphcli"
	"js-wf/journal"
)

func selectClientGraph(authority, prefix, bucket string, replicas int, encoding journal.Encoding, version int) (*journal.NativeGraphConfig, error) {
	return graphcli.Select(authority, prefix, bucket, replicas, encoding, version)
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

func fetchGraphReplayBundle(ctx context.Context, js jetstream.JetStream, graph *journal.GraphStore, typ, id string) (replayBundle, error) {
	if err := identity.Validate(typ, id); err != nil {
		return replayBundle{}, err
	}
	inv, err := js.Stream(ctx, "WF_INV")
	if err != nil {
		return replayBundle{}, err
	}
	source, err := inv.GetLastMsgForSubject(ctx, identity.InvocationSubject(typ, id))
	if err != nil {
		return replayBundle{}, err
	}
	snapshot, err := worker.ReadGraphReplaySnapshot(ctx, graph, typ, id, source)
	if err != nil {
		return replayBundle{}, err
	}
	digest := sha256.Sum256(snapshot.Input)
	bundle := replayBundle{Type: typ, ID: id, InvSeq: source.Sequence, Input: snapshot.Input, InputHash: hex.EncodeToString(digest[:]), Journal: snapshot.Records, Objects: snapshot.Objects}
	if pending := snapshot.PendingSignal; pending != nil {
		// The graph queue and terminal ownership authorize these replay inputs;
		// source headers are synthesized, not copied from a legacy stream.
		header := nats.Header{}
		header.Set("Wf-Inv-Seq", strconv.FormatUint(source.Sequence, 10))
		header.Set("Wf-Signal-Ref", pending.Ref)
		header.Set("Wf-Input-SHA256", pending.Hash)
		bundle.PendingSignal = &replayPendingSignal{Sequence: pending.Sequence, Subject: "wf.sig." + typ + "." + id + "." + pending.Name, Header: header}
	}
	return bundle, nil
}
