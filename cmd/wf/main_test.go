package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"testing"
	"time"

	"js-wf/assignment"
	"js-wf/client"
	"js-wf/identity"
	"js-wf/journal"
	"js-wf/provision"
	"js-wf/testcluster"
	"js-wf/visibility"
	"js-wf/wf"

	"github.com/nats-io/nats.go/jetstream"
)

func TestOperatorCommands(t *testing.T) {
	cluster, err := testcluster.Start(t.TempDir(), 1)
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Close()
	js, err := jetstream.New(cluster.Clients[0])
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := provision.Ensure(ctx, js, 1); err != nil {
		t.Fatal(err)
	}
	const typ, id = "test", "operator"
	ack, err := js.Publish(ctx, identity.InvocationSubject(typ, id), []byte(`null`))
	if err != nil {
		t.Fatal(err)
	}
	j := journal.New(js)
	seq, err := j.Append(ctx, typ, id, journal.Entry{Index: 0, Epoch: 1, Kind: journal.Started}, 0)
	if err != nil {
		t.Fatal(err)
	}
	terminal, _ := json.Marshal(wf.Outcome{InvSeq: ack.Sequence, Result: []byte(`1`)})
	if _, err := j.Append(ctx, typ, id, journal.Entry{Index: 1, Epoch: 1, Kind: journal.Completed, Payload: terminal}, seq); err != nil {
		t.Fatal(err)
	}
	state, _ := js.KeyValue(ctx, "WF_STATE")
	if _, err := state.Put(ctx, identity.Key(typ, id), terminal); err != nil {
		t.Fatal(err)
	}
	base := []string{"-url", cluster.Servers[0].ClientURL()}
	call := func(args ...string) []byte {
		t.Helper()
		var output bytes.Buffer
		if err := run(append(append([]string{}, base...), args...), &output); err != nil {
			t.Fatal(err)
		}
		return output.Bytes()
	}
	call("assignment-init", "owner-a", "owner-b")
	var assignmentRow struct {
		Owner    string `json:"owner"`
		Revision uint64 `json:"revision"`
	}
	if err := json.Unmarshal(call("assignment-get", "0"), &assignmentRow); err != nil || assignmentRow.Owner != "owner-a" || assignmentRow.Revision == 0 {
		t.Fatalf("assignment=%+v err=%v", assignmentRow, err)
	}
	oldRevision := assignmentRow.Revision
	if err := json.Unmarshal(call("assignment-move", "0", "owner-b", strconv.FormatUint(oldRevision, 10)), &assignmentRow); err != nil || assignmentRow.Owner != "owner-b" || assignmentRow.Revision <= oldRevision {
		t.Fatalf("moved assignment=%+v err=%v", assignmentRow, err)
	}
	var ignored bytes.Buffer
	if err := run(append(append([]string{}, base...), "assignment-move", "0", "owner-a", strconv.FormatUint(oldRevision, 10)), &ignored); !errors.Is(err, assignment.ErrConflict) {
		t.Fatalf("stale move: %v", err)
	}
	var rows []visibility.Row
	if err := json.Unmarshal(call("-rebuild", "list", "completed"), &rows); err != nil || len(rows) != 1 || rows[0].ID != id {
		t.Fatalf("list=%+v err=%v", rows, err)
	}
	var lag struct {
		Pending uint64 `json:"pending"`
	}
	if err := json.Unmarshal(call("lag"), &lag); err != nil || lag.Pending != 2 {
		t.Fatalf("lag=%+v err=%v", lag, err)
	}
	var scan struct {
		Reenqueued int `json:"reenqueued"`
	}
	if err := json.Unmarshal(call("scan-suspended"), &scan); err != nil || scan.Reenqueued != 0 {
		t.Fatalf("scan=%+v err=%v", scan, err)
	}
	var described struct {
		Row     visibility.Row   `json:"row"`
		Journal []journal.Record `json:"journal"`
	}
	if err := json.Unmarshal(call("describe", typ, id), &described); err != nil || described.Row.Status != "completed" || len(described.Journal) != 2 {
		t.Fatalf("describe=%+v err=%v", described, err)
	}
	var exported []journal.Record
	if err := json.Unmarshal(call("export-journal", typ, id), &exported); err != nil || len(exported) != 2 {
		t.Fatalf("export=%+v err=%v", exported, err)
	}
	if _, err := client.New(js).Start(ctx, typ, "cancel-target", []byte(`null`)); err != nil {
		t.Fatal(err)
	}
	var cancellation struct {
		Requested bool   `json:"requested"`
		SignalSeq uint64 `json:"signal_seq"`
	}
	if err := json.Unmarshal(call("cancel", typ, "cancel-target"), &cancellation); err != nil || !cancellation.Requested || cancellation.SignalSeq == 0 {
		t.Fatalf("cancel=%+v err=%v", cancellation, err)
	}
	call("-grace", "1ns", "purge", typ, id)
	if err := json.Unmarshal(call("-rebuild", "list", "completed"), &rows); err != nil || len(rows) != 0 {
		t.Fatalf("list after purge=%+v err=%v", rows, err)
	}
	var sweep struct {
		Deleted int `json:"deleted"`
	}
	if err := json.Unmarshal(call("sweep-tombstones"), &sweep); err != nil || sweep.Deleted != 1 {
		t.Fatalf("sweep=%+v err=%v", sweep, err)
	}
}

func TestJournalCapacityAlert(t *testing.T) {
	cluster, err := testcluster.Start(t.TempDir(), 1)
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Close()
	js, err := jetstream.New(cluster.Clients[0])
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	const limit int64 = 32 * 1024
	if err := provision.EnsureWithJournalLimit(ctx, js, 1, limit); err != nil {
		t.Fatal(err)
	}
	if err := provision.EnsureWithJournalLimit(ctx, js, 1, limit); err != nil {
		t.Fatalf("same cap should be idempotent: %v", err)
	}
	if err := provision.EnsureWithJournalLimit(ctx, js, 1, 2*limit); err == nil {
		t.Fatal("different journal cap was silently adopted")
	}
	capacity, err := provision.CheckJournalCapacity(ctx, js)
	if err != nil || capacity.Alert || capacity.LimitBytes != limit {
		t.Fatalf("initial capacity=%+v err=%v", capacity, err)
	}
	var output bytes.Buffer
	args := []string{"-url", cluster.Servers[0].ClientURL(), "journal-capacity"}
	if err := run(args, &output); err != nil {
		t.Fatalf("healthy capacity command: %v", err)
	}
	stepPayload, _ := json.Marshal(strings.Repeat("x", 1024))
	store := journal.New(js)
	var seq uint64
	for index := 0; index < 50; index++ {
		kind := journal.StepRequested
		if index == 0 {
			kind = journal.Started
		}
		seq, err = store.Append(ctx, "capacity", "alert", journal.Entry{Index: uint64(index), Epoch: 1, Kind: kind, Payload: stepPayload}, seq)
		if err != nil {
			t.Fatal(err)
		}
		capacity, err = provision.CheckJournalCapacity(ctx, js)
		if err != nil {
			t.Fatal(err)
		}
		if capacity.Alert {
			break
		}
	}
	if !capacity.Alert || capacity.Utilization < .7 || capacity.Utilization >= 1 {
		t.Fatalf("expected pre-quota warning: %+v", capacity)
	}
	output.Reset()
	if err := run(args, &output); err == nil || !strings.Contains(err.Error(), "70% alert threshold") {
		t.Fatalf("capacity command did not alert: %v", err)
	}
	var reported provision.JournalCapacity
	if err := json.Unmarshal(output.Bytes(), &reported); err != nil || !reported.Alert || reported.UsedBytes != capacity.UsedBytes {
		t.Fatalf("capacity report=%+v err=%v", reported, err)
	}
}
