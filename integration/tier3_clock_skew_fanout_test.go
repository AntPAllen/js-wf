//go:build linux

package integration_test

import (
	"context"
	"encoding/json"
	"sort"
	"strconv"
	"testing"
	"time"

	"js-wf/client"
	"js-wf/history"
	"js-wf/identity"
	"js-wf/journal"
	"js-wf/provision"
	"js-wf/wf"

	"github.com/anishathalye/porcupine"
	"github.com/nats-io/nats.go/jetstream"
)

const tier3ClockFanoutChildren = 6
const tier3ClockFanoutChildType = "tier3-clock-child"

func tier3ClockFanoutParent(c *wf.Context) (json.RawMessage, error) {
	promises := make([]wf.Promise, tier3ClockFanoutChildren)
	for i := range promises {
		promise, err := wf.CallAsync(c, tier3ClockFanoutChildType, json.RawMessage(strconv.Itoa(i)))
		if err != nil {
			return nil, err
		}
		promises[i] = promise
	}
	var sum int
	for _, promise := range promises {
		value, err := wf.AwaitPromise(c, promise)
		if err != nil {
			return nil, err
		}
		var n int
		if err := json.Unmarshal(value, &n); err != nil {
			return nil, err
		}
		sum += n
	}
	return json.Marshal(sum)
}

func tier3ClockFanoutChild(c *wf.Context, input json.RawMessage) (json.RawMessage, error) {
	var n int
	if err := json.Unmarshal(input, &n); err != nil {
		return nil, err
	}
	value, err := wf.Run(c, "double", n, func(context.Context) (int, error) { return n * 2, nil })
	if err != nil {
		return nil, err
	}
	return json.Marshal(value)
}

func tier3ClockFanoutRequests(t *testing.T, ctx context.Context, js jetstream.JetStream, typ, id string) ([]string, []uint32) {
	t.Helper()
	var childIDs []string
	var last journal.Kind
	for until := time.Now().Add(30 * time.Second); time.Now().Before(until) && ctx.Err() == nil; {
		attempt, stop := context.WithTimeout(ctx, 3*time.Second)
		records, _, err := journal.New(js).Read(attempt, typ, id)
		stop()
		if err != nil {
			t.Fatalf("read skewed fan-out requests: %v", err)
		}
		childIDs = childIDs[:0]
		for _, record := range records {
			if record.Kind != journal.StepRequested {
				continue
			}
			var request struct {
				Kind    string `json:"kind"`
				ChildID string `json:"child_id"`
			}
			if err := json.Unmarshal(record.Payload, &request); err != nil {
				t.Fatal(err)
			}
			if request.Kind == "call_async" {
				childIDs = append(childIDs, request.ChildID)
			}
		}
		if len(records) > 0 {
			last = records[len(records)-1].Kind
		}
		if len(childIDs) == tier3ClockFanoutChildren && last == journal.Suspended {
			break
		}
		time.Sleep(25 * time.Millisecond)
	}
	if len(childIDs) != tier3ClockFanoutChildren || last != journal.Suspended {
		t.Fatalf("skewed fan-out requests=%d journal_last=%s ctx_err=%v", len(childIDs), last, ctx.Err())
	}
	seen := make(map[string]bool)
	parts := make(map[uint32]bool)
	for _, childID := range childIDs {
		if childID == "" || seen[childID] {
			t.Fatalf("duplicate or empty skewed child ID %q", childID)
		}
		seen[childID] = true
		parts[identity.Partition(tier3ClockFanoutChildType, childID, provision.Partitions)] = true
	}
	partitions := make([]uint32, 0, len(parts))
	for part := range parts {
		partitions = append(partitions, part)
	}
	sort.Slice(partitions, func(i, j int) bool { return partitions[i] < partitions[j] })
	return append([]string(nil), childIDs...), partitions
}

func tier3ClockFanoutVerify(t *testing.T, ctx context.Context, js jetstream.JetStream, observed *client.Client, recorder *history.Recorder, typ, id string, childIDs []string) (time.Duration, int) {
	t.Helper()
	value, err := observed.Await(ctx, typ, id)
	if err != nil || string(value) != `30` {
		t.Fatalf("skewed fan-out parent result=%s err=%v", value, err)
	}
	store := journal.New(js)
	parent, _, err := store.Read(ctx, typ, id)
	if err != nil || len(parent) == 0 || parent[len(parent)-1].Kind != journal.Completed {
		t.Fatalf("skewed fan-out parent journal=%+v err=%v", parent, err)
	}
	var requests, consumed int
	for _, record := range parent {
		if record.Kind == journal.StepRequested {
			var request struct {
				Kind string `json:"kind"`
			}
			if err := json.Unmarshal(record.Payload, &request); err != nil {
				t.Fatal(err)
			}
			if request.Kind == "call_async" {
				requests++
			}
		}
		if record.Kind == journal.SignalConsumed {
			consumed++
		}
	}
	if requests != tier3ClockFanoutChildren || consumed != tier3ClockFanoutChildren {
		t.Fatalf("skewed fan-out parent requests=%d consumed=%d", requests, consumed)
	}
	stream, err := js.Stream(ctx, "WF_JRN")
	if err != nil {
		t.Fatal(err)
	}
	parentRaw, err := stream.GetMsg(ctx, parent[len(parent)-1].Sequence)
	if err != nil {
		t.Fatal(err)
	}
	var lastChildAt time.Time
	for i, childID := range childIDs {
		result, err := client.New(js).Await(ctx, tier3ClockFanoutChildType, childID)
		if err != nil || string(result) != strconv.Itoa(i*2) {
			t.Fatalf("skewed child %d result=%s err=%v", i, result, err)
		}
		entries, _, err := store.Read(ctx, tier3ClockFanoutChildType, childID)
		if err != nil || len(entries) != 4 || entries[0].Kind != journal.Started || entries[1].Kind != journal.StepRequested || entries[2].Kind != journal.StepCompleted || entries[3].Kind != journal.Completed {
			t.Fatalf("skewed child %d journal=%+v err=%v", i, entries, err)
		}
		raw, err := stream.GetMsg(ctx, entries[3].Sequence)
		if err != nil {
			t.Fatal(err)
		}
		if raw.Time.After(lastChildAt) {
			lastChildAt = raw.Time
		}
	}
	latency := parentRaw.Time.Sub(lastChildAt)
	if latency < 0 || latency >= 30*time.Second {
		t.Fatalf("skewed fan-out parent terminal=%s last_child_terminal=%s latency=%s", parentRaw.Time, lastChildAt, latency)
	}
	operations := recorder.Snapshot()
	if result, err := history.CheckStarts(operations, 10*time.Second); err != nil || result != porcupine.Ok {
		t.Fatalf("skewed fan-out start history=%s err=%v", result, err)
	}
	if result, err := history.CheckSignals(operations, 10*time.Second); err != nil || result != porcupine.Ok {
		t.Fatalf("skewed fan-out signal history=%s err=%v", result, err)
	}
	if result, err := history.CheckResults(operations, 10*time.Second); err != nil || result != porcupine.Ok {
		t.Fatalf("skewed fan-out result history=%s err=%v", result, err)
	}
	return latency, len(parent) + 4*len(childIDs)
}
