//go:build linux

package integrity

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// This ordered diagnostic records each admission verdict separately. A test
// PASS means observations were retained and cleanup ran, not that both audits
// passed. The caller supplies a new, verified complete store copy.
func TestRetainedStateAdmissionCopiedDiagnostic(t *testing.T) {
	ctx, js, kv, _, _, root := stateSnapshotCopiedFixture(t)
	stream, err := js.Stream(ctx, "KV_WF_STATE")
	if err != nil {
		t.Fatal(err)
	}
	type result struct {
		Label     string                     `json:"label"`
		ElapsedNS int64                      `json:"elapsed_ns"`
		Error     string                     `json:"error"`
		Accepted  bool                       `json:"accepted"`
		Values    int                        `json:"values"`
		Report    Report                     `json:"report"`
		Before    *jetstream.StreamInfo      `json:"before"`
		After     *jetstream.StreamInfo      `json:"after"`
		Frames    []StateSnapshotObservation `json:"frames"`
		Consumers []*jetstream.ConsumerInfo  `json:"watch_consumer_metadata"`
		MetaError string                     `json:"watch_consumer_metadata_error"`
	}
	var results []result
	for _, label := range []string{"standalone-state-20s", "concurrent-full87920-20s"} {
		meta, stopMeta := context.WithTimeout(ctx, 5*time.Second)
		before, err := stream.Info(meta)
		stopMeta()
		if err != nil {
			t.Fatal(err)
		}
		if before.Config.Replicas != 5 || before.Config.MaxMsgsPerSubject != 1 {
			t.Fatal("diagnostic requires original R5/history1 state source")
		}
		var mu sync.Mutex
		var metadataDone sync.WaitGroup
		r := result{Label: label, Before: before}
		call, stop := context.WithTimeout(ctx, 20*time.Second)
		observed := WithStateSnapshotObserver(call, func(frame StateSnapshotObservation) {
			mu.Lock()
			r.Frames = append(r.Frames, frame)
			mu.Unlock()
			if frame.Event != "watch_created" {
				return
			}
			// Observe public leader-routed consumer metadata asynchronously. No
			// updates are relayed and the synchronous watch callback is unchanged.
			metadataDone.Add(1)
			go func() {
				defer metadataDone.Done()
				query, cancel := context.WithTimeout(call, 2*time.Second)
				defer cancel()
				listing := stream.ListConsumers(query)
				var consumers []*jetstream.ConsumerInfo
				for info := range listing.Info() {
					consumers = append(consumers, info)
				}
				mu.Lock()
				r.Consumers = append(r.Consumers, consumers...)
				r.MetaError = fmt.Sprint(listing.Err())
				mu.Unlock()
			}()
		})
		began := time.Now()
		if label == "standalone-state-20s" {
			var state jetstream.KeyValue
			state, err = initialAuditState(observed, kv, func(string) bool { return true })
			if err == nil {
				r.Values = len(state.(*auditStateSnapshot).values)
			}
		} else {
			r.Report, err = CheckThroughInvocationSequenceWithChunkedConcurrentStateReads(observed, js, 87920)
		}
		r.ElapsedNS, r.Error = time.Since(began).Nanoseconds(), fmt.Sprint(err)
		stop()
		metadataDone.Wait()
		meta, stopMeta = context.WithTimeout(ctx, 5*time.Second)
		r.After, err = stream.Info(meta)
		stopMeta()
		if err != nil {
			t.Fatal(err)
		}
		barrier, clean := false, true
		for _, frame := range r.Frames {
			barrier = barrier || frame.Event == "initial_complete"
			if frame.Event == "watch_stopped" && frame.Error != "" {
				clean = false
			}
		}
		a, b := before.State, r.After.State
		stable := a.FirstSeq == b.FirstSeq && a.LastSeq == b.LastSeq && a.Msgs == b.Msgs && a.Bytes == b.Bytes && a.Consumers == b.Consumers
		r.Accepted = r.Error == "<nil>" && barrier && clean && stable && r.ElapsedNS < int64(20*time.Second)
		if label == "standalone-state-20s" {
			r.Accepted = r.Accepted && uint64(r.Values) == before.State.Msgs
		} else {
			r.Accepted = r.Accepted && r.Report == (Report{Invocations: 87920, Journals: 87920, Entries: 969925, Terminal: 87920})
		}
		results = append(results, r)
		data, marshalErr := json.MarshalIndent(map[string]any{"results": results, "scope": "ordered diagnostic on fresh complete copies; each admission has its own verdict; consumer metadata is leader-routed, not local physical evidence; no bulk/24h qualification"}, "", "  ")
		if marshalErr != nil {
			t.Fatal(marshalErr)
		}
		if err := os.WriteFile(filepath.Join(root, "state-admission.json"), append(data, '\n'), 0600); err != nil {
			t.Fatal(err)
		}
		t.Logf("STATE_ADMISSION label=%s accepted=%v elapsed=%s values=%d report=%+v error=%s", label, r.Accepted, time.Duration(r.ElapsedNS), r.Values, r.Report, r.Error)
		if r.Error != "<nil>" {
			// Admission failures remain explicit observations.
			t.Logf("admission did not certify the source: %s", r.Error)
		}
	}
}
