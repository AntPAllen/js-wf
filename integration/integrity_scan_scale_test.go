package integration_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"js-wf/client"
	"js-wf/identity"
	"js-wf/integrity"
	"js-wf/journal"
)

func TestRetainedAuditThirtyThousandEntriesWithinCheckpointDeadline(t *testing.T) {
	if os.Getenv("WF_AUDIT_SCALE") != "1" {
		t.Skip("set WF_AUDIT_SCALE=1 for the retained-state scan boundary")
	}
	all, _ := setup(t)
	js := all[0]
	ctx, stop := context.WithTimeout(context.Background(), 3*time.Minute)
	defer stop()
	state, err := js.KeyValue(ctx, "WF_STATE")
	if err != nil {
		t.Fatal(err)
	}
	c := client.New(js)
	const invocations = 800
	const entries = 42
	tasks := make(chan int)
	failures := make(chan error, invocations)
	var writers sync.WaitGroup
	for i := 0; i < 32; i++ {
		writers.Add(1)
		go func() {
			defer writers.Done()
			for invocation := range tasks {
				id := fmt.Sprintf("audit-%d", invocation)
				if _, err := c.Start(ctx, "auditscale", id, []byte(`null`)); err != nil {
					failures <- err
					continue
				}
				var writeErr error
				for index := uint64(0); index < entries; index++ {
					kind := journal.StepRequested
					switch {
					case index == 0:
						kind = journal.Started
					case index == entries-1:
						kind = journal.Completed
					case index%2 == 0:
						kind = journal.StepCompleted
					}
					entry := journal.Entry{Kind: kind, Index: index, Epoch: 1, WorkerID: "audit-fixture", Payload: json.RawMessage(`42`)}
					data, _ := json.Marshal(entry)
					if _, writeErr = js.Publish(ctx, identity.JournalSubject("auditscale", id), data); writeErr != nil {
						break
					}
				}
				if writeErr == nil {
					_, writeErr = state.Put(ctx, identity.Key("auditscale", id), []byte(`42`))
				}
				if writeErr != nil {
					failures <- writeErr
				}
			}
		}()
	}
	for i := 0; i < invocations; i++ {
		tasks <- i
	}
	close(tasks)
	writers.Wait()
	close(failures)
	for err := range failures {
		t.Fatal(err)
	}
	audit, done := context.WithTimeout(ctx, 20*time.Second)
	defer done()
	started := time.Now()
	report, err := integrity.Check(audit, js)
	want := integrity.Report{Invocations: invocations, Journals: invocations, Entries: invocations * entries, Terminal: invocations}
	if err != nil || report != want {
		t.Fatalf("retained scan elapsed=%s report=%+v want=%+v err=%v", time.Since(started), report, want, err)
	}
	t.Logf("AUDIT_SCALE invocations=%d entries=%d elapsed=%s deadline=20s", report.Invocations, report.Entries, time.Since(started))
}
