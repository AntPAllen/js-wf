package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"js-wf/client"
	"js-wf/identity"
	"js-wf/journal"
	"js-wf/provision"
	"js-wf/wf"
	"js-wf/worker"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// The wrapper runs in a child process. Its marker proves that the real worker
// reached the scheduled publish, after wf.Sleep had appended StepRequested.
// The parent kills the process before the publish can enter JetStream.
type timerPublishBarrier struct {
	jetstream.JetStream
	marker string
}

func (b *timerPublishBarrier) PublishMsg(_ context.Context, msg *nats.Msg, _ ...jetstream.PublishOpt) (*jetstream.PubAck, error) {
	if !strings.HasPrefix(msg.Subject, "wf.schedule.") {
		return nil, fmt.Errorf("unexpected message at timer publish barrier: %s", msg.Subject)
	}
	if err := os.WriteFile(b.marker+".tmp", []byte(msg.Subject), 0600); err != nil {
		return nil, err
	}
	if err := os.Rename(b.marker+".tmp", b.marker); err != nil {
		return nil, err
	}
	select {}
}

func TestTimerPublishKillChild(t *testing.T) {
	if os.Getenv("WF_TIMER_PUBLISH_KILL_CHILD") != "1" {
		t.Skip("timer publish child helper")
	}
	url, marker, id := os.Getenv("WF_TIMER_PUBLISH_KILL_URL"), os.Getenv("WF_TIMER_PUBLISH_KILL_MARKER"), os.Getenv("WF_TIMER_PUBLISH_KILL_ID")
	if url == "" || marker == "" || id == "" {
		t.Fatal("missing timer publish child configuration")
	}
	nc, err := nats.Connect(url, nats.NoReconnect(), nats.IgnoreDiscoveredServers())
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	const typ = "timer-kill"
	w, err := worker.New(context.Background(), &timerPublishBarrier{JetStream: js, marker: marker}, "timer-kill-child-"+id, map[string]worker.Handler{typ: func(c *wf.Context, _ json.RawMessage) (json.RawMessage, error) {
		if err := wf.Sleep(c, "gap", time.Second); err != nil {
			return nil, err
		}
		return json.RawMessage(`true`), nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	if err := w.RunPartition(context.Background(), identity.Partition(typ, id, provision.Partitions)); err != nil {
		t.Fatal(err)
	}
}

// Run with WF_TIMER_PROCESS_KILL_SCALE=1. Count may be reduced to diagnose a
// failed full run, but the proof requires 200 real SIGKILLs.
func TestTwoHundredWorkerKillsBeforeTimerPublish(t *testing.T) {
	if os.Getenv("WF_TIMER_PROCESS_KILL_SCALE") == "" {
		t.Skip("set WF_TIMER_PROCESS_KILL_SCALE=1 for 200 worker process kills")
	}
	count := 200
	if raw := os.Getenv("WF_TIMER_PROCESS_KILL_COUNT"); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed < 1 || parsed > 200 {
			t.Fatalf("invalid WF_TIMER_PROCESS_KILL_COUNT=%q", raw)
		}
		count = parsed
	}
	all, cluster := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	logFile, err := os.Create(filepath.Join(root, "children.log"))
	if err != nil {
		t.Fatal(err)
	}
	defer logFile.Close()
	const typ = "timer-kill"
	c := client.New(all[0])
	j := journal.New(all[0])
	run, err := all[0].Stream(ctx, "WF_RUN")
	if err != nil {
		t.Fatal(err)
	}
	for index := 0; index < count; index++ {
		id := fmt.Sprintf("gap-%03d", index)
		marker := filepath.Join(root, id)
		cmd := exec.Command(executable, "-test.run=^TestTimerPublishKillChild$")
		cmd.Env = append(os.Environ(), "WF_TIMER_PUBLISH_KILL_CHILD=1", "WF_TIMER_PUBLISH_KILL_URL="+cluster.Servers[index%3].ClientURL(), "WF_TIMER_PUBLISH_KILL_MARKER="+marker, "WF_TIMER_PUBLISH_KILL_ID="+id)
		cmd.Stdout, cmd.Stderr = logFile, logFile
		if err := cmd.Start(); err != nil {
			t.Fatalf("start child %s: %v", id, err)
		}
		waited := false
		func() {
			defer func() {
				if !waited {
					_ = cmd.Process.Kill()
					_ = cmd.Wait()
				}
			}()
			if _, err := c.Start(ctx, typ, id, []byte(`null`)); err != nil {
				t.Fatalf("start %s: %v", id, err)
			}
			until := time.Now().Add(10 * time.Second)
			for {
				subject, err := os.ReadFile(marker)
				if err == nil {
					if !strings.HasPrefix(string(subject), "wf.schedule."+typ+"."+id+".") {
						t.Fatalf("child %s reached wrong timer publish: %s", id, subject)
					}
					break
				}
				if !errors.Is(err, os.ErrNotExist) || ctx.Err() != nil || time.Now().After(until) {
					logData, _ := os.ReadFile(logFile.Name())
					t.Fatalf("child %s did not reach timer publish: marker=%v ctx=%v logs=%s", id, err, ctx.Err(), logData)
				}
				time.Sleep(5 * time.Millisecond)
			}
			records, _, err := j.Read(ctx, typ, id)
			if err != nil || len(records) != 2 || records[1].Kind != journal.StepRequested {
				t.Fatalf("child %s journal before kill: records=%+v err=%v", id, records, err)
			}
			if err := cmd.Process.Kill(); err != nil {
				t.Fatalf("kill child %s: %v", id, err)
			}
			waitErr := cmd.Wait()
			waited = true
			status, ok := cmd.ProcessState.Sys().(syscall.WaitStatus)
			if waitErr == nil || !ok || !status.Signaled() || status.Signal() != syscall.SIGKILL {
				t.Fatalf("child %s was not SIGKILLed: state=%v err=%v", id, cmd.ProcessState, waitErr)
			}
		}()
		// Drop the unacked start wakeup. The only durable path left is the
		// journal request found by the timer reconciler.
		if err := run.Purge(ctx); err != nil {
			t.Fatalf("purge start wakeup %s: %v", id, err)
		}
		if (index+1)%25 == 0 {
			t.Logf("killed %d/%d workers before timer publish", index+1, count)
		}
	}
	info, err := run.Info(ctx)
	if err != nil || info.State.Msgs != 0 {
		t.Fatalf("unreconciled run queue: info=%+v err=%v", info, err)
	}
	state, err := all[0].KeyValue(ctx, "WF_STATE")
	if err != nil {
		t.Fatal(err)
	}
	for index := 0; index < count; index++ {
		id := fmt.Sprintf("gap-%03d", index)
		if _, err := state.Get(ctx, identity.Key(typ, id)); !errors.Is(err, jetstream.ErrKeyNotFound) {
			t.Fatalf("result appeared without timer reconciliation for %s: %v", id, err)
		}
	}
	recoverMissingTimerSchedules(t, ctx, all, typ, count)
	t.Logf("SIGKILLed %d worker processes between timer journal append and scheduled publish; all recovered", count)
}
