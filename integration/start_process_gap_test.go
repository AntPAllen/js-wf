//go:build !windows

package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"js-wf/client"
	"js-wf/identity"
	"js-wf/provision"
)

type startGapReceipt struct {
	PID        int                     `json:"pid"`
	ServerID   string                  `json:"server_id"`
	Version    string                  `json:"version"`
	Invoked    time.Time               `json:"invoked"`
	At         time.Time               `json:"at"`
	RunSubject string                  `json:"run_subject"`
	RunData    string                  `json:"run_data"`
	Invocation *jetstream.RawStreamMsg `json:"invocation"`
}
type startGapProof struct {
	Receipt       startGapReceipt         `json:"receipt"`
	KillStarted   time.Time               `json:"kill_started"`
	Killed        time.Time               `json:"killed"`
	WaitError     string                  `json:"wait_error"`
	Signal        string                  `json:"signal"`
	Retained      *jetstream.RawStreamMsg `json:"retained"`
	RunMessages   int                     `json:"run_messages"`
	JournalAbsent bool                    `json:"journal_absent"`
}

type startDispatchBarrier struct {
	jetstream.JetStream
	nc              *nats.Conn
	typ, id, marker string
	invoked         time.Time
}

func (b *startDispatchBarrier) Publish(ctx context.Context, subject string, data []byte, _ ...jetstream.PublishOpt) (*jetstream.PubAck, error) {
	if subject != identity.RunSubject(b.typ, b.id, provision.Partitions) || string(data) != identity.Key(b.typ, b.id) {
		return nil, fmt.Errorf("unexpected start dispatch at barrier: %s", subject)
	}
	inv, err := b.JetStream.Stream(ctx, "WF_INV")
	if err != nil {
		return nil, err
	}
	retained, err := inv.GetLastMsgForSubject(ctx, identity.InvocationSubject(b.typ, b.id))
	if err != nil {
		return nil, err
	}
	receipt := startGapReceipt{PID: os.Getpid(), ServerID: b.nc.ConnectedServerId(), Version: b.nc.ConnectedServerVersion(), Invoked: b.invoked, At: time.Now().UTC(), RunSubject: subject, RunData: string(data), Invocation: retained}
	raw, err := json.MarshalIndent(receipt, "", "  ")
	if err != nil {
		return nil, err
	}
	if err = os.WriteFile(b.marker+".tmp", raw, 0600); err != nil {
		return nil, err
	}
	if err = os.Rename(b.marker+".tmp", b.marker); err != nil {
		return nil, err
	}
	// The real invocation publish has returned. No dispatch publish reaches NATS.
	<-ctx.Done()
	return nil, ctx.Err()
}
func TestStartGapCrashChild(t *testing.T) {
	if os.Getenv("WF_START_GAP_CHILD") != "1" {
		t.Skip("Start process crash helper")
	}
	nc, err := nats.Connect(os.Getenv("WF_START_GAP_URL"), nats.NoReconnect(), nats.IgnoreDiscoveredServers(), nats.Timeout(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	b := &startDispatchBarrier{JetStream: js, nc: nc, typ: os.Getenv("WF_START_GAP_TYPE"), id: os.Getenv("WF_START_GAP_ID"), marker: os.Getenv("WF_START_GAP_MARKER"), invoked: time.Now().UTC()}
	ctx, stop := context.WithTimeout(context.Background(), time.Minute)
	defer stop()
	_, err = client.New(b).Start(ctx, b.typ, b.id, []byte(os.Getenv("WF_START_GAP_INPUT")))
	t.Fatalf("Start returned before the intended process kill: %v", err)
}
func crashStartBeforeDispatch(ctx context.Context, js jetstream.JetStream, url, typ, id string, input []byte, root string) (proof startGapProof, resultErr error) {
	if err := os.MkdirAll(root, 0755); err != nil {
		return proof, err
	}
	executable, err := os.Executable()
	if err != nil {
		return proof, err
	}
	marker := filepath.Join(root, "receipt.json")
	output, err := os.Create(filepath.Join(root, "child.log"))
	if err != nil {
		return proof, err
	}
	defer output.Close()
	child := exec.Command(executable, "-test.run=^TestStartGapCrashChild$", "-test.count=1")
	child.Env = append(os.Environ(), "WF_START_GAP_CHILD=1", "WF_START_GAP_URL="+url, "WF_START_GAP_TYPE="+typ, "WF_START_GAP_ID="+id, "WF_START_GAP_MARKER="+marker, "WF_START_GAP_INPUT="+string(input))
	child.Stdout, child.Stderr = output, output
	if err = child.Start(); err != nil {
		return proof, err
	}
	waited := false
	defer func() {
		if !waited {
			_ = child.Process.Kill()
			_ = child.Wait()
		}
	}()
	defer func() {
		raw, _ := json.MarshalIndent(proof, "", "  ")
		if err := os.WriteFile(filepath.Join(root, "killed-gap.json"), raw, 0644); resultErr == nil && err != nil {
			resultErr = err
		}
	}()
	bound, stop := context.WithTimeout(ctx, 15*time.Second)
	defer stop()
	for bound.Err() == nil {
		raw, readErr := os.ReadFile(marker)
		if readErr == nil {
			if err = json.Unmarshal(raw, &proof.Receipt); err != nil {
				return proof, err
			}
			break
		}
		if !errors.Is(readErr, os.ErrNotExist) {
			return proof, readErr
		}
		select {
		case <-time.After(time.Millisecond):
		case <-bound.Done():
		}
	}
	if bound.Err() != nil {
		return proof, fmt.Errorf("Start gap barrier not reached: %w", bound.Err())
	}
	if proof.Receipt.PID != child.Process.Pid || proof.Receipt.Invocation == nil || proof.Receipt.Invocation.Sequence == 0 {
		return proof, fmt.Errorf("Start gap child identity/commit proof mismatch")
	}
	proof.KillStarted = time.Now().UTC()
	if err = child.Process.Kill(); err != nil {
		return proof, err
	}
	err = child.Wait()
	waited = true
	proof.Killed = time.Now().UTC()
	if err == nil {
		return proof, fmt.Errorf("Start gap child exited normally")
	}
	proof.WaitError = err.Error()
	status, ok := child.ProcessState.Sys().(syscall.WaitStatus)
	if !ok || !status.Signaled() || status.Signal() != syscall.SIGKILL {
		return proof, fmt.Errorf("Start gap child was not SIGKILLed: %v", child.ProcessState)
	}
	proof.Signal = "SIGKILL"
	inv, err := js.Stream(ctx, "WF_INV")
	if err != nil {
		return proof, err
	}
	proof.Retained, err = inv.GetLastMsgForSubject(ctx, identity.InvocationSubject(typ, id))
	if err != nil {
		return proof, err
	}
	if proof.Retained.Sequence != proof.Receipt.Invocation.Sequence || string(proof.Retained.Data) != string(input) {
		return proof, fmt.Errorf("Start gap invocation changed after process death")
	}
	jrn, err := js.Stream(ctx, "WF_JRN")
	if err != nil {
		return proof, err
	}
	_, err = jrn.GetLastMsgForSubject(ctx, identity.JournalSubject(typ, id))
	if !errors.Is(err, jetstream.ErrMsgNotFound) {
		return proof, fmt.Errorf("Start gap unexpectedly has journal: %v", err)
	}
	proof.JournalAbsent = true
	runs, err := js.Stream(ctx, "WF_RUN")
	if err != nil {
		return proof, err
	}
	info, err := runs.Info(ctx)
	if err != nil {
		return proof, err
	}
	for sequence := info.State.FirstSeq; sequence <= info.State.LastSeq; sequence++ {
		m, err := runs.GetMsg(ctx, sequence)
		if errors.Is(err, jetstream.ErrMsgNotFound) {
			continue
		}
		if err != nil {
			return proof, err
		}
		if string(m.Data) == identity.Key(typ, id) {
			proof.RunMessages++
		}
	}
	if proof.RunMessages != 0 {
		return proof, fmt.Errorf("Start gap dispatch already present")
	}
	return proof, nil
}
