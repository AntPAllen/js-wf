package sim

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/anishathalye/porcupine"
	"js-wf/client"
	"js-wf/history"
	"js-wf/identity"
)

// Fixed Tier1 reproduction of the legal client retry sequence rejected by the
// earlier Start history checker. Calls execute the production client protocol.
func runStartEnqueueUnknownHistory(firstFault string, replay *Trace) (Trace, error) {
	s := NewScheduler(42)
	if replay != nil {
		var err error
		s, err = ReplayScheduler(*replay)
		if err != nil {
			return Trace{}, err
		}
	}
	if err := s.SetWorkload("start_enqueue_unknown_history"); err != nil {
		return Trace{}, err
	}
	if _, err := s.Choose([]string{firstFault}); err != nil {
		return s.Trace(), err
	}
	model := NewStartTransport(s)
	c := client.NewWithStartPort(model)
	input := []byte(`1`)
	hash := sha256.Sum256(input)
	args, _ := json.Marshal(map[string]string{"type": "test", "id": "enqueue-history", "input_hash": hex.EncodeToString(hash[:])})
	base := time.Unix(1700000000, 0).UTC()
	var operations []client.Operation
	var invocationSequence uint64
	for i := 0; i < 3; i++ {
		if i < 2 {
			fault := "drop_before_commit"
			if i == 0 {
				fault = firstFault
			}
			if err := model.QueueFault(StartFault{Operation: "enqueue_run", Kind: fault}); err != nil {
				return s.Trace(), err
			}
		}
		called := base.Add(time.Duration(s.NowMillis()) * time.Millisecond)
		handle, err := c.Start(context.Background(), "test", "enqueue-history", input)
		status := "enqueue_unknown"
		if i < 2 && !errors.Is(err, client.ErrEnqueueUnknown) || i == 2 && !errors.Is(err, client.ErrAlreadyStarted) {
			return s.Trace(), fmt.Errorf("client retry%d handle=%+v err=%v", i, handle, err)
		}
		if i == 2 {
			status = "already_started"
		}
		if handle.InvSeq == 0 || invocationSequence != 0 && invocationSequence != handle.InvSeq {
			return s.Trace(), fmt.Errorf("invocation identity changed")
		}
		invocationSequence = handle.InvSeq
		result, _ := json.Marshal(map[string]any{"status": status, "inv_seq": handle.InvSeq})
		operations = append(operations, client.Operation{Op: "start", InvokeTS: called, ReturnTS: base.Add(time.Duration(s.NowMillis()) * time.Millisecond), Args: args, Result: result, Error: err.Error()})
	}
	stored, ok := model.Invocation(identity.InvocationSubject("test", "enqueue-history"))
	last, err := model.LastInvocationSequence(context.Background())
	if err != nil || !ok || stored.Sequence != invocationSequence || last != 1 || string(stored.Data) != string(input) || len(model.Runs()) != 1 {
		return s.Trace(), fmt.Errorf("retained start/dispatch changed: last=%d runs=%d err=%v", last, len(model.Runs()), err)
	}
	if verdict, err := history.CheckStarts(operations, time.Second); err != nil || verdict != porcupine.Ok {
		return s.Trace(), fmt.Errorf("client-generated retry history=%s err=%v", verdict, err)
	}
	s.RecordTransport(TransportEvent{Operation: "check_start_enqueue_unknown_history", Subject: stored.Subject, Sequence: invocationSequence, Outcome: "legal_once", AtMillis: s.NowMillis()})
	if err := s.Finish(); err != nil {
		return s.Trace(), err
	}
	return s.Trace(), nil
}

func TestStartRepeatedEnqueueUnknownHistoryReplay(t *testing.T) {
	for _, fault := range []string{"drop_before_commit", "lose_ack_after_commit"} {
		t.Run(fault, func(t *testing.T) {
			generated, err := runStartEnqueueUnknownHistory(fault, nil)
			if err != nil {
				t.Fatal(err)
			}
			replayed, err := runStartEnqueueUnknownHistory(fault, &generated)
			if err != nil || !reflect.DeepEqual(generated, replayed) {
				t.Fatalf("exact replay: %v", err)
			}
			if root := os.Getenv("START_ENQUEUE_HISTORY_TRACE_ROOT"); root != "" {
				if err := generated.Save(filepath.Join(root, fault+".json")); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}
