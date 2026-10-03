//go:build linux

package integration_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/anishathalye/porcupine"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"js-wf/client"
	"js-wf/history"
	"js-wf/identity"
	"js-wf/journal"
	"js-wf/provision"
	"testing"
)

// This transport is used only by the production start repair loop. Failed
// publication keeps its cursor at the stranded invocation until release.
// Other client, worker and repair transports use the real JetStream directly.
type fiveStartRepairGate struct {
	jetstream.JetStream
	mu      sync.Mutex
	key     string
	subject string
}

func (g *fiveStartRepairGate) hold(typ, id string) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.key != "" {
		return fmt.Errorf("start repair gate already held")
	}
	g.key = identity.Key(typ, id)
	g.subject = identity.RunSubject(typ, id, provision.Partitions)
	return nil
}
func (g *fiveStartRepairGate) release() { g.mu.Lock(); g.key = ""; g.mu.Unlock() }
func (g *fiveStartRepairGate) Publish(ctx context.Context, subject string, data []byte, opts ...jetstream.PublishOpt) (*jetstream.PubAck, error) {
	g.mu.Lock()
	held := g.key != "" && string(data) == g.key && subject == g.subject
	g.mu.Unlock()
	if held {
		return nil, nats.ErrTimeout
	}
	return g.JetStream.Publish(ctx, subject, data, opts...)
}

type fiveStartGapReady struct {
	typ, id string
	proof   startGapProof
	err     error
}
type fiveStartGapRequest struct {
	node     int
	root     string
	ready    chan fiveStartGapReady
	released chan struct{}
}
type fiveStartGapObservation struct {
	Type           string                  `json:"type"`
	ID             string                  `json:"id"`
	Root           string                  `json:"root"`
	Proof          startGapProof           `json:"proof"`
	AfterUpgrade   *jetstream.RawStreamMsg `json:"after_upgrade"`
	RepairReleased time.Time               `json:"repair_released"`
}

// A killed process has no SDK response. Record its invocation as an uncertain
// call bounded by confirmed process death, rather than inventing a successful
// Start response. Raw commit receipt resolves the uncertainty separately.
func recordFiveCrashedStart(recorder *history.Recorder, typ, id string, input []byte, proof startGapProof) {
	digest := sha256.Sum256(input)
	args, _ := json.Marshal(map[string]string{"type": typ, "id": id, "input_hash": hex.EncodeToString(digest[:])})
	result, _ := json.Marshal(map[string]any{"status": "unknown", "inv_seq": uint64(0)})
	recorder.Record(client.Operation{InvokeTS: proof.Receipt.Invoked, ReturnTS: proof.Killed, Op: "start", Args: args, Result: result, Error: "client process SIGKILL; no SDK response"})
}

func prepareFiveStartGap(ctx context.Context, js jetstream.JetStream, gate *fiveStartRepairGate, request *fiveStartGapRequest, url, typ, id string, input []byte, recorder *history.Recorder) (startGapProof, error) {
	if err := gate.hold(typ, id); err != nil {
		return startGapProof{}, err
	}
	proof, err := crashStartBeforeDispatch(ctx, js, url, typ, id, input, request.root)
	request.ready <- fiveStartGapReady{typ: typ, id: id, proof: proof, err: err}
	if err != nil {
		gate.release()
		return proof, err
	}
	recordFiveCrashedStart(recorder, typ, id, input, proof)
	select {
	case <-request.released:
		return proof, nil
	case <-ctx.Done():
		return proof, ctx.Err()
	}
}

// Recheck the gap after upgrade before enabling any repair publication.
func releaseFiveStartGap(ctx context.Context, js jetstream.JetStream, gate *fiveStartRepairGate, ready fiveStartGapReady, request *fiveStartGapRequest) (*fiveStartGapObservation, error) {
	observation := &fiveStartGapObservation{Type: ready.typ, ID: ready.id, Root: filepath.Base(request.root), Proof: ready.proof}
	inv, err := js.Stream(ctx, "WF_INV")
	if err != nil {
		return observation, err
	}
	observation.AfterUpgrade, err = inv.GetLastMsgForSubject(ctx, identity.InvocationSubject(ready.typ, ready.id))
	if err != nil {
		return observation, err
	}
	a, _ := json.Marshal(ready.proof.Retained)
	b, _ := json.Marshal(observation.AfterUpgrade)
	if string(a) != string(b) {
		return observation, fmt.Errorf("rolling Start gap invocation changed across upgrade")
	}
	jrn, err := js.Stream(ctx, "WF_JRN")
	if err != nil {
		return observation, err
	}
	if _, err = jrn.GetLastMsgForSubject(ctx, identity.JournalSubject(ready.typ, ready.id)); !errors.Is(err, jetstream.ErrMsgNotFound) {
		return observation, fmt.Errorf("rolling Start gap repaired before release: %v", err)
	}
	observation.RepairReleased = time.Now().UTC()
	raw, err := json.MarshalIndent(observation, "", "  ")
	if err == nil {
		err = os.WriteFile(filepath.Join(request.root, "upgrade-gap.json"), raw, 0644)
	}
	if err != nil {
		return observation, err
	}
	gate.release()
	close(request.released)
	return observation, nil
}

func verifyFiveStartGapTerminal(ctx context.Context, js jetstream.JetStream, c *client.Client, typ, id string, input []byte, proof startGapProof, root string) error {
	if time.Since(proof.Killed) >= 30*time.Second {
		return fmt.Errorf("rolling Start gap recovery exceeded30s")
	}
	handle, err := c.Start(ctx, typ, id, input)
	if !errors.Is(err, client.ErrAlreadyStarted) || handle.InvSeq != proof.Retained.Sequence {
		return fmt.Errorf("rolling Start gap duplicate identity changed: %+v %v", handle, err)
	}
	jrn, err := js.Stream(ctx, "WF_JRN")
	if err != nil {
		return err
	}
	terminal, err := jrn.GetLastMsgForSubject(ctx, identity.JournalSubject(typ, id))
	if err != nil {
		return err
	}
	var entry journal.Entry
	err = journal.UnmarshalEntry(terminal.Data, &entry)
	if err != nil || entry.Kind != journal.Completed {
		return fmt.Errorf("rolling Start gap lacks terminal entry: %v", err)
	}
	verified := time.Now().UTC()
	if verified.Sub(proof.Killed) >= 30*time.Second {
		return fmt.Errorf("rolling Start gap verified terminal exceeded30s")
	}
	completion, _ := json.MarshalIndent(map[string]any{"verified": verified, "kill_to_terminal_ns": verified.Sub(proof.Killed), "inv_seq": handle.InvSeq}, "", "  ")
	if err := os.WriteFile(filepath.Join(root, "completion.json"), completion, 0644); err != nil {
		return err
	}
	raw, err := json.MarshalIndent(terminal, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(root, "terminal.json"), raw, 0644)
}

// Exercise the publication gate with a transport that records real forwarding.
// The target is rejected before transport; unrelated identities and subjects
// continue, and releasing the target restores the original transport result.
type fiveStartRecordingPublisher struct {
	jetstream.JetStream
	calls int
}

func (p *fiveStartRecordingPublisher) Publish(context.Context, string, []byte, ...jetstream.PublishOpt) (*jetstream.PubAck, error) {
	p.calls++
	return &jetstream.PubAck{Sequence: uint64(p.calls)}, nil
}
func TestFiveStartRepairGatePreservesOtherPublications(t *testing.T) {
	p := &fiveStartRecordingPublisher{}
	g := &fiveStartRepairGate{JetStream: p}
	if err := g.hold("matrixshort", "held"); err != nil {
		t.Fatal(err)
	}
	subject := identity.RunSubject("matrixshort", "held", provision.Partitions)
	if _, err := g.Publish(context.Background(), subject, []byte("matrixshort.held")); !errors.Is(err, nats.ErrTimeout) || p.calls != 0 {
		t.Fatalf("held publication reached transport: %d %v", p.calls, err)
	}
	if err := g.hold("matrixshort", "other"); err == nil {
		t.Fatal("concurrent gap replaced held identity")
	}
	for _, publication := range []struct{ subject, data string }{{subject, "matrixshort.other"}, {"unrelated.subject", "matrixshort.held"}} {
		if _, err := g.Publish(context.Background(), publication.subject, []byte(publication.data)); err != nil {
			t.Fatal(err)
		}
	}
	g.release()
	if ack, err := g.Publish(context.Background(), subject, []byte("matrixshort.held")); err != nil || ack.Sequence != 3 {
		t.Fatalf("release failed to restore transport: %+v %v", ack, err)
	}
}
func TestFiveCrashedStartHistoryResolvesThroughDuplicate(t *testing.T) {
	var r history.Recorder
	now := time.Now()
	p := startGapProof{Receipt: startGapReceipt{Invoked: now}, Killed: now.Add(time.Millisecond)}
	recordFiveCrashedStart(&r, "matrixshort", "held", []byte("null"), p)
	original := r.Snapshot()[0]
	duplicate := original
	duplicate.InvokeTS = p.Killed.Add(time.Millisecond)
	duplicate.ReturnTS = duplicate.InvokeTS.Add(time.Millisecond)
	duplicate.Result = json.RawMessage(`{"status":"already_started","inv_seq":12}`)
	duplicate.Error = client.ErrAlreadyStarted.Error()
	r.Record(duplicate)
	if result, err := history.CheckStarts(r.Snapshot(), time.Second); err != nil || result != porcupine.Ok {
		t.Fatalf("crashed call uncertainty unresolved: %v %v", result, err)
	}
}
