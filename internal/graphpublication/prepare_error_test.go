package graphpublication

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"js-wf/internal/retainedgraph"
)

type preparationFaultPort struct {
	Port
	operation  string
	at         int
	after      bool
	calls      map[string]int
	rootWrites int
}

func (p *preparationFaultPort) cut(op string) bool {
	p.calls[op]++
	return op == p.operation && p.calls[op] == p.at
}
func (p *preparationFaultPort) ReadRoot(c context.Context, k string) (Root, error) {
	if p.cut("read_root") {
		return Root{}, context.DeadlineExceeded
	}
	return p.Port.ReadRoot(c, k)
}
func (p *preparationFaultPort) ReadBlob(c context.Context, k string) (Record, error) {
	if p.cut("read_blob") {
		return Record{}, context.DeadlineExceeded
	}
	return p.Port.ReadBlob(c, k)
}
func (p *preparationFaultPort) CASBlob(c context.Context, k string, n uint64, f Fence) (Record, error) {
	cut := p.cut("cas_blob")
	if cut && !p.after {
		return Record{}, context.DeadlineExceeded
	}
	r, err := p.Port.CASBlob(c, k, n, f)
	if err == nil && cut {
		return Record{}, context.DeadlineExceeded
	}
	return r, err
}
func (p *preparationFaultPort) Put(c context.Context, k string, data []byte) error {
	cut := p.cut("put")
	if cut && !p.after {
		return context.DeadlineExceeded
	}
	err := p.Port.Put(c, k, data)
	if err == nil && cut {
		return context.DeadlineExceeded
	}
	return err
}
func (p *preparationFaultPort) Get(c context.Context, l retainedgraph.Link, n int) ([]byte, error) {
	if p.cut("get") {
		return nil, context.DeadlineExceeded
	}
	return p.Port.Get(c, l, n)
}
func (p *preparationFaultPort) CASRoot(c context.Context, k string, n uint64, r Root) (Root, error) {
	p.rootWrites++
	return p.Port.CASRoot(c, k, n, r)
}

func TestGraphAppendPreparationErrorPhases(t *testing.T) {
	type cut struct {
		name, op, phase, acquisition string
		at                           int
		after                        bool
	}
	cuts := []cut{
		{name: "context", phase: "check append preparation context"},
		{name: "configuration", phase: "validate append preparation configuration"},
		{name: "head", op: "read_root", at: 1, phase: "observe append preparation head"},
		{name: "conflict", phase: "check append preparation head"},
		{name: "token", phase: "create append publication token"},
		{name: "owned", phase: "verify append owned payload"},
		{name: "payload_intent_read", op: "read_blob", at: 1, phase: "acquire append payload", acquisition: "observe blob intent"},
		{name: "payload_intent_rejected", op: "cas_blob", at: 1, phase: "acquire append payload", acquisition: "publish blob intent"},
		{name: "payload_intent_lost", op: "cas_blob", at: 1, after: true, phase: "acquire append payload", acquisition: "publish blob intent"},
		{name: "payload_upload_rejected", op: "put", at: 1, phase: "acquire append payload", acquisition: "upload immutable blob"},
		{name: "payload_upload_lost", op: "put", at: 1, after: true, phase: "acquire append payload", acquisition: "upload immutable blob"},
		{name: "payload_uploaded_read", op: "read_blob", at: 2, phase: "acquire append payload", acquisition: "observe uploaded blob intent"},
		{name: "payload_selection_rejected", op: "cas_blob", at: 2, phase: "acquire append payload", acquisition: "select uploaded blob"},
		{name: "payload_selection_lost", op: "cas_blob", at: 2, after: true, phase: "acquire append payload", acquisition: "select uploaded blob"},
		{name: "node_intent_read", op: "read_blob", at: 3, phase: "stage append nodes", acquisition: "observe blob intent"},
		{name: "node_intent_rejected", op: "cas_blob", at: 3, phase: "stage append nodes", acquisition: "publish blob intent"},
		{name: "node_intent_lost", op: "cas_blob", at: 3, after: true, phase: "stage append nodes", acquisition: "publish blob intent"},
		{name: "node_upload_rejected", op: "put", at: 2, phase: "stage append nodes", acquisition: "upload immutable blob"},
		{name: "node_upload_lost", op: "put", at: 2, after: true, phase: "stage append nodes", acquisition: "upload immutable blob"},
		{name: "node_uploaded_read", op: "read_blob", at: 4, phase: "stage append nodes", acquisition: "observe uploaded blob intent"},
		{name: "node_selection_rejected", op: "cas_blob", at: 4, phase: "stage append nodes", acquisition: "select uploaded blob"},
		{name: "node_selection_lost", op: "cas_blob", at: 4, after: true, phase: "stage append nodes", acquisition: "select uploaded blob"},
		{name: "node_read", op: "get", at: 1, phase: "stage append nodes"},
	}
	for _, cut := range cuts {
		t.Run(cut.name, func(t *testing.T) {
			m, p := newModel(cut.name)
			originalID := p.NewID
			expected := uint64(0)
			if cut.name == "node_read" {
				prior, err := p.PrepareAppend(ctx, "owner", 0, []byte("prior"), nil, epoch.Add(time.Minute))
				if err != nil {
					t.Fatal(err)
				}
				if _, err = p.Commit(ctx, prior); err != nil {
					t.Fatal(err)
				}
				expected = 1
			}
			before, err := m.ReadRoot(ctx, "owner")
			if err != nil {
				t.Fatal(err)
			}
			port := &preparationFaultPort{Port: m, operation: cut.op, at: cut.at, after: cut.after, calls: map[string]int{}}
			p.Port = port
			attempt, cancel := context.WithCancel(ctx)
			defer cancel()
			destination := "owner"
			cause := error(context.DeadlineExceeded)
			var owned []OwnedPayload
			switch cut.name {
			case "context":
				cancel()
				cause = context.Canceled
			case "configuration":
				destination = ""
				cause = nil
			case "conflict":
				expected = 42
				cause = ErrConflict
			case "token":
				p.NewID = func() (string, error) { return "", context.DeadlineExceeded }
			case "owned":
				owned = []OwnedPayload{{Stream: "other"}}
				cause = nil
			}
			prepared, err := p.PrepareAppendWithOwned(attempt, destination, expected, []byte("entry"), [][]byte{[]byte("body")}, owned, epoch.Add(time.Minute))
			var detail *AppendPrepareError
			if !errors.As(err, &detail) || detail.Phase != cut.phase || !strings.Contains(err.Error(), cut.phase) {
				t.Fatalf("preparation detail=%+v error=%v", detail, err)
			}
			if cause != nil && !errors.Is(err, cause) {
				t.Fatal("classification lost", err)
			}
			var acquisition *AcquisitionError
			if cut.acquisition != "" && (!errors.As(err, &acquisition) || acquisition.Phase != cut.acquisition) {
				t.Fatalf("acquisition detail=%+v error=%v", acquisition, err)
			}
			if prepared.destination != "" || port.rootWrites != 0 {
				t.Fatal("failed preparation exposed a plan or published a root", prepared, port.rootWrites)
			}
			actual, err := m.ReadRoot(ctx, "owner")
			if err != nil || !reflect.DeepEqual(before, actual) {
				t.Fatal("failed preparation changed canonical root", actual, err)
			}
			if cut.op != "" && port.calls[cut.op] != cut.at {
				t.Fatal("uncertain operation retried", port.calls)
			}
			if cut.name != "context" && attempt.Err() != nil {
				t.Fatal("request deadline canceled parent")
			}
			// Recovery creates new grants and immutable attempts from the witnessed
			// original head; it does not adopt any physical bytes left by the cut.
			oldObjects := make(map[string]bool, len(m.objects))
			for name := range m.objects {
				oldObjects[name] = true
			}
			p.Port, p.NewID = m, originalID
			recovered, err := p.PrepareAppend(ctx, "owner", before.Head, []byte("entry"), [][]byte{[]byte("body")}, epoch.Add(time.Minute))
			if err != nil {
				t.Fatal(err)
			}
			root, err := p.Commit(ctx, recovered)
			if err != nil || root.Graph.Count != before.Graph.Count+1 {
				t.Fatal("fresh preparation recovery failed", root, err)
			}
			for _, record := range m.blobs {
				if _, ok := record.Fence.Intents[recovered.publication.Token]; ok {
					if record.Fence.Phase != "ready" || oldObjects[record.Fence.Object] {
						t.Fatal("fresh publication adopted an unready grant or old upload")
					}
				}
			}
		})
	}
}
