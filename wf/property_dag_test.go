package wf

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"reflect"
	"strconv"
	"testing"
	"time"
)

type dagOp struct {
	kind  string
	id    int
	value int
}

var errDAGKill = errors.New("injected workflow kill")

func randomDAG(seed int64) ([]dagOp, []Signal, int, int) {
	rng := rand.New(rand.NewSource(seed))
	ops := []dagOp{{kind: "run", id: 0, value: 1}, {kind: "sleep", id: 1}, {kind: "call", id: 2, value: 2}}
	pending := []dagOp{ops[2]}
	for i := 0; i < rng.Intn(10); i++ {
		kinds := []string{"run", "sleep", "call"}
		if len(pending) != 0 {
			kinds = append(kinds, "await")
		}
		kind := kinds[rng.Intn(len(kinds))]
		if kind == "await" {
			index := rng.Intn(len(pending))
			call := pending[index]
			ops = append(ops, dagOp{kind: kind, id: call.id, value: call.value})
			pending = append(pending[:index], pending[index+1:]...)
			continue
		}
		op := dagOp{kind: kind, id: len(ops), value: 1 + rng.Intn(9)}
		ops = append(ops, op)
		if kind == "call" {
			pending = append(pending, op)
		}
	}
	var signals []Signal
	for index, op := range ops {
		if op.kind != "call" {
			continue
		}
		payload, _ := json.Marshal(Outcome{Result: []byte(strconv.Itoa(op.value * 3))})
		signals = append(signals, Signal{Sequence: uint64(len(signals) + 1), Name: "child_" + strconv.Itoa(2*index), Payload: payload})
	}
	rng.Shuffle(len(pending), func(i, j int) { pending[i], pending[j] = pending[j], pending[i] })
	for _, call := range pending {
		ops = append(ops, dagOp{kind: "await", id: call.id, value: call.value})
	}
	var expected int
	for _, op := range ops {
		switch op.kind {
		case "run":
			expected += (expected + op.value) * 2
		case "await":
			expected += op.value * 3
		}
	}
	return ops, signals, expected, rng.Intn(2 * len(ops))
}

func executeDAG(ops []dagOp, signals []Signal, records *[]Entry, killAfter int, starts map[string]struct{}) (int, error) {
	appendFn := func(_ context.Context, kind Kind, payload json.RawMessage) error {
		if killAfter == 0 {
			return errDAGKill
		}
		if killAfter > 0 {
			killAfter--
		}
		*records = append(*records, Entry{Index: uint64(len(*records) + 1), Kind: kind, Payload: append(json.RawMessage(nil), payload...)})
		return nil
	}
	c := NewContext(context.Background(), append([]Entry(nil), (*records)...), appendFn, signals...)
	c.SetChildSupport("dag", "root", 1, func(_ context.Context, _ string, childID string, _ []byte, _ string) error {
		starts[childID] = struct{}{}
		return nil
	})
	result, err := runDAG(c, ops)
	if err != nil {
		return 0, err
	}
	return result, c.CheckComplete()
}

func runDAG(c *Context, ops []dagOp) (int, error) {
	promises := map[int]Promise{}
	var total int
	for _, op := range ops {
		switch op.kind {
		case "run":
			input := total + op.value
			value, err := Run(c, fmt.Sprintf("step-%d", op.id), input, func(context.Context) (int, error) {
				return input * 2, nil
			})
			if err != nil {
				return 0, err
			}
			total += value
		case "sleep":
			if err := Sleep(c, fmt.Sprintf("zero-%d", op.id), 0*time.Second); err != nil {
				return 0, err
			}
		case "call":
			promise, err := CallAsync(c, "child", []byte(strconv.Itoa(total+op.value)))
			if err != nil {
				return 0, err
			}
			promises[op.id] = promise
		case "await":
			value, err := AwaitPromise(c, promises[op.id])
			if err != nil {
				return 0, err
			}
			got, err := strconv.Atoi(string(value))
			if err != nil {
				return 0, err
			}
			total += got
		default:
			return 0, fmt.Errorf("unknown DAG operation %q", op.kind)
		}
	}
	return total, nil
}

func TestRandomWorkflowDAGsRecoverAfterKill(t *testing.T) {
	const cases = 10000
	for seed := int64(1); seed <= cases; seed++ {
		ops, signals, expected, cut := randomDAG(seed)
		var clean []Entry
		cleanStarts := map[string]struct{}{}
		cleanResult, err := executeDAG(ops, signals, &clean, -1, cleanStarts)
		if err != nil || cleanResult != expected || len(cleanStarts) != len(signals) || len(clean) != 2*len(ops) {
			t.Fatalf("seed %d clean result=%d want=%d entries=%d/%d children=%d/%d error=%v", seed, cleanResult, expected, len(clean), 2*len(ops), len(cleanStarts), len(signals), err)
		}
		cuts := []int{cut}
		if seed <= 100 {
			cuts = cuts[:0]
			for index := range clean {
				cuts = append(cuts, index)
			}
		}
		for _, cut := range cuts {
			var retained []Entry
			faultedStarts := map[string]struct{}{}
			if _, err := executeDAG(ops, signals, &retained, cut, faultedStarts); !errors.Is(err, errDAGKill) {
				t.Fatalf("seed %d cut=%d did not kill workflow: %v", seed, cut, err)
			}
			recovered, err := executeDAG(ops, signals, &retained, -1, faultedStarts)
			if err != nil || recovered != expected || !reflect.DeepEqual(retained, clean) || !reflect.DeepEqual(faultedStarts, cleanStarts) {
				t.Fatalf("seed %d cut=%d recovered=%d want=%d error=%v records=%d/%d children=%d/%d", seed, cut, recovered, expected, err, len(retained), len(clean), len(faultedStarts), len(cleanStarts))
			}
		}
	}
}
