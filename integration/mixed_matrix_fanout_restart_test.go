//go:build linux

package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"js-wf/journal"
	"js-wf/testcluster"
	"js-wf/wf"
	"js-wf/worker"
)

// The barrier holds actual grandchild effects only while locating a restart
// cut. It is released after replica recovery, including on controller errors.
type matrixFanoutBarrier struct {
	mu      sync.Mutex
	release chan struct{}
	entered chan struct{}
	parents []string
}

func (b *matrixFanoutBarrier) noteParent(id string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.parents = append(b.parents, id)
	if len(b.parents) > 64 {
		b.parents = b.parents[len(b.parents)-64:]
	}
}

func (b *matrixFanoutBarrier) handlers() map[string]worker.Handler {
	handlers := matrixLeaderHandlers()
	handlers["matrixgrandchild"] = func(c *wf.Context, _ json.RawMessage) (json.RawMessage, error) {
		value, err := wf.Run(c, "effect", 0, func(ctx context.Context) (int, error) {
			b.mu.Lock()
			release, entered := b.release, b.entered
			b.mu.Unlock()
			if release != nil {
				select {
				case entered <- struct{}{}:
				default:
				}
				select {
				case <-ctx.Done():
					return 0, ctx.Err()
				case <-release:
				}
			}
			return 3, nil
		})
		encoded, _ := json.Marshal(value)
		return encoded, err
	}
	return handlers
}

func (b *matrixFanoutBarrier) restart(ctx context.Context, js jetstream.JetStream, cluster *testcluster.ProcessCluster, scheduled time.Time) (matrixLeaderFault, error) {
	prefix := os.Getenv("MATRIX_ARTIFACT_PREFIX")
	if prefix != "" {
		prefix = fmt.Sprintf("%s-fault-%d", prefix, scheduled.UnixNano())
	}
	return b.restartWith(ctx, js, scheduled, prefix, func() (matrixLeaderFault, error) { return killMatrixAllServers(ctx, js, cluster, scheduled) })
}

// Share the cut and identity checks across native R3 and container R5 rows.
func (b *matrixFanoutBarrier) restartWith(ctx context.Context, js jetstream.JetStream, scheduled time.Time, artifactPrefix string, kill func() (matrixLeaderFault, error)) (matrixLeaderFault, error) {
	event := matrixLeaderFault{Scheduled: scheduled, Node: -1}
	b.mu.Lock()
	b.release, b.entered = make(chan struct{}), make(chan struct{}, 1)
	entered := b.entered
	b.mu.Unlock()
	defer func() {
		b.mu.Lock()
		close(b.release)
		b.release = nil
		b.mu.Unlock()
	}()
	bound, done := context.WithTimeout(ctx, 20*time.Second)
	defer done()
	select {
	case <-entered:
	case <-bound.Done():
		return event, fmt.Errorf("no grandchild effect at restart cut: %w", bound.Err())
	}
	j := journal.New(js)
	var prefix []journal.Record
	var childPrefixes map[string][]journal.Record
	for bound.Err() == nil {
		b.mu.Lock()
		parents := append([]string(nil), b.parents...)
		b.mu.Unlock()
		for i := len(parents) - 1; i >= 0; i-- {
			records, tail, err := j.Read(bound, "matrixfanout", parents[i])
			if err != nil {
				return event, err
			}
			if len(records) == 0 || records[len(records)-1].Kind != journal.Suspended {
				continue
			}
			children := make(map[string]bool)
			for _, record := range records {
				if record.Kind != journal.StepRequested {
					continue
				}
				var request struct {
					Kind    string `json:"kind"`
					ChildID string `json:"child_id"`
				}
				if err := json.Unmarshal(record.Payload, &request); err != nil {
					return event, err
				}
				if request.Kind == "call_async" {
					if request.ChildID == "" || children[request.ChildID] {
						return event, fmt.Errorf("invalid or duplicate child request in %s", parents[i])
					}
					children[request.ChildID] = true
				}
			}
			if len(children) != 6 {
				continue
			}
			var pending []string
			childPrefixes = make(map[string][]journal.Record)
			for child := range children {
				childRecords, _, err := j.Read(bound, "matrixchild", child)
				if err != nil {
					return event, err
				}
				childPrefixes[child] = childRecords
				if len(childRecords) == 0 || (childRecords[len(childRecords)-1].Kind != journal.Completed && childRecords[len(childRecords)-1].Kind != journal.Failed) {
					pending = append(pending, child)
				}
			}
			if len(pending) == 0 {
				continue
			}
			sort.Strings(pending)
			event.FanoutPendingChildren = pending

			event.FanoutParent, event.FanoutTail = parents[i], tail
			for _, record := range records {
				if record.Kind != journal.StepRequested {
					continue
				}
				var request struct {
					Kind    string `json:"kind"`
					ChildID string `json:"child_id"`
				}
				_ = json.Unmarshal(record.Payload, &request)
				if request.Kind == "call_async" {
					event.FanoutChildren = append(event.FanoutChildren, request.ChildID)
				}
			}
			prefix = records
			break
		}
		if prefix != nil {
			break
		}
		select {
		case <-bound.Done():
		case <-time.After(25 * time.Millisecond):
		}
	}
	if prefix == nil {
		return event, fmt.Errorf("no suspended six-child parent at restart cut: %w", bound.Err())
	}
	if artifactPrefix != "" {
		cut := struct {
			ObservedAt    time.Time
			Fault         matrixLeaderFault
			ParentPrefix  []journal.Record
			ChildPrefixes map[string][]journal.Record
		}{time.Now().UTC(), event, prefix, childPrefixes}
		data, err := json.MarshalIndent(cut, "", "  ")
		if err != nil {
			return event, err
		}
		if err = os.WriteFile(artifactPrefix+"-fanout-cut.json", data, 0644); err != nil {
			return event, err
		}
	}
	killed, err := kill()
	event.Killed, event.Healed, event.Nodes = killed.Killed, killed.Healed, killed.Nodes
	if err != nil {
		return event, err
	}
	verify, stop := context.WithTimeout(ctx, 10*time.Second)
	defer stop()
	records, _, err := j.Read(verify, "matrixfanout", event.FanoutParent)
	if err != nil {
		return event, err
	}
	if len(records) < len(prefix) {
		return event, fmt.Errorf("fanout %s lost journal prefix", event.FanoutParent)
	}
	before, _ := json.Marshal(prefix)
	after, _ := json.Marshal(records[:len(prefix)])
	if !bytes.Equal(before, after) {
		return event, fmt.Errorf("fanout %s changed journal prefix during restart", event.FanoutParent)
	}
	if artifactPrefix != "" {
		data, err := json.MarshalIndent(records, "", "  ")
		if err != nil {
			return event, err
		}
		if err = os.WriteFile(artifactPrefix+"-fanout-recovered-prefix.json", data, 0644); err != nil {
			return event, err
		}
	}
	return event, nil
}

// Final proof ties each restart cut to the same six terminal children and
// exactly two terminal grandchildren per child, rather than only counting
// aggregate completions from the mixed generator.
func verifyMatrixRestartFanouts(ctx context.Context, js jetstream.JetStream, faults []matrixLeaderFault) error {
	root := os.Getenv("MATRIX_ARTIFACT_PREFIX")
	if root != "" {
		root += "-fanout-final"
		if err := os.Mkdir(root, 0700); err != nil {
			return err
		}
	}
	return verifyMatrixRestartFanoutsWithArtifacts(ctx, js, faults, root)
}

func verifyMatrixRestartFanoutsWithArtifacts(ctx context.Context, js jetstream.JetStream, faults []matrixLeaderFault, root string) error {
	j := journal.New(js)
	for faultIndex, fault := range faults {
		if fault.FanoutParent == "" || len(fault.FanoutChildren) != 6 || len(fault.FanoutPendingChildren) == 0 {
			return fmt.Errorf("restart lacks an unfinished six-child fanout cut: %+v", fault)
		}
		records, _, err := j.Read(ctx, "matrixfanout", fault.FanoutParent)
		if err != nil {
			return err
		}
		parentRecords := records
		childRecords := make(map[string][]journal.Record)
		grandchildRecords := make(map[string][]journal.Record)
		children, err := matrixTerminalChildren(records, 6)
		if err != nil {
			return fmt.Errorf("%s: %w", fault.FanoutParent, err)
		}
		expected := append([]string(nil), fault.FanoutChildren...)
		sort.Strings(expected)
		for i, child := range children {
			if child != expected[i] {
				return fmt.Errorf("restart changed child identity for %s", fault.FanoutParent)
			}
			records, _, err := j.Read(ctx, "matrixchild", child)
			if err != nil {
				return err
			}
			childRecords[child] = records
			grandchildren, err := matrixTerminalChildren(records, 2)
			if err != nil {
				return fmt.Errorf("child %s: %w", child, err)
			}
			for _, grandchild := range grandchildren {
				records, _, err := j.Read(ctx, "matrixgrandchild", grandchild)
				if err != nil {
					return err
				}
				grandchildRecords[grandchild] = records
				if len(records) == 0 || records[len(records)-1].Kind != journal.Completed {
					return fmt.Errorf("grandchild %s not completed", grandchild)
				}
			}

		}
		if root != "" {
			proof := struct {
				Parent        []journal.Record
				Children      map[string][]journal.Record
				Grandchildren map[string][]journal.Record
			}{parentRecords, childRecords, grandchildRecords}
			data, err := json.MarshalIndent(proof, "", "  ")
			if err != nil {
				return err
			}
			if err = os.WriteFile(fmt.Sprintf("%s/fault-%d-fanout-final.json", root, faultIndex+1), data, 0644); err != nil {
				return err
			}
		}
	}
	return nil
}

func matrixTerminalChildren(records []journal.Record, count int) ([]string, error) {
	if len(records) == 0 || records[len(records)-1].Kind != journal.Completed {
		return nil, fmt.Errorf("fanout not completed")
	}
	var children []string
	seen := make(map[string]bool)
	for _, record := range records {
		if record.Kind != journal.StepRequested {
			continue
		}
		var request struct {
			Kind    string `json:"kind"`
			ChildID string `json:"child_id"`
		}
		if err := json.Unmarshal(record.Payload, &request); err != nil {
			return nil, err
		}
		if request.Kind == "call_async" {
			if request.ChildID == "" || seen[request.ChildID] {
				return nil, fmt.Errorf("duplicate or empty child ID")
			}
			seen[request.ChildID] = true
			children = append(children, request.ChildID)
		}
	}
	if len(children) != count {
		return nil, fmt.Errorf("children=%d want=%d", len(children), count)
	}
	sort.Strings(children)
	return children, nil
}
