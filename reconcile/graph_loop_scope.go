package reconcile

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/nats-io/nats.go/jetstream"
)

// Graph recovery cursor/lease state belongs to the graph namespace, even when
// several isolated stores share WF_STATE and WF_LEASE in one account/domain.
type graphRepairLoopPort struct {
	*jetStreamLoopPort
	scope string
}

func (p graphRepairLoopPort) key(kind string) (string, error) {
	if _, err := (nativeReaderExpiryPort{scope: p.scope}).scopeKey(); err != nil {
		return "", err
	}
	switch kind {
	case "start", "graph-start", "graph-signal", "graph-terminal", "graph-terminal-audit", "graph-continuation", "signal", "timer", "fallback-timer", "suspended":
		return "graph-repair." + p.scope + "." + kind, nil
	default:
		return "", fmt.Errorf("invalid graph repair loop kind %q", kind)
	}
}

func (p graphRepairLoopPort) Acquire(ctx context.Context, kind, workerID string) (LoopLease, error) {
	if _, err := p.key(kind); err != nil {
		return nil, err
	}
	return p.jetStreamLoopPort.Acquire(ctx, "graph-repair-"+p.scope+"-"+kind, workerID)
}

func (p graphRepairLoopPort) LoadCursor(ctx context.Context, kind string) (uint64, uint64, error) {
	key, err := p.key(kind)
	if err != nil {
		return 0, 0, err
	}
	entry, err := p.state.Get(ctx, "scan."+key)
	if errors.Is(err, jetstream.ErrKeyNotFound) {
		return 1, 0, nil
	}
	if err != nil {
		return 0, 0, err
	}
	var stored struct {
		Version     int
		Scope, Kind string
		Next        uint64
	}
	if err := json.Unmarshal(entry.Value(), &stored); err != nil || stored.Version != 1 || stored.Scope != p.scope || stored.Kind != kind || stored.Next == 0 {
		return 0, 0, fmt.Errorf("invalid scoped graph repair checkpoint")
	}
	return stored.Next, entry.Revision(), nil
}

func (p graphRepairLoopPort) SaveCursor(ctx context.Context, kind string, next, revision uint64) (uint64, error) {
	key, err := p.key(kind)
	if err != nil {
		return 0, err
	}
	if next == 0 {
		return 0, fmt.Errorf("zero graph repair cursor")
	}
	data, err := json.Marshal(struct {
		Version     int
		Scope, Kind string
		Next        uint64
	}{1, p.scope, kind, next})
	if err != nil {
		return 0, err
	}
	var updated uint64
	if revision == 0 {
		updated, err = p.state.Create(ctx, "scan."+key, data)
	} else {
		updated, err = p.state.Update(ctx, "scan."+key, data, revision)
	}
	var api *jetstream.APIError
	if errors.Is(err, jetstream.ErrKeyExists) || errors.Is(err, jetstream.ErrKeyRevisionMismatch) || errors.As(err, &api) && api.ErrorCode == 10164 {
		return 0, ErrCursorStale
	}
	return updated, err
}
