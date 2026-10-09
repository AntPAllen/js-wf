package journal

import (
	"context"
	"fmt"
	"js-wf/internal/graphpublication"
	"time"
)

// GraphReaderMaintenance exposes reader fencing only. It does not expose the
// publication port, object collection, journal mutation or payload grants.
type GraphReaderMaintenance struct {
	protocol graphpublication.Protocol
	scope    string
}

// ReaderMaintenance binds the facade to this store's isolated native scope.
// Capability/scope checks perform no storage operations.
func (s *GraphStore) ReaderMaintenance() (*GraphReaderMaintenance, error) {
	if s == nil {
		return nil, fmt.Errorf("reader maintenance requires graph store")
	}
	if _, ok := s.cfg.Protocol.Port.(graphpublication.RootScanPort); !ok {
		return nil, fmt.Errorf("reader maintenance requires bounded scan")
	}
	if _, ok := s.cfg.Protocol.Port.(graphpublication.RootCatalogWatermarkPort); !ok {
		return nil, fmt.Errorf("reader maintenance requires watermark")
	}
	port, ok := s.cfg.Protocol.Port.(interface{ ReaderMaintenanceScope() string })
	if !ok {
		return nil, fmt.Errorf("reader maintenance requires isolated scope")
	}
	scope := port.ReaderMaintenanceScope()
	if !startHex(scope, 32) {
		return nil, fmt.Errorf("invalid reader maintenance scope")
	}
	return &GraphReaderMaintenance{protocol: s.cfg.Protocol, scope: scope}, nil
}
func (m *GraphReaderMaintenance) ReaderMaintenanceScope() string {
	if m == nil {
		return ""
	}
	return m.scope
}
func (m *GraphReaderMaintenance) BeginReaderSweep(ctx context.Context) (graphpublication.ReaderSweepCursor, error) {
	if m == nil {
		return graphpublication.ReaderSweepCursor{}, fmt.Errorf("nil reader maintenance")
	}
	return m.protocol.BeginReaderSweep(ctx)
}
func (m *GraphReaderMaintenance) ExpireReaderBatch(ctx context.Context, c graphpublication.ReaderSweepCursor, budget int, now time.Time) (graphpublication.ReaderSweepResult, error) {
	if m == nil {
		return graphpublication.ReaderSweepResult{Cursor: c}, fmt.Errorf("nil reader maintenance")
	}
	return m.protocol.ExpireReaderBatch(ctx, c, budget, now)
}
