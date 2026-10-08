package reconcile

import (
	"context"
	"js-wf/journal"
)

// graphCatalogCycle retains one scheduling watermark across serial budgeted
// calls. Read witnesses and concurrent publications are deferred to the next
// cycle. Restart, rewind, changed checkpoint or uncertainty refreshes the bound.
// A captured bound is local to each Scan, including reentrant observer calls.
type graphCatalogCycle struct {
	through      uint64
	ready        bool
	expectedNext uint64
}

func (c *graphCatalogCycle) begin(ctx context.Context, graph *journal.GraphStore, next uint64) (uint64, error) {
	if !c.ready || next == 1 || next != c.expectedNext {
		through, err := graph.StartCatalogHighWater(ctx)
		if err != nil {
			return 0, err
		}
		c.through, c.ready = through, true
	}
	return c.through, nil
}
func (c *graphCatalogCycle) end(result ScanResult, err error) {
	c.expectedNext = result.NextSequence
	if err != nil || result.NextSequence == 1 {
		c.ready = false
	}
}
