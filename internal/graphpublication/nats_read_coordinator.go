package graphpublication

import (
	"context"
	"sync"
)

// This coordinates physical read witnesses on one adapter, never authority or
// payload ownership. It does not replace the quorum witness or server CAS.
// Entries exist only while reads are active or queued, bounding local memory.
type authorityReadCoordinator struct {
	mu      sync.Mutex
	entries map[string]*authorityReadEntry
}
type authorityReadEntry struct {
	gate       chan struct{}
	references int
}

func (c *authorityReadCoordinator) acquire(ctx context.Context, subject string) (func(), error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if c == nil {
		return func() {}, nil
	}
	c.mu.Lock()
	if c.entries == nil {
		c.entries = make(map[string]*authorityReadEntry)
	}
	entry := c.entries[subject]
	if entry == nil {
		entry = &authorityReadEntry{gate: make(chan struct{}, 1)}
		entry.gate <- struct{}{}
		c.entries[subject] = entry
	}
	entry.references++
	c.mu.Unlock()
	dropReference := func() {
		c.mu.Lock()
		entry.references--
		if entry.references == 0 {
			delete(c.entries, subject)
		}
		c.mu.Unlock()
	}
	select {
	case <-ctx.Done():
		dropReference()
		return nil, ctx.Err()
	case <-entry.gate:
	}
	release := func() { entry.gate <- struct{}{}; dropReference() }
	if err := ctx.Err(); err != nil {
		release()
		return nil, err
	}
	return release, nil
}
