// Package handlecache coalesces metadata lookups while letting each waiting
// caller cancel independently of the caller performing the network request.
package handlecache

import (
	"context"
	"sync"
)

// Cache retains successful immutable client handles. Its zero value is ready
// to use. Failures are not cached; one later caller can retry the lookup.
type Cache[T any] struct {
	mu      sync.Mutex
	value   T
	ready   bool
	loading chan struct{}
}

func (c *Cache[T]) Get(ctx context.Context, load func(context.Context) (T, error)) (T, error) {
	var zero T
	for {
		if err := ctx.Err(); err != nil {
			return zero, err
		}
		c.mu.Lock()
		if c.ready {
			value := c.value
			c.mu.Unlock()
			return value, nil
		}
		if pending := c.loading; pending != nil {
			c.mu.Unlock()
			select {
			case <-ctx.Done():
				return zero, ctx.Err()
			case <-pending:
			}
			continue
		}
		c.loading = make(chan struct{})
		c.mu.Unlock()
		value, err := load(ctx)
		c.mu.Lock()
		if err == nil {
			c.value = value
			c.ready = true
		}
		close(c.loading)
		c.loading = nil
		c.mu.Unlock()
		if canceled := ctx.Err(); canceled != nil {
			return zero, canceled
		}
		return value, err
	}
}
