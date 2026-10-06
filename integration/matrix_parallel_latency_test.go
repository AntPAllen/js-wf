//go:build linux

package integration_test

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

type matrixInvocationAuditResult struct {
	samples []matrixLatencySample
	mixed   bool
}

// Independent point audits retain their original 20-second request context.
// At most 32 are live; output stays in invocation sequence order. Cancellation
// joins all readers before returning and a failed read invalidates the entire
// result. This changes final audit scheduling, not timestamps or latency gates.
func matrixParallelInvocationAudits(ctx context.Context, first, last uint64, read func(context.Context, uint64) (matrixInvocationAuditResult, error)) ([]matrixInvocationAuditResult, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if last < first {
		return nil, nil
	}
	if last-first >= uint64(int(^uint(0)>>1)) {
		return nil, fmt.Errorf("invocation audit range exceeds addressable memory")
	}
	count := int(last-first) + 1
	results := make([]matrixInvocationAuditResult, count)
	bound, cancel := context.WithCancel(ctx)
	defer cancel()
	var next atomic.Int64
	var joined sync.WaitGroup
	var failed sync.Once
	var failure error
	for worker := 0; worker < min(32, count); worker++ {
		joined.Add(1)
		go func() {
			defer joined.Done()
			for bound.Err() == nil {
				index := int(next.Add(1) - 1)
				if index >= count {
					return
				}
				attempt, stop := context.WithTimeout(bound, 20*time.Second)
				result, err := read(attempt, first+uint64(index))
				stop()
				if err != nil {
					failed.Do(func() {
						failure = fmt.Errorf("invocation sequence %d: %w", first+uint64(index), err)
						cancel()
					})
					return
				}
				results[index] = result
			}
		}()
	}
	joined.Wait()
	if failure != nil {
		return nil, failure
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return results, nil
}
