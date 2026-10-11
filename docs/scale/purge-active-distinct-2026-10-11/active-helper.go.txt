package integration_test

import (
	"sync"
	"testing"
)

// A total callback count can hide a missing invocation behind a duplicate.
// Entry/exit and readiness share one lock, so the gate observes one cohort.
type purgeScaleActiveHandlers struct {
	mu            sync.Mutex
	counts        []int
	total, unique int
}
type purgeScaleActiveSnapshot struct {
	Total, Unique, Expected          int
	MissingIndices, DuplicateIndices []int
}

func newPurgeScaleActiveHandlers(count int) *purgeScaleActiveHandlers {
	return &purgeScaleActiveHandlers{counts: make([]int, count)}
}
func (a *purgeScaleActiveHandlers) enter(index int) func() {
	a.mu.Lock()
	if a.counts[index] == 0 {
		a.unique++
	}
	a.counts[index]++
	a.total++
	a.mu.Unlock()
	return func() {
		a.mu.Lock()
		a.counts[index]--
		a.total--
		if a.counts[index] == 0 {
			a.unique--
		}
		a.mu.Unlock()
	}
}
func (a *purgeScaleActiveHandlers) ready() bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.unique == len(a.counts) && a.total == len(a.counts)
}
func (a *purgeScaleActiveHandlers) snapshot() purgeScaleActiveSnapshot {
	a.mu.Lock()
	defer a.mu.Unlock()
	s := purgeScaleActiveSnapshot{Total: a.total, Unique: a.unique, Expected: len(a.counts)}
	for i, n := range a.counts {
		if n == 0 && len(s.MissingIndices) < 20 {
			s.MissingIndices = append(s.MissingIndices, i)
		}
		if n > 1 && len(s.DuplicateIndices) < 20 {
			s.DuplicateIndices = append(s.DuplicateIndices, i)
		}
	}
	return s
}
func TestPurgeScaleActiveHandlersRequireDistinctCohort(t *testing.T) {
	a := newPurgeScaleActiveHandlers(2)
	first := a.enter(0)
	duplicate := a.enter(0)
	if a.ready() {
		t.Fatal("duplicate callbacks substituted for a missing invocation")
	}
	s := a.snapshot()
	if s.Total != 2 || s.Unique != 1 || len(s.MissingIndices) != 1 || s.MissingIndices[0] != 1 || len(s.DuplicateIndices) != 1 || s.DuplicateIndices[0] != 0 {
		t.Fatal("collision diagnostic", s)
	}
	second := a.enter(1)
	if a.ready() {
		t.Fatal("extra active duplicate accepted")
	}
	duplicate()
	if !a.ready() {
		t.Fatal("distinct cohort rejected")
	}
	first()
	if a.ready() {
		t.Fatal("departed invocation remains ready")
	}
	second()
	s = a.snapshot()
	if s.Total != 0 || s.Unique != 0 || a.ready() {
		t.Fatal("cohort not drained", s)
	}
}
func TestPurgeScaleActiveHandlersConcurrentCohort(t *testing.T) {
	const count = 128
	a := newPurgeScaleActiveHandlers(count)
	release := make(chan struct{})
	var entered, done sync.WaitGroup
	entered.Add(count)
	done.Add(count)
	for i := 0; i < count; i++ {
		go func(index int) { defer done.Done(); leave := a.enter(index); entered.Done(); <-release; leave() }(i)
	}
	entered.Wait()
	ready := a.ready()
	s := a.snapshot()
	close(release)
	done.Wait()
	if !ready || s.Total != count || s.Unique != count || len(s.MissingIndices) != 0 || len(s.DuplicateIndices) != 0 {
		t.Fatal("concurrent distinct cohort", s)
	}
	if a.ready() || a.snapshot().Total != 0 {
		t.Fatal("concurrent cohort not drained")
	}
}
