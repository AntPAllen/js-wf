package sim

import (
	"fmt"
	"reflect"
	"sync"
	"testing"
)

type graphSeedResult struct {
	seed  int64
	trace Trace
	err   error
}
type graphSeedJob struct {
	seed  int64
	reply chan graphSeedResult
}

// Two independent schedules run concurrently under the test process's existing
// CPU/memory budget. Results are certified in seed order. Exactly two slots
// bound in-flight traces, even when one seed is slow. No simulation state is
// shared across seeds, and each worker checks the complete exact replay.
func graphChildSeedPipeline(t *testing.T, limit int64, run func(int64, *Trace) (Trace, error)) func(int64) graphSeedResult {
	t.Helper()
	jobs := make(chan graphSeedJob, 2)
	replies := [2]chan graphSeedResult{make(chan graphSeedResult, 1), make(chan graphSeedResult, 1)}
	var joined sync.WaitGroup
	for i := 0; i < 2; i++ {
		joined.Add(1)
		go func() {
			defer joined.Done()
			for job := range jobs {
				generated, err := run(job.seed, nil)
				if err == nil {
					replayed, e := run(job.seed, &generated)
					if e != nil || !reflect.DeepEqual(generated, replayed) {
						err = fmt.Errorf("child pipeline replay differs: %v", e)
					}
				}
				job.reply <- graphSeedResult{job.seed, generated, err}
			}
		}()
	}
	t.Cleanup(func() { close(jobs); joined.Wait() })
	for seed := int64(1); seed <= min(limit, int64(2)); seed++ {
		jobs <- graphSeedJob{seed, replies[(seed-1)%2]}
	}
	expected := int64(1)
	return func(seed int64) graphSeedResult {
		if seed != expected {
			t.Fatalf("seed pipeline consumed out of order: %d expected %d", seed, expected)
		}
		result := <-replies[(seed-1)%2]
		if result.seed != seed {
			t.Fatalf("seed pipeline returned wrong seed: %d expected %d", result.seed, seed)
		}
		expected++
		if next := seed + 2; next <= limit {
			jobs <- graphSeedJob{next, replies[(seed-1)%2]}
		}
		return result
	}
}

func TestGraphChildSeedPipelineOrdersAndJoins(t *testing.T) {
	var mu sync.Mutex
	active, maxActive := 0, 0
	release := make(chan struct{})
	secondStarted := make(chan struct{})
	firstStarted := make(chan struct{})
	var once sync.Once
	run := func(seed int64, replay *Trace) (Trace, error) {
		mu.Lock()
		active++
		maxActive = max(maxActive, active)
		mu.Unlock()
		defer func() { mu.Lock(); active--; mu.Unlock() }()
		if replay == nil && seed == 1 {
			close(firstStarted)
			<-release
		}
		if replay == nil && seed == 2 {
			<-firstStarted
			once.Do(func() { close(secondStarted) })
		}
		if replay != nil {
			return *replay, nil
		}
		return Trace{Seed: seed}, nil
	}
	resultFor := graphChildSeedPipeline(t, 6, run)
	<-secondStarted
	close(release)
	for seed := int64(1); seed <= 6; seed++ {
		r := resultFor(seed)
		if r.err != nil || r.seed != seed || r.trace.Seed != seed {
			t.Fatal("pipeline result differs", r)
		}
	}
	mu.Lock()
	defer mu.Unlock()
	if active != 0 || maxActive != 2 {
		t.Fatal("seed execution was not bounded and joined", active, maxActive)
	}
}
