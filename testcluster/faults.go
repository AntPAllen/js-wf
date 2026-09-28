package testcluster

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"time"
)

type FaultOp string

const (
	KillNode       FaultOp = "KillNode"
	PartitionNodes FaultOp = "PartitionNodes"
	PauseNode      FaultOp = "PauseNode"
	SlowDisk       FaultOp = "SlowDisk"
)

type FaultEvent struct {
	AtMillis      int64   `json:"at_ms"`
	Op            FaultOp `json:"op"`
	A             int     `json:"a"`
	B             int     `json:"b,omitempty"`
	LatencyMillis int64   `json:"latency_ms,omitempty"`
}

type FaultSchedule struct {
	Seed   int64        `json:"seed"`
	Events []FaultEvent `json:"events"`
}

func SeedFromEnv() (int64, error) {
	value := os.Getenv("FAULT_SEED")
	if value == "" {
		return 1, nil
	}
	seed, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("FAULT_SEED: %w", err)
	}
	return seed, nil
}

// Generate is deterministic for a seed and independent of wall time.
func Generate(seed int64, count int, maxGap time.Duration) (FaultSchedule, error) {
	if count < 0 || maxGap < 0 {
		return FaultSchedule{}, fmt.Errorf("invalid fault count or gap")
	}
	schedule := FaultSchedule{Seed: seed, Events: make([]FaultEvent, 0, count)}
	rng := rand.New(rand.NewSource(seed))
	verbs := []FaultOp{KillNode, PartitionNodes, PauseNode, SlowDisk}
	var at int64
	for i := 0; i < count; i++ {
		if maxGap > 0 {
			at += rng.Int63n(maxGap.Milliseconds() + 1)
		}
		a := rng.Intn(3)
		b := (a + 1 + rng.Intn(2)) % 3
		schedule.Events = append(schedule.Events, FaultEvent{AtMillis: at, Op: verbs[rng.Intn(len(verbs))], A: a, B: b, LatencyMillis: int64(rng.Intn(5000) + 1)})
	}
	return schedule, nil
}

func (s FaultSchedule) Validate() error {
	var prev int64
	for i, e := range s.Events {
		if e.AtMillis < prev || e.AtMillis < 0 || e.A < 0 || e.A > 2 || e.B < 0 || e.B > 2 {
			return fmt.Errorf("invalid fault event %d", i)
		}
		switch e.Op {
		case KillNode, PartitionNodes, PauseNode, SlowDisk:
			if e.Op == PartitionNodes && e.A == e.B {
				return fmt.Errorf("invalid fault event %d: identical partition nodes", i)
			}
		default:
			return fmt.Errorf("invalid fault verb %q", e.Op)
		}
		prev = e.AtMillis
	}
	return nil
}

func (s FaultSchedule) Save(path string) error {
	if err := s.Validate(); err != nil {
		return err
	}
	data, err := json.MarshalIndent(s, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	return os.WriteFile(path, data, 0644)
}

func LoadFaultSchedule(path string) (FaultSchedule, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return FaultSchedule{}, err
	}
	var schedule FaultSchedule
	if err := json.Unmarshal(data, &schedule); err != nil {
		return FaultSchedule{}, err
	}
	if err := schedule.Validate(); err != nil {
		return FaultSchedule{}, err
	}
	return schedule, nil
}

// Run replays the recorded event times. apply controls the actual fault
// mechanism, allowing the same schedule on in-process and VM fixtures.
func (s FaultSchedule) Run(ctx context.Context, apply func(FaultEvent) error) error {
	if err := s.Validate(); err != nil {
		return err
	}
	start := time.Now()
	for _, event := range s.Events {
		if err := ctx.Err(); err != nil {
			return err
		}
		wait := time.Until(start.Add(time.Duration(event.AtMillis) * time.Millisecond))
		if wait > 0 {
			timer := time.NewTimer(wait)
			select {
			case <-ctx.Done():
				timer.Stop()
				return ctx.Err()
			case <-timer.C:
			}
		}
		if err := apply(event); err != nil {
			return err
		}
	}
	return nil
}
