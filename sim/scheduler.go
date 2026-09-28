// Package sim provides a deterministic transport model for fast workflow
// state-machine tests. It is not a replacement for real JetStream fixtures.
package sim

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"os"
	"reflect"
	"sort"
)

const TraceVersion = 3
const DefaultMaxSteps = 100000

var ErrStepLimit = errors.New("simulation step limit reached")

type StepLimitError struct {
	Limit      int
	AtMillis   int64
	Enabled    []string
	LastChosen string
}

func (e *StepLimitError) Error() string {
	return fmt.Sprintf("%v after %d choices at virtual time %dms; last=%q pending=%v", ErrStepLimit, e.Limit, e.AtMillis, e.LastChosen, e.Enabled)
}

func (*StepLimitError) Unwrap() error { return ErrStepLimit }

type Decision struct {
	AtMillis int64    `json:"at_ms"`
	Enabled  []string `json:"enabled"`
	Chosen   string   `json:"chosen"`
}

type TransportEvent struct {
	AtMillis   int64  `json:"at_ms"`
	Operation  string `json:"operation"`
	Subject    string `json:"subject,omitempty"`
	Expected   uint64 `json:"expected,omitempty"`
	Sequence   uint64 `json:"sequence,omitempty"`
	DataSHA256 string `json:"data_sha256,omitempty"`
	Outcome    string `json:"outcome,omitempty"`
}

type Trace struct {
	Version   int              `json:"version"`
	Seed      int64            `json:"seed"`
	Workload  string           `json:"workload"`
	StepLimit int              `json:"step_limit,omitempty"`
	Decisions []Decision       `json:"decisions"`
	Transport []TransportEvent `json:"transport"`
}

func validTraceVersion(version int) bool { return version == 2 || version == TraceVersion }

func (t Trace) validate() error {
	if !validTraceVersion(t.Version) {
		return fmt.Errorf("unsupported simulation trace version %d", t.Version)
	}
	if t.Workload == "" {
		return fmt.Errorf("simulation trace has no workload")
	}
	if t.Version == TraceVersion && t.StepLimit < 1 {
		return fmt.Errorf("simulation trace has invalid step limit %d", t.StepLimit)
	}
	return nil
}

func (t Trace) Marshal() ([]byte, error) {
	if err := t.validate(); err != nil {
		return nil, err
	}
	return json.MarshalIndent(t, "", "  ")
}

func (t Trace) Save(path string) error {
	data, err := t.Marshal()
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(data, '\n'), 0644)
}

func LoadTrace(path string) (Trace, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Trace{}, err
	}
	var trace Trace
	if err := json.Unmarshal(data, &trace); err != nil {
		return Trace{}, err
	}
	if err := trace.validate(); err != nil {
		return Trace{}, err
	}
	return trace, nil
}

// Scheduler chooses from a canonical enabled set. Replay consumes recorded
// choices and fails immediately if an interleaving is no longer reachable.
type Scheduler struct {
	rng      *rand.Rand
	trace    Trace
	replay   *Trace
	position int
	now      int64
	maxSteps int
}

func NewScheduler(seed int64) *Scheduler {
	return &Scheduler{rng: rand.New(rand.NewSource(seed)), trace: Trace{Version: TraceVersion, Seed: seed, StepLimit: DefaultMaxSteps, Decisions: []Decision{}, Transport: []TransportEvent{}}, maxSteps: DefaultMaxSteps}
}

// SetMaxSteps bounds scheduler choices, including cooperative actor turns.
// Use the same limit when generating and replaying a trace.
func (s *Scheduler) SetMaxSteps(limit int) error {
	if limit < 1 || s.position > limit {
		return fmt.Errorf("invalid simulation step limit %d", limit)
	}
	if s.replay != nil && s.replay.StepLimit != 0 && s.replay.StepLimit != limit {
		return fmt.Errorf("simulation step limit %d differs from trace %d", limit, s.replay.StepLimit)
	}
	s.maxSteps = limit
	s.trace.StepLimit = limit
	return nil
}

func ReplayScheduler(trace Trace) (*Scheduler, error) {
	if err := trace.validate(); err != nil {
		return nil, err
	}
	s := NewScheduler(trace.Seed)
	s.trace.Version = trace.Version
	s.trace.StepLimit = trace.StepLimit
	if trace.StepLimit > 0 {
		s.maxSteps = trace.StepLimit
	}
	s.replay = &trace
	return s, nil
}

func (s *Scheduler) SetWorkload(name string) error {
	if name == "" || s.trace.Workload != "" && s.trace.Workload != name {
		return fmt.Errorf("invalid simulation workload %q", name)
	}
	if s.replay != nil && s.replay.Workload != name {
		return fmt.Errorf("simulation workload %q differs from trace %q", name, s.replay.Workload)
	}
	s.trace.Workload = name
	return nil
}

func (s *Scheduler) NowMillis() int64 { return s.now }

func (s *Scheduler) AdvanceMillis(delay int64) error {
	if delay < 0 || s.now > math.MaxInt64-delay {
		return fmt.Errorf("invalid virtual delay %dms at %dms", delay, s.now)
	}
	s.now += delay
	return nil
}

func (s *Scheduler) Choose(enabled []string) (string, error) {
	choices := append([]string(nil), enabled...)
	sort.Strings(choices)
	if len(choices) == 0 {
		return "", fmt.Errorf("simulation has no enabled action")
	}
	for i, choice := range choices {
		if choice == "" || i > 0 && choice == choices[i-1] {
			return "", fmt.Errorf("invalid enabled action %q", choice)
		}
	}
	if s.position >= s.maxSteps {
		last := ""
		if len(s.trace.Decisions) != 0 {
			last = s.trace.Decisions[len(s.trace.Decisions)-1].Chosen
		}
		return "", &StepLimitError{Limit: s.maxSteps, AtMillis: s.now, Enabled: choices, LastChosen: last}
	}
	chosen := ""
	if s.replay != nil {
		if s.position >= len(s.replay.Decisions) {
			return "", fmt.Errorf("simulation trace ended before decision %d", s.position)
		}
		want := s.replay.Decisions[s.position]
		if want.AtMillis != s.now || !reflect.DeepEqual(want.Enabled, choices) {
			return "", fmt.Errorf("simulation decision %d diverged: at=%d enabled=%v; trace at=%d enabled=%v", s.position, s.now, choices, want.AtMillis, want.Enabled)
		}
		chosen = want.Chosen
		index := sort.SearchStrings(choices, chosen)
		if index >= len(choices) || choices[index] != chosen {
			return "", fmt.Errorf("simulation trace chose disabled action %q", chosen)
		}
	} else {
		chosen = choices[s.rng.Intn(len(choices))]
	}
	s.trace.Decisions = append(s.trace.Decisions, Decision{AtMillis: s.now, Enabled: choices, Chosen: chosen})
	s.position++
	return chosen, nil
}

func (s *Scheduler) Trace() Trace { return s.trace }

func (s *Scheduler) Finish() error {
	if s.replay != nil && s.position != len(s.replay.Decisions) {
		return fmt.Errorf("simulation trace has %d unused decisions", len(s.replay.Decisions)-s.position)
	}
	if s.replay != nil && !reflect.DeepEqual(s.trace.Transport, s.replay.Transport) {
		return fmt.Errorf("simulation transport trace diverged")
	}
	return nil
}
