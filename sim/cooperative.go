package sim

import (
	"context"
	"fmt"
	"sort"
	"strings"
)

type StallError struct {
	Cause      error
	WaitingFor string
	Pending    []string
	Unfinished []string
}

func (e *StallError) Error() string {
	return fmt.Sprintf("simulation stalled waiting for %q: pending=%v unfinished=%v: %v", e.WaitingFor, e.Pending, e.Unfinished, e.Cause)
}

func (e *StallError) Unwrap() error { return e.Cause }

// YieldFunc makes one shared-state operation available to the scheduler.
// work runs only when that actor's turn is chosen.
type YieldFunc func(context.Context, string, func()) error

type CooperativeActor struct {
	Name string
	Run  func(context.Context, YieldFunc) error
}

type cooperativeTurn struct {
	actor  string
	action string
	work   func()
	reply  chan struct{}
}

// RunCooperative schedules actors only at explicit yield points. It waits for
// the chosen actor to yield again or finish before selecting another turn, so
// the enabled set is independent of OS goroutine arrival order.
func RunCooperative(ctx context.Context, schedule *Scheduler, actors []CooperativeActor) (map[string]error, error) {
	if schedule == nil || len(actors) == 0 {
		return nil, fmt.Errorf("simulation needs a scheduler and actors")
	}
	seen := map[string]bool{}
	for _, actor := range actors {
		if actor.Name == "" || strings.Contains(actor.Name, ":") || actor.Run == nil || seen[actor.Name] {
			return nil, fmt.Errorf("invalid or duplicate simulation actor %q", actor.Name)
		}
		seen[actor.Name] = true
	}
	actorCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	turns := make(chan cooperativeTurn, len(actors))
	done := make(chan actorResult, len(actors))
	for _, actor := range actors {
		actor := actor
		go func() {
			yield := func(_ context.Context, action string, work func()) error {
				if action == "" || strings.Contains(action, ":") || work == nil {
					return fmt.Errorf("invalid simulation action %q", action)
				}
				turn := cooperativeTurn{actor: actor.Name, action: action, work: work, reply: make(chan struct{})}
				// Once an operation is offered, the scheduler decides when it
				// observes cancellation. Let the transport work see its own
				// operation context; racing that context against turn delivery
				// makes an identical trace take a different branch on replay.
				select {
				case turns <- turn:
				case <-actorCtx.Done():
					return actorCtx.Err()
				}
				select {
				case <-turn.reply:
					return nil
				case <-actorCtx.Done():
					return actorCtx.Err()
				}
			}
			done <- actorResult{name: actor.Name, err: actor.Run(actorCtx, yield)}
		}()
	}
	pending := map[string]cooperativeTurn{}
	results := map[string]error{}
	collect := func(target string) error {
		for {
			select {
			case <-ctx.Done():
				pendingActions := make([]string, 0, len(pending))
				for _, turn := range pending {
					pendingActions = append(pendingActions, turn.actor+":"+turn.action)
				}
				sort.Strings(pendingActions)
				unfinished := make([]string, 0, len(actors)-len(results))
				for name := range seen {
					if _, finished := results[name]; !finished {
						unfinished = append(unfinished, name)
					}
				}
				sort.Strings(unfinished)
				return &StallError{Cause: ctx.Err(), WaitingFor: target, Pending: pendingActions, Unfinished: unfinished}
			case turn := <-turns:
				if _, finished := results[turn.actor]; finished {
					return fmt.Errorf("finished actor %q yielded again", turn.actor)
				}
				if _, exists := pending[turn.actor]; exists {
					return fmt.Errorf("actor %q yielded twice without a response", turn.actor)
				}
				pending[turn.actor] = turn
				if target == "" || turn.actor == target {
					return nil
				}
			case result := <-done:
				if _, exists := results[result.name]; exists {
					return fmt.Errorf("actor %q finished twice", result.name)
				}
				results[result.name] = result.err
				if target == "" || result.name == target {
					return nil
				}
			}
		}
	}
	for len(results)+len(pending) < len(actors) {
		if err := collect(""); err != nil {
			return nil, err
		}
	}
	for len(results) < len(actors) {
		enabled := make([]string, 0, len(pending))
		byAction := map[string]string{}
		for name, turn := range pending {
			action := name + ":" + turn.action
			enabled = append(enabled, action)
			byAction[action] = name
		}
		choice, err := schedule.Choose(enabled)
		if err != nil {
			return nil, err
		}
		name := byAction[choice]
		turn := pending[name]
		delete(pending, name)
		turn.work()
		close(turn.reply)
		if err := collect(name); err != nil {
			return nil, err
		}
	}
	return results, nil
}
