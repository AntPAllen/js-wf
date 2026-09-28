package sim

import (
	"context"
	"fmt"
	"time"

	"js-wf/journal"
)

// AppendActor runs production append logic through a cooperative transport.
// Its function must yield through AppendPort for shared-state operations.
type AppendActor struct {
	Name string
	Run  func(context.Context, journal.AppendPort) error
}

type appendCall struct {
	actor    string
	kind     string
	ctx      context.Context
	subject  string
	data     []byte
	expected uint64
	delay    time.Duration
	reply    chan appendResponse
}

type appendResponse struct {
	tail journal.AppendTail
	seq  uint64
	err  error
}

type actorResult struct {
	name string
	err  error
}

type yieldingAppendPort struct {
	actor string
	calls chan<- appendCall
}

var _ journal.AppendPort = yieldingAppendPort{}

func (p yieldingAppendPort) call(c appendCall) appendResponse {
	c.actor = p.actor
	c.reply = make(chan appendResponse, 1)
	select {
	case p.calls <- c:
	case <-c.ctx.Done():
		return appendResponse{err: c.ctx.Err()}
	}
	select {
	case response := <-c.reply:
		return response
	case <-c.ctx.Done():
		return appendResponse{err: c.ctx.Err()}
	}
}

func (p yieldingAppendPort) Last(ctx context.Context, subject string) (journal.AppendTail, error) {
	response := p.call(appendCall{kind: "last", ctx: ctx, subject: subject})
	return response.tail, response.err
}

func (p yieldingAppendPort) Publish(ctx context.Context, subject string, data []byte, expected uint64) (uint64, error) {
	response := p.call(appendCall{kind: "publish", ctx: ctx, subject: subject, data: append([]byte(nil), data...), expected: expected})
	return response.seq, response.err
}

func (p yieldingAppendPort) Wait(ctx context.Context, delay time.Duration) error {
	return p.call(appendCall{kind: "wait", ctx: ctx, delay: delay}).err
}

// RunAppendActors executes one transport operation at a time. It waits for
// each selected actor to reach its next yield or finish before choosing again,
// so OS goroutine timing cannot change the enabled action set.
func RunAppendActors(ctx context.Context, schedule *Scheduler, transport journal.AppendPort, actors []AppendActor) (map[string]error, error) {
	if schedule == nil || transport == nil || len(actors) == 0 {
		return nil, fmt.Errorf("simulation needs a scheduler, transport and actors")
	}
	seen := map[string]bool{}
	for _, actor := range actors {
		if actor.Name == "" || actor.Run == nil || seen[actor.Name] {
			return nil, fmt.Errorf("invalid or duplicate simulation actor %q", actor.Name)
		}
		seen[actor.Name] = true
	}
	actorCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	calls := make(chan appendCall, len(actors))
	done := make(chan actorResult, len(actors))
	for _, actor := range actors {
		actor := actor
		go func() {
			done <- actorResult{name: actor.Name, err: actor.Run(actorCtx, yieldingAppendPort{actor: actor.Name, calls: calls})}
		}()
	}
	pending := map[string]appendCall{}
	results := map[string]error{}
	collect := func(target string) error {
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case call := <-calls:
				if _, finished := results[call.actor]; finished {
					return fmt.Errorf("finished actor %q yielded again", call.actor)
				}
				if _, exists := pending[call.actor]; exists {
					return fmt.Errorf("actor %q yielded twice without a response", call.actor)
				}
				pending[call.actor] = call
				if target == "" || call.actor == target {
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
		for name, call := range pending {
			action := name + ":" + call.kind
			enabled = append(enabled, action)
			byAction[action] = name
		}
		choice, err := schedule.Choose(enabled)
		if err != nil {
			return nil, err
		}
		name := byAction[choice]
		call := pending[name]
		delete(pending, name)
		var response appendResponse
		switch call.kind {
		case "last":
			response.tail, response.err = transport.Last(call.ctx, call.subject)
		case "publish":
			response.seq, response.err = transport.Publish(call.ctx, call.subject, call.data, call.expected)
		case "wait":
			response.err = transport.Wait(call.ctx, call.delay)
		default:
			return nil, fmt.Errorf("unknown simulated operation %q", call.kind)
		}
		call.reply <- response
		if err := collect(name); err != nil {
			return nil, err
		}
	}
	return results, nil
}
