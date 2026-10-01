# Replay a workflow

`wf replay` runs the caller's workflow handler against its retained journal
through `wf.Replay` or `wf.ReplayWithContinuations`. It reads invocation input and referenced `WF_BLOB`
objects. For a completed invocation, it compares the replayed result bytes
with the terminal result. For a failed invocation, it compares the replayed
handler error with the terminal error. For a suspended invocation, it checks
that replay reaches the same recorded wait. Recorded `wf.Run` effects are
read from the journal and are not invoked again. A recorded timer or child
request does not publish or start anything during replay.

The CLI is generic, so it needs the workflow's Go code as a plugin. Build the
plugin and CLI with the same Go toolchain and the same `js-wf` source version.
For a workflow without continuations, export a function with this signature:

```go
func Workflow(c *wf.Context, input json.RawMessage) (json.RawMessage, error)
```

The function should call the same SDK steps, in the same order, as the worker
handler that wrote the journal. Use `-handler-symbol Name` to select another
exported function.

For checkpoint continuations, export a definition containing the same initial
handler and named stages registered on the live worker:

```go
var Workflow = worker.WorkflowDefinition{
    Handler: initial,
    Continuations: map[string]worker.ContinuationHandler{
        "finish_v1": finish,
    },
}
```

`initial` has the signature shown above. Each stage has signature
`func(*wf.Context, json.RawMessage, json.RawMessage) (json.RawMessage, error)`;
its arguments are the original input and checkpoint locals. A factory
`func Workflow() worker.WorkflowDefinition` is also accepted. Replay verifies
checkpoint objects and absolute SDK positions, then dispatches the named stage.
Missing registrations, missing objects and changed declarations fail replay.
Keep old stage names registered while their stored checkpoints can be resumed.

The worker runner accepts a map of these definitions (or a factory returning
that map), selected with `-handler-symbol`. For example, the same plugin can
export `var Workflows = map[string]worker.WorkflowDefinition{"orders": Workflow}`.
Use `wf-worker -handler-plugin workflow.so -handler-symbol Workflows` for
execution and `wf -handler-plugin workflow.so -handler-symbol Workflow replay
orders order-42` for replay. The existing function and handler-map exports
remain supported.

To replay from a live cluster:

```sh
go build -o wf-cli ./cmd/wf
go build -buildmode=plugin -o workflow.so ./path/to/handlerplugin
./wf-cli -url nats://localhost:4222 -handler-plugin ./workflow.so replay math job-1
```

To capture the durable inputs and replay without a NATS connection:

```sh
./wf-cli -url nats://localhost:4222 export-replay math job-1 > replay.json
./wf-cli -handler-plugin ./workflow.so -replay-bundle replay.json replay
```

The bundle contains the invocation input, logical journal, referenced
Object Store bytes, and a retained signal source when a signal drain was
rejected at the journal limit. Offline replay checks the input hash, journal step order,
step and signal object hashes, and the matching terminal result, error, or
wait. The command returns a nonzero exit status if a step differs, an object
is missing or corrupt, or replay reaches a different outcome.

For a cancellation, replay checks the last recorded handler suspension and
the consumed cancellation signal. If cancellation arrived before the first
handler run, it checks the journal and signal without calling the handler.
When the worker reaches the journal entry limit before recording a new step
request, its terminal failure includes that attempted request. Replay checks
that the handler reaches the same request, then stops before running its
effect. `wf.Replay` and `wf.ReplayWithContinuations` perform this request
check directly on the original Failed journal; pass Type, ID and InvSeq in
ReplayOptions. They return `ErrReplayPendingStep` for a rejected effect and
preserve terminal identity validation. The rejected request contributes one
pending SDK entry, without changing the raw journal or its absolute anchors.
A rejected panic attempt or suspension also records its attempted
entry; replay checks the reproduced panic or wait. Older journal-limit
failures without attempted-entry metadata return an explicit verification
error. For a rejected signal drain, replay checks the attempted entry against
the exported `WF_SIG` source, its invocation generation, payload hash, and the
handler's prior suspended state. Export requires that signal source and any
referenced object to remain retained.
