# Replay a workflow

`wf replay` runs the caller's workflow handler against its retained journal
through `wf.Replay`. It reads invocation input and referenced `WF_BLOB`
objects. For a completed invocation, it compares the replayed result bytes
with the terminal result. For a failed invocation, it compares the replayed
handler error with the terminal error. For a suspended invocation, it checks
that replay reaches the same recorded wait. Recorded `wf.Run` effects are
read from the journal and are not invoked again. A recorded timer or child
request does not publish or start anything during replay.

The CLI is generic, so it needs the workflow's Go code as a plugin. Build the
plugin and CLI with the same Go toolchain and the same `js-wf` source version.
The plugin must export a function with this signature:

```go
func Workflow(c *wf.Context, input json.RawMessage) (json.RawMessage, error)
```

The function should call the same SDK steps, in the same order, as the worker
handler that wrote the journal. Use `-handler-symbol Name` to select another
exported function.

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
