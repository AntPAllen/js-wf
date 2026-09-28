# Replay a completed workflow

`wf replay` runs the caller's workflow handler against its retained journal
through `wf.Replay`. It reads invocation input and referenced `WF_BLOB`
objects, then checks that the replayed result bytes equal the terminal result.
Recorded `wf.Run` effects are read from the journal and are not invoked again.
The command currently accepts completed invocations; failed and suspended
invocations are not yet supported.

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

The bundle contains the invocation input, logical journal, and referenced
Object Store bytes. Offline replay checks the input hash, journal step order,
step and signal object hashes, and terminal result. The command returns a
nonzero exit status if a step differs, an object is missing or corrupt, or the
replayed result differs from the committed result.
