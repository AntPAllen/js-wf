# Canonical graph checkpoint reader — 2026-10-09

`GraphView.ReadCheckpoint(ctx, type, id)` discovers the latest completed
continuation checkpoint within its pinned generation and returns the verified
frame, runtime anchor, journal suffix and captured tail. The view's caller must
retain/renew the pin while resolving payload references and must release it.
The reader verifies consecutive sequences, monotone epochs, SDK request/completion
pairing and position, checkpoint completion shape, stage, invocation identity,
frame hash/size and declared locals hash. Only an edge owned by the completion
record authorizes the frame read; a matching payload elsewhere is insufficient.

Absent or pending first checkpoints return nil. Corrupt metadata, foreign
identity, unreadable frame bytes and expired pins return errors. A newer append
cannot alter an older pin's selected checkpoint. Full retained history remains
available for audit; this reader does not publish a mutable manifest, compact the
graph, or resolve every payload reference embedded in the materialized frame.

## Executed development controls

```sh
go test -race ./journal -run '^(TestGraphCheckpointOwnedFrameAndSuffix|TestCheckpointRead.*|TestGraphJournalGenerationRetainedReaderAndDrain)$' -count=1 -v
```

`component-race.log` is the initial reader control. `final-race.log` strengthens
ownership and uncertainty: ten cases in each of JSON and protobuf-v1 cover valid,
absent, pending, wrong invocation, mismatched locals, missing ownership edge,
frame owned by another record, exact frame GET failure, expiry and newer
checkpoint selection while an older pin remains stable. Existing checkpoint
admission/read controls and retained-generation reader/drain controls also pass.
Sources were observed at review in the development checkout, not frozen before
compilation. No native continuation worker, compaction, full simulation, extended
seed or release requirement is established by these tests.

This is a prerequisite for the remaining continuation migration. Worker stage
dispatch, graph checkpoint publication/recovery, archive/prefix compaction,
materialized payload ownership through collection, full audit/offline replay and
native kill/uncertainty/limit qualification still need implementation/integration.
Graph worker admission continues to reject continuation registrations. All
original broader runtime/scale/soak/drain/adoption/release gates remain open.
