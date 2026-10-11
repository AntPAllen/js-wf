# Raw worker checkpoint metadata and native SDK audit — 2026-10-11

The independent raw journal audit now validates optional worker checkpoint
annotations: complete canonical metadata pointer,owned bounded bytes,version,
workflow/generation/anchor/frame hash,child declarations from the journal prefix,
and exact consumed-signal count/high-water at the checkpoint. Buffered child
provenance must match the recorded prefix event,the pending SDK frame signal,
the child declaration and owned payload/result edges. Omitted buffered child
provenance is rejected. Production metadata restore/replay helpers are not called.
Unannotated journal-only checkpoints retain their existing valid format.

## Focused race evidence

```sh
WF_GRAPH_SDK_RAW_AUDIT=1 go test -race ./worker ./integrity \
  -run '^(TestNativeGraphContinuationSDKFlow|TestNativeGraphContinuationChildPromise|TestNativeGraphContinuationBufferedChildPromise|TestRawGraphCheckpointMetadataBindings|TestRawGraphCheckpointMaterializedState|TestRawGraphCheckpointPointerAndFrameBindings|TestRawGraphCheckpointPromiseOwnership|TestRawGraphJournalGenerationsOutcomesAndArchive)$' \
  -count=1 -v -timeout=8m
```

Actual corrected combined exit0:worker83.780s,integrity1.389s. Six native SDK cases
pass(R1/R3Domain × ordinary continuation/selected child/buffered child),each with
a raw audit after production worker closure. These are actual SDK-generated
frames and worker annotations,not manually fabricated native frame fixtures.
The live corrected worker binary receipt records PID209915,SHA256
51afb166cc0ba785f2a8e8800b27ec3f6c2efd0fa3fa30c1efc083e5c1d48b98 and-race=true.
Named development input hashes captured during the run recheck unchanged.
This is focused development evidence,not complete clean-source qualification.

Two JSON/protobuf metadata positives and ten reference-valid semantic corruptions
pass. A separate retired-source race(1.024s,actual exit0) passes one positive and
seven corruptions. The complete new native SDK CI guard was executed against the
actual log; only the new metadata/retirement subset of the separate integrity CI
guard was verified here. Existing whole reference suite is not rerun by this
command. CI requires all cases and six native receipts,including four retired
projection-only counts.

## Retired-source scope and preserved failures

The first combined command exited1(worker77.359s):ordinary SDK cases passed,but
four child cases exposed a checker assumption that every retired root must be
purging and have no retained invocation. The fixture legitimately calls terminal
GraphStore.Retire without purging WF_INV/WF_STATE. initial-race.log,initial source
hashes and initial binary receipt preserve that failure. The checker now validates
this distinct API state:empty live forests,terminal cursor,matching source/Start
identity and retained result generation/kind. It reports RetiredProjectionOnly
separately; it does not count such a child as a fully audited journal/terminal.
Exact terminal bytes cannot be compared with an already removed live journal.
Four native child cases therefore report one parent full journal/terminal and
one separately checked retired child projection. Purging roots with retained
sources,wrong generations/results and remaining live forests still fail.

The first pure retired positive fixture used a nil frontier,which violates the
canonical empty array representation. initial-retired-race.log preserves that
fixture failure; an empty array corrected it without another implementation
change. All eight corrected cases pass.

## Remaining scope

Canonical signal queue/input/index contents and raw child source history are
not fully audited here. SDK-history reconstruction of materialized state,
latest checkpoint discovery,protected-reader application replay,archived SDK
kill boundaries,orphan projections,client linearizability and full physical
scale/disk acceptance remain open. This adds no production worker behavior,
public continuation admission,import or online collection enablement.
Frozen00eeb13 complete159 normal and9189259 classified original100000 normal
continue under their existing invocations; these changes do not restart them.
