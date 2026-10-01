# Seeded continuation retirement and quiescent blob collection

The continuation_retirement workload runs production SDK state/promise/Continue
operations, CAS journal append, runtime-checkpoint publication/purge, checkpoint
restoration, retention.PurgeWithPort and quiescent blob mark/sweep. They share one
PurgeTransport store. No copying between disconnected model stores is used.
This workload does not run the worker dispatcher or model Raft/disk behavior;
SDK deliveries are explicitly stopped at frame publication and restoration cuts.

Two invocations hold a content-addressed promise result through their materialized
frames. Unfinished retirement is rejected. Eight retirement modes cross five GC
modes (40 combinations): marker/signal/tombstone/event/invocation uncertain
acknowledgments, dropped journal/snapshot deletion, corrupt/transient frame reads
and dropped/hidden object-delete acknowledgments. GC runs before retirement,
between an uncertain retirement and its retry, after retiring only one invocation,
and after retiring both. Mark failures cause no deletion. The unfinished survivor
retains its archive, frame and shared result; restored promise/state/locals remain
23. Final GC removes owned objects, preserves the unmanaged object, and retained
tombstones and purge events identify each invocation generation. Completed logical
histories pass the raw integrity checker before retirement.

Final-source proof: 100,000 seeds, 200,000 choices and 28,191,051 events in
104.968 seconds, maximum virtual time zero. This is a transport-response fault
workload; it does not claim timing/TTL coverage. All 40 combinations are required.
First ten schedules replay exactly, seed 42 is byte-identical in separate
processes, and its new immutable regression pin is dispatched by the replay
harness. Focused full modeled workload/corpus race passes in 21.738 seconds.

Compiled omission of frame PromiseOutcomes marks fails with only four referenced
objects and deletion of the live shared result. Compiled tombstone generation=1
fails the survivor's expected generation. Package vet/diff checks pass. The
existing independent R3 retirement/reuse race proof passes in 39.635 seconds,
including dropped fresh runtime-manifest publication. It covers real worker/
publication/reuse behavior beyond this model, not identical fault scheduling.

Worker-integrated retirement/reuse, active blob writers/online GC, combined
server/process cuts and full release gates remain open. No production code or
existing model pins change in this slice.

The independent real R3 resolved-promise restart/retirement race contract also
passes in 18.264 seconds: a 614,402-byte child result remains referenced by a
579-byte frame, successor restoration reads zero archive objects, live GC deletes
zero objects, and retired GC deletes three. This is the matching frame-held
promise reference contract; the seed workload uses small result bytes to explore
faults quickly and does not establish spill throughput or large-object limits.
