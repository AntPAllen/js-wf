# First admitted canonical R5 native attempts and follow-up fixes

Both attempts ran source`0b921b7cc26e892538329b79496c0cf0b9e9b305` with
five independent probes, one shared worker/repair provider, and required
pending-positive-Sleep admission. Both are terminal failures; neither clears
native clock acceptance. All67 ahead and34 behind original artifacts/API/full
job files are deterministically compressed and verified against their original
bytes, with SHA256 manifests.

## Ahead: latency components pass; physical drain fails

[Run36960942468](https://github.com/AntPAllen/js-wf/actions/runs/36960942468)
fails153.92s at the unchanged30s physical drain gate. Before that failure,
168 invocations complete with1,851 retained journal entries and144 audited
timer waits. Worst terminal/progress p99 are4.998015171s /23.015785087s.
Current independent component review passes canonical topology, all144 domains,
creation/completion latency provenance, the actual pending-Sleep removal cut
and physical clock-role transitions. Its report explicitly sets
`workflow_accepted=false`; no terminal workflow result is fabricated.

The final backlog has five ordinary`wf.run.54` messages for the already-terminal
`matrixsignal/tier3-1-batch-5-7`:562,569,571,572,578. Its terminal append is
03:43:10.820665472Z..03:43:10.824090573Z. Dispatch records show a local
asynchronous ACK return for562 at03:43:01.398861583Z, and successful local NAK
returns for569/571/572/578 before the cut. Their broker timestamps are around
03:44:01Z; these timestamps are shifted data, not proof of enqueue latency.
Both before/after diagnostics show`WF_P_54` with five ACK-pending, zero pending,
zero redelivered and an unshifted leader. The messages remain readable at the
failed gate. ACK/NAK return is not a confirmed server commit. These observations
do not isolate why the broker retains or delays these messages; no drain timeout
or latency threshold was changed.

## Behind: child-start path remains unfinished

[Run36960953444](https://github.com/AntPAllen/js-wf/actions/runs/36960953444)
fails380.08s: batch5's fan-out Await expires after five minutes. One required
clock cut executes; no final controller audit or acceptance result exists.
The independently observed parent prefix ends at request index7 for
`call_async/child_6`, child`c-099a68419733f4797aed7a56808d6e5b`, observed
03:43:36.770248094Z. Its successful request append returns03:43:36.771380157Z.
No later parent append or fencing event is retained. This locates the stalled
runtime path but does not prove which server request or callback caused it.

Code inspection finds that worker child starts previously passed the entire
delivery context through production client create/read/enqueue retries. The
worker now bounds the whole sequence to five seconds; an uncertain result
returns`wf.ErrChildStart` through the SDK, preserving the durable request for
replay with the same deterministic identity and parent generation. Matching
existing children remain accepted. New`child_start` operation observations are
retained separately in clock fixtures and are not timing anchors for the
controller audit. This fixes a confirmed unbounded path, not a confirmed broker
cause or a proved recovery of this native failure.

## Python audit correction and verification

The Python controller reviewer still accepted only legacy`timer_clock` events.
It now derives canonical deadlines from a unique matching domain-clock creation
interval and`ClockUpper+duration`, while ignoring native delivery hints as
latency evidence. Unknown, absent, malformed, shifted, incorrect and ambiguous
origins are rejected. The original early-return and p99 calculations apply.
The workflow runs controller-reviewer tests as well as Tier3 artifact tests.

The actual retained eight-timer canonical artifact and nine rejection controls
pass, along with the original legacy controls: three Python methods. All40
Tier3 artifact methods pass. Two five-second production client/SDK/worker-port
budget cases preserve the unfinished request, recover one child after healing,
and avoid another start on completed replay:11.016s package. Full worker race
passes41.507s. Native fixtures compile and skip without opt-in. Exact changed
source hashes and logs are retained under`checks/`.

Admitted canonical native recovery, full updated simulator suite, final-source
100k, sustained200-seed clock rows and the24-hour matrix remain open. Further
native attempts must use the changed budget/reviewer source, not rerun these
unchanged failures.

The updated production worker's1,000-seed interrupted fan-out workload also
passes31.99s /33.018s race package. Its log is retained under`checks/`.
A corrected admitted behind35s smoke is queued as
[run36962797494](https://github.com/AntPAllen/js-wf/actions/runs/36962797494)
at source`410272fee5280abc4551f73475d7f33c5e4ea738`, including the whole child
budget, child-start observations and canonical Python reviewer. Both clock
and pending-Sleep inputs are enabled. Its API launch is retained; queued state
is not acceptance. No unchanged ahead drain attempt was relaunched.
