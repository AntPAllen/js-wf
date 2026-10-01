# Worker-held resolved promises through retirement and GC

## Shared stores and production decisions

The new `worker_promise_retirement` Tier 1 workload extends the existing shared
worker-retirement port with production client Signal and child-result object
operations. Start, Signal, parent/child worker dispatch, journal, leases,
checkpoint publication/restoration, terminal state, retirement and quiescent
GC share physical retained stores. Run delivery uses WorkerTransport.

A real child handler returns 614,402 bytes, above the production 600 KiB terminal
inline threshold. The parent resolves its promise and captures it in a named
continuation frame; the frame contains the result reference/hash, not the large
inline result. A replacement waits for an actual client Signal, restores the
promise, checks repeated reads are detached, and completes without archive
reads or re-entering the initial handler. The terminal raw-state audit requires
two invocations and outcomes, one child declaration, one child signal consumption
and the expected original call/await requests.

Eight modes cover clean execution, dropped/lost-reply child notification,
dropped/lost-reply terminal object publication, one unavailable restored-result
read, lost child invocation-purge reply and dropped parent journal purge.
Uncertain terminal object writes may repeat the child handler; the fixture
requires one recorded outcome and deduplicated notification, not once-only
arbitrary effects. No old model pins or production thresholds are changed.

Child retirement must reject an unfinished parent. With the parent terminal,
child retirement may complete while its retained parent frame still references
the child result. GC between uncertain retirement and retry preserves all
three referenced objects. Offline ReplayWithContinuations audits the exported
complete logical parent history, including initial declarations once and its
named stage, using retained frame/result objects without executing child code.
Final parent retirement reclaims all three objects. Both tombstones must match
the retired generations; invocation and snapshot/purging metadata must be gone.

## Verification and controls

- Final checker source: 1,000 schedules pass in 14.007 seconds, including exact
  first-ten replay and seed-42 byte identity in separate processes.
- Real R3 resolved-promise restart/retirement race passes in 19.125 seconds:
  614,402 child bytes, 579-byte frame, zero replacement archive reads, one
  child result read and three reclaimed objects. This is a matching integration
  contract, not byte-identical differential replay of every fault mode.
- Vet passes. Final-source complete regression corpus and new workload race
  pass in 95.086 seconds, including replay of the new pin.
- Compiled frame-promise marking omission fails with Referenced=2/Deleted=1
  instead of preserving three objects (0.023 seconds).
- Compiled restored-promise metadata omission fails the child-result-read
  checker (0.012 seconds).
- Compiled signal dedup omission fails with two child signal consumptions
  (0.043 seconds). Patches and semantic failure logs are retained. No control
  relies on a build error, skip or timeout.

The source preceding the final signal-consumption count assertions passed
100,000 schedules in 1,154.847 seconds (36,961,056 transport events, maximum
virtual time 1,000 ms). This does not establish those later assertions at the
release seed count. The final-checker 100,000-seed campaign passes in 1,164.872 seconds with
36,961,056 transport events and maximum virtual time 1,000 ms. Its exact and
cross-process checks are included; the stored source manifest matches current
model/runtime files. The final-checker log is retained here. Response faults and
up to one second of virtual retry time do not prove lease expiry, process
partitions or sustained liveness.

## Independent hosted fixture failure

The db043bc standard workflow failed its first four-fault seed before injecting
faults: Stream.Info returned an empty elected leader after stream creation.
The combined fixture now waits at most five seconds for valid election metadata
before choosing fault targets. Invalid nonempty fixture names still fail,
individual lookups are bounded, and a missing election times out. Deterministic
absent/empty-to-elected, invalid-name and deadline checks pass; three actual
four-fault repeats pass in 10.011 seconds total. This changes startup readiness,
not fault duration, invariant or recovery gates. Hosted confirmation is pending.
The independent db043bc mixed campaign passed all 20 seeds and pressure/disk jobs.

Remaining scope: combined promise worker/server SIGKILL cuts, other transport
edges, coordinated online GC, TTL timing, final-source full Tier 1, whole-matrix
200-seed Tier 2 and five-node 24-hour Tier 3 acceptance remain open.
