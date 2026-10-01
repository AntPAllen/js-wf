# Shared lease renewal pressure and isolated response costs

Source hashes identify the final fixture/model and unchanged production runtime.
The originating mixed failure is source 83253a2 seed 4, retained in the sibling
mixed-seed4-83253a2 directory. The pressure failure below is hosted source cebd561.

## Retained mixed evidence

For mixedsignal/mixed-00-0, 52 pre-append renewals take 28.702689 seconds,
including 28.701955 seconds inside KV Update and 0.000088 seconds at the local
revision gate. Journal appends total 0.202343 seconds. The operation totals are
retained as JSON. The delayed node-2 trace contains 3,233 completed delayed
syscalls with five unfinished calls and zero unmatched parsed lines. It includes
495 lease Raft writes (34.797894 seconds summed across threads) and 273 lease
data writes (19.253622 seconds summed). Client update windows overlap those and
many run-consumer groups. These sums/overlaps are not a single RPC's server time
and do not establish cause. During-recovery snapshots show healthy lease leader
node 1 and delayed journal leader node 2; these are snapshots, not per-RPC leader
attribution. The mixed terminal p99 remains a failed 31.049-second gate.

## Controlled real pressure

The existing fixture now adds eight independent owners/keys/journals alongside
one and two owners, for eight rows total. It retains unconditional renewal,
48 calls per owner, three-second call bounds, all synthesized journal identity
and terminal audits, and owner/revision checks. Lease leader is healthy node 0,
journal leader delayed node 1, node 2 stopped. A 70 ms disk injection has physical
DELAYED evidence. No run consumer, signal drain, membership or reconciler runs.

Final race PASS 48.401 seconds. Eight delayed owners spend 7.266–8.576 seconds
each in KV Update; the row lasts 17.211 seconds. Eight undelayed owners spend
44–56 ms each in KV Update. This larger steady workload still does not reproduce
the mixed ~29-second update sum. Third-node catch-up, recovery transients and
consumer-group pressure remain candidates; no production optimization is
promoted from this result.

The latest hosted six-row pressure job failed before any disk injection: the
first append audit exhausted bounded snapshot-manifest reads while KV_WF_STATE
still named stopped node 2 as leader. Its full job log and monitoring/partial
rows are retained. The controlled fixture now pins the audit's KV_WF_STATE
bucket to survivor node 0 before killing node 2, and checks that placement at
row ends. This removes an unintended metadata election from a steady pressure
measurement. It does not fix/explain a server election failure or relax any
runtime/mixed recovery gate. The final eight-row race result above uses this
explicit placement; hosted confirmation remains pending.

## Tier 1 lease-only cost slice

The existing uniform-cost production-worker workload retains its choice sets
and exact pinned traces. A new worker_signal_lease_latency workload reuses the
production worker, lease and journal ports while delaying only KV updates
before commit. Choices are 0/170/560/620 ms. All 16 buffered signals are consumed
once, 50 journal records are retained, 51 required KV updates occur (including
initialization), the result is 120, outcomes are immutable and raw integrity
passes. Heartbeat ticks are quiet to isolate unconditional pre-append renewals;
lease TTL stays alive through individual renewals. Journal and signal reads have
zero injected delay. Exact operation/update/gate accounting is verified.

Expected virtual durations are 0/8,670/28,560/31,620 ms. The 620 ms case is
explicitly a known_over_30s_control. Successful execution of that control means
the checker correctly exposes a liveness miss; it is NOT a clean runtime seed
or release-gate completion. This does not replay the exact mixed invocation or
model why NATS responds slowly.

Final 100,000 schedules PASS 75.377 seconds, 24,700,000 transport events,
maximum virtual time 31,620 ms. Exact first-ten replay and separate-process
seed-42 identity pass. Uniform/new cost workloads plus old corpus pass under
race in 27.839 seconds; the new pin explicitly passes race in 1.043 seconds.
Vet passes. A compiled control reusing a recent lease renewal before append
fails semantically at seed 1 (one update instead of required 51) in 0.005
seconds. The patch/log is retained; no skipped renewal is introduced into
production. The full latest-source simulation and real matrix/soak gates remain
open independently of these contract/cost checks.

## Hosted confirmation at a8064ac

[Run 36826882488](https://github.com/AntPAllen/js-wf/actions/runs/36826882488)
passes the final eight-row pressure job, lease-disk contract and all 20 mixed
seeds. Retained pressure rows show delayed eight-owner KV sums 7.817–9.261
seconds with no owner errors, consistent with the local steady case and still
below the originating mixed ~29-second observation. The snapshot bucket was
pinned to survivor node 0 in this source. Its pass controls that leaf's topology;
it does not explain the prior stopped-metadata-leader failure or establish the
whole-matrix 200-seed gate. Artifacts and full pressure job log are retained.
