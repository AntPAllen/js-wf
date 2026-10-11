# Copied R3 reopen readiness and pending Start recovery

Two fresh copies were verified against the closed full-scale failure store before
opening: 2443 files / 198,307,813 bytes each. No original store files changed.
Both helpers use public NATS/client/journal APIs, with no workers, ordinary Start,
replacement payloads, or automatic retry of uncertain publication.

## Initial readiness observation

The first probe closes actual exit 1 after 9.009 seconds. Immediately after
`testcluster.Start` reports socket readiness, node 0's first authority StreamInfo
request reaches its separate eight-second diagnostic deadline. All eleven
subsequent stream-info requests succeed on the three nodes. The probe stops
before opening the graph store or recovering any Start.

This isolates a copied-cluster startup observation that can stall an immediate
metadata request. It does not prove the earlier unlabelled three-minute helper
failed at the same call, nor explain the original full-scale Start deadline.
Socket readiness does not establish JetStream metadata readiness.

## Read-only metadata recheck and recovery

The second helper, on a separately verified fresh copy, preserves the same early
deadline observation. After the all-node metadata observations, it explicitly
rechecks the four stream-info calls on node 0. These read-only rechecks succeed.
It then opens the graph store and inspects the raw Start catalog:

| Observation | Result |
| --- | --- |
| Already-bound Start cursor descriptors | 984, unchanged after recovery |
| Pending descriptors with original owned input | 43 |
| Pending sources already physically present | 6, same sequence retained |
| Pending sources absent before recovery | 37, exact original tokens bound |
| Successful token-bound recovery calls | 43 |
| Maximum observed recovery-call duration | 63.729 ms |
| Whole helper actual exit / duration | 0 / 11.056 s |

Each recovery is attempted once with the captured original token. Public input
validation checks the owned input and descriptor before publication. Afterwards,
the source, token, hash, pointer bytes, handle and bound cursor agree; pending
journal count/tail remain zero. The helper rechecks the entire application
descriptor bytes of all 984 already-bound roots, without modifying them.

`review.py` independently checks each captured input hash/size, source header,
pointer, physical sequence, handle, zero-history status and descriptor identity.
It also verifies both actual closed processes, retained executable/helper hashes,
1161 exact Go/module source inputs per experiment, original store bytes and both
closed copied-store censuses. The full store/source snapshots/executables remain
under `/home/exedev/js-wf-canonical-start-phase-probe-20261011` and
`/home/exedev/js-wf-canonical-start-ready-probe-20261011`. Initial build commands
returned actual exit 0; build duration/compile-process receipts were not captured.
Executable build information and helper/source bytes are retained. `run.py`
captures the second execution; the first used the equivalent inline wrapper.

These are pending-Start recovery and reopen observations. They do not prove worker
reexecution, complete history integrity, the full 10k purge/reuse gate, a server
cause for the original failure, or storage power-loss behavior. Production retry
rules and the original runtime deadlines are unchanged. No full-scale campaign
was repeated. Next, investigate the instrumented original Start path under its
actual concurrent-worker load.
