# Byte-bounded audit refill and iterator tracing

## Reproduced client defect

The pinned nats.go1.54.0 `PullMaxBytes` + `StopAfter` combination stalls healthy
refill. A plain/unwrapped8MiB iterator delivers31 of48 quarter-MiB records then
returns ErrNoHeartbeat. The production adapter regression likewise fails with
31/48 and the preserved timeout/heartbeat error (6.68s including cluster setup).
The SDK resets pending message count to its uncapped byte-mode batch maximum,
although StopAfter caps the actual request. Refill then subtracts this overstated
pending count from the remaining StopAfter allowance and cannot request more.
The inspected pull.go/options source matches the cached official1.54.0 module ZIP;
review-time copies/hashes are retained. Full external dependency closure is not
claimed from these two files or a build-info module version.

This explains this focused healthy transport failure. It does NOT establish the
cause of checkpoint1750, earlier soak/matrix failures or any NATS server defect.

## Implementation

The byte scanner uses one continuous8MiB iterator per replicated audit consumer.
Its adapter caps each delivery batch at4096 records. It retains prefetch between
record windows and stops the iterator when the scan ends or the cursor changes.
SDK StopAfter is removed from byte delivery; stream bounds, leader gap checks,
audit20s attempt/60s total, two-resume budget and all workflow gates are unchanged.
Canceling a finished batch cannot close the shared iterator; active cancellation
still stops blocked delivery.

A continuous AckNone iterator can replay older sequences after a consumer-leader
move. Backward delivery only admits cursor recovery after a current Info reply
confirms the SAME name/stream, replicated memory/AckNone configuration and a
nonempty leader different from the captured initial leader. Recreate at the
first unaccepted sequence within the original resume budget. Unconfirmed order,
wrong stream and semantic visitor errors remain fatal. Ordinary Fetch scans
retain their original order policy.

Opt-in retained-audit tracing now delegates Messages/Next and records call and
payload-byte totals, latest success/completion times, outstanding call counts
and wait times. Slow>=10ms/error calls enter the existing64-call recent ring;
fast calls only update aggregates. Consumer IDs identify slow iterator calls.
Opaque SDK options are forwarded unchanged; trace records invent no deadline.
Stop/Drain delegate to the original iterator. Next wait excludes later audit
processing, so delivered bytes now distinguish slow progress from no delivery.
The elapsed contribution from tracing has not been qualified at the100k/R5 gate.

## Executed controls

- Baseline private healthy-refill regression: FAIL31/48; original SDK/source/
 native stores retained. Its post-source ledger was not captured before editing;
 only the captured pre-build selected inputs and actual binary/process establish
 this baseline's source scope. The regression body is unchanged in corrected tests.
- Final normal/race:48 exact quarter-MiB records cross8MiB with no recovery error;
 4100 exact4KiB records cross byte and4096-record windows, one consumer, zero
 leader-gap reads and zero consumers observed within2s after cleanup.
- Existing116-record30MiB holes/deleted-cutoff oracle and cancellation pass in
 both builds. Record caps, blocked cancellation, semantic errors, heartbeat
 recovery and cross-window prefetch unit controls pass.
- Both state modes retain1500 invocations/6000entries after admitted real
 consumer-leader kills.4,039 records remain pending at each kill;8MiB prefetch
 cannot consume the whole admitted fixture. Final normal fault audit elapsed
 .591/.418s; race3.797/2.663s, within original20s. Cancellation at128 visits passes.
 Requested-step fixture payload is16KiB to admit a real pending tail; counts,
 audit invariants and fault/context limits remain unchanged.
- Same/unknown leader, wrong name/stream/configuration and API errors do not
 admit replay recovery. A unit oracle verifies confirmed recovery resumes without
 duplicate visits while unconfirmed order stays fatal.
- Trace native/plain comparison and real NextContext deadline/cancel controls
 pass normal3.75s/race5.68s,48records/12MiB/50Next calls/two context errors.
 Its captured working-tree snapshot predates the final replay guard; tracing
 files match the final implementation, while linked runtime source differs.
- Dropping Next options through a retained Go overlay fails the delegation
 oracle as intended. Mutant binary retained; no live mutant-process identity.

## Preserved intermediate failures

failed-setup: stream creation times out before iterator assertions; fixture then
uses existing bounded readiness/provisioning. failed-small-window: artificial1KiB
iterator stalls at7 records. failed-unwrapped-refill: production8MiB plain SDK
stalls at31. first-continuous: healthy data passes, race immediate StreamInfo
still reports one consumer after cleanup; final test observes convergence within
2s without repair. Its additional fault run retains one pre-admission pending0
failure and an actual leader-kill sequence replay failure before the guard.
These failed parent/package verdicts remain preserved and are not promoted.

## Evidence and limits

Every proof-index archive member and split part is read back/hash-verified. All
original fixture roots and canonical proofs remain under /tmp at the recorded
producer paths. Actual normal/race SDK executable hashes, live process identity,
full build info and647 selected Go/module inputs are captured. Final integrity
and trace campaigns have unchanged before/after selected-source inventories;
final integrity inputs also match current code. Embedded actual NATS2.15 servers
are linked into the SDK test process; no separate server-executable claim.

No fixture was reopened or repaired. Full/final-source matrices, legacy/domain
compatibility,100k/R5 performance, actual24h and million physical drain remain
open. Unchanged Tier1 modeled runtime bodies are not rerun for this native-reader
and observational change. The existing million campaign shares the VM; its
historical/live verdict is separate. No new native soak has been launched.

## Sparse worktree restoration

Headroom directories record manifest+canonical pushed-Git+Git-blob checks before
omitting archive working copies. Original failed/live fixtures remain intact.
Restore selected proof parts before extraction or raw replay, for example:

    git restore --ignore-skip-worktree-bits --source=HEAD -- 'docs/scale/byte-bounded-audit-2026-10-05/24h-failed-1750/proof.tar.gz.part-*'

Verify restored bytes against that directory's archive-verification.json.
`git sparse-checkout reapply` can omit these working copies again.
