# Retirement/reuse across fresh-manifest worker SIGKILL

Clean4ab4bdd actual race SDKPASS35.39s. Original30s startup/60s scenario and
strict kill-to-result under30s remain; recovery12.650599017s with production
TTL12s (heartbeat3s/AckWait13s defaults unchanged). This adds a new combined case,
not another run of the historicalTTL30s/31.1s mismatch.

Two workflows finish generation1/survivor2. Stop and close the original worker,
retire generation1, quiescent-collect exactly two retired frame/archive objects,
and verify shared content plus survivor references retained. Reuse as generation3;
reinjected old manifest must fail generation validation before collected object
access. The child worker publishes the fresh generation frame/manifest, then
blocks before journal purge/handoff. Parent confirms that manifest and live lease
owner/revision/epoch51, actual child SDK bytes matching the retained parent race
binary, then sends and reaps actualSIGKILL before starting the successor.

Successor returns2 in12.65s with terminal epoch62 above51, zero archive reads and
one frame read. Exact fsynced ledger: old initial/effect/stage twice, fresh initial/
effect/stage once, three effects overall and two raw-integrity terminals. All
three pinned peers return fresh2, survivor stays1. After closing the successor,
quiescent collection preserves shared bytes, survivor and fresh frames. No active
writers are present during either sweep; no online-GC claim.

Independent review verifies actual live parent/child SDK hashes and retained
clean VCS/fullbuild/race fields,651 selected repository Go/module inputs against
Git and unchanged before/after ledgers. All three actual native server process
executable hashes/build fields match their retained binary. Kill/fence and exact
result/effect/cursor counts are checked against raw artifacts and original test
assertions. Stop/reap SIGKILL has actual WaitStatus assertion scope; no separate
kernel event log claim. Complete1,084-member archive/two split parts read back.
Stopped originals remain /tmp/js-wf-retirement-worker-sigkill-race-20261005;
no independent store reopen or exhaustive toolchain/external/assembly inventory.

Focused fresh-manifest retirement/reuse worker SIGKILL accepted at executed
source. Other retirement publication timings, combined server outage, domain/
legacy faults, active-writer GC, full current-source matrices and actual24h stay
open. No production runtime/state-machine change or unchanged Tier1 graph rerun.
Existingb2d7011 journal24h continues on its original live handle; native race/build
shares its~one-hour region on the VM, not a stable latency/overhead experiment.
