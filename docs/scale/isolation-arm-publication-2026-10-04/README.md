# Atomic isolation arm token publication

The local handoff test's original CI timeout is retained in
[the failed CI archive](../ci-failures-2026-10-04/). Its log does not prove the
exact filesystem interleaving. This change independently demonstrates and
closes a real publication hazard in that fixture; it does not attribute the
other mixed-latency or pressure failures to this mechanism.

Previously `armMatrixIsolationTarget` used `os.WriteFile` directly on the marker
read by the worker. That exposes a create/truncate phase before token bytes are
written. A worker can publish readiness with an empty token; once the parent
writes the real token the worker leaves the barrier, and the parent rejects its
stale readiness until selection times out. No NATS operation is involved.

The fixture now writes a staging file, closes it and atomically renames it into
place. The worker sees an absent marker or a complete token. During replacement
it continues seeing the previous complete token until publication. A failed
write preserves that previous marker; selection's existing cleanup remains in
place. Timing, selection, fencing and recovery gates are unchanged.

## Executed deterministic controls

The writer seam uses actual filesystem operations and forces an observation
after open/create/truncate but before writing any token. Both initial publication
and replacement must keep incomplete bytes invisible. A partial-write error must
preserve the previously published token. Existing acquisition handoff and failed
selection/disarm tests also run.

- Fixed normal: `js-wf/integration`, **0.017 s**.
- Fixed race: `js-wf/integration`, **1.037 s**.
- A compiled Go overlay restores direct publication. It fails both visibility
  cases and the partial-write contract, **0.007 s**, with explicit incomplete,
  truncated and partial token observations. This is a semantic negative control,
  not a stochastic repeated race run.

The focused command selects exactly these four top-level tests and uses
`-count=1`; the full captured logs and control source/overlay are retained. A
post-verification inventory records all **609** current Go/module inputs and
proves that only the fixture and its new test differ from baseline `374c8d6`.
That ledger was not captured before execution. Actual compiled executables are
not retained, and no broader binary-provenance claim is made.

`proof.tar.gz` has **10** independently read-back SHA-verified members: fixed
fixture/test source, exact diff, direct-publication control/overlay, all three
logs, scoped verification and source inventory. `manifest.json` records hashes.
When replaying the overlay after relocation, map the current checkout's fixture
path to the restored control file.

This is integration fixture work only. Production runtime and the qualified
121-workload normal100k/race1k simulation graph remain unchanged. No real fault
row, full 200-seed matrix, original failed CI run or 24-hour gate is qualified by
these fast local controls. Existing hosted campaigns and the seed-15 audit
diagnostic continue without restart.
