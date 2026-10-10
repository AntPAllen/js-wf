# Staging pause, intent renewal and checkpoint resumption

`CompactionStage.BeginIntentRenewal` freezes its staging handle while the existing
original-head renewal examines all publication scopes in caller-sized batches.
Staging, overlapping renewal and checkpoint emission are blocked during renewal.
Only full success adopts the new expiry and unfreezes staging. The renewal handle
returns no prepared plan or verification certificate. Successful checkpoints
carry the renewed expiry in the unchanged staging descriptor schema.

Upfront cancellation and invalid budgets preserve the pending operation. An
in-batch error poisons the old stage, retaining the uncertain outcome. Callers
must save the pre-renewal checkpoint and requested expiry before beginning.
Fresh Resume followed by renewal to that exact requested expiry reconciles
matching updates while the old deadline and original head remain valid. There
is no new durable runtime store or persisted renewal certificate here.

## Development evidence

Fourteen controls cover ordinary, pre/post-renewal descriptor resumption,
repeated renewal, empty/completed staging, cancellation/invalid budget,
dropped/lost renewal replies, dropped/lost staging uploads, expiry, collection
and concurrent append. Healthy prefix4 renewal examines42 scopes and renews8;
repeated renewal after prefix6 examines44 and renews10. Completion independently
compares12 records and validates19 nodes. No staging progress or root is advanced
by a partial renewal. Unknown outcomes require fresh handles and the saved input.

Two further controls supply forged record/payload prefixes with valid freshly
renewed grants. Full commit rejects both, preserving the original root. Removing
record/payload comparisons publishes the forged target and fails both controls.
Removing staging/checkpoint freeze guards fails13 controls each; omitting final
expiry adoption fails11. Exact unsafe sources, commands and outputs are retained.

The broader graph-compaction race selection passes with existing and renewed
native R1/R3 restart variants. That run precedes the two renewed-forgery tests;
the final restored16-control race selection includes those tests. Renewed native
variants restart all embedded peers with the saved
one-record staging descriptor, reopen native adapters, examine13 metadata scopes
and renew2 grants in one-scope batches. They write/reload the renewed descriptor
and sweep with a controlled authority clock beyond the old expiry. Source
authority remains unchanged. Staging then completes, independent verification
checks4 records/6 nodes, archive/live counts remain2/2, all surviving bytes match
and11 old objects are reclaimed. The renewal itself occurs after peer restart;
renewed metadata is not subjected to a second peer restart in this fixture.

These are component checks, not process SIGKILL, VM power/storage loss or real
clock-jump qualification. The existing native45-second parent deadline is
unchanged. All853 saved simulation traces pass unchanged.
Run `python3 docs/scale/graph-compaction-stage-renewal-2026-10-10/review.py`
to verify source hashes and scoped development evidence. Older staging/renewal
reviews remain frozen at their commits; changed inputs are qualified here.

## Remaining work

Worker/journal renewal adoption, durable runtime binding of the saved descriptor
and requested expiry, interrupted-renewal recovery and long maintenance request
lifetimes remain required. Renewal still enumerates and retains the complete
namespace key list; no bounded-memory census is claimed. The worker's whole
handoff still has its15-second deadline and fixed intent expiry. Actual100000
and all broader original simulation/native/fault/scale/soak/retention/import/
admission/collector/rollout gates remain open. Public graph continuation admission
stays closed and production collection stays off.
