# Contradictory replay worker epochs — 2026-10-10

Shared replay admission now applies the integrity checker's I2 ownership rule:
two distinct nonempty worker IDs on one nonzero epoch are corrupt. A missing ID
does not erase a previously observed owner. Higher epochs may change owner;
legacy missing IDs and zero epochs retain compatibility. This rejects declared
contradictions before ordinary handler code, CLI plugin loading and graph snapshot
export. It cannot establish worker ownership where old records omit evidence.

Eight SDK controls pass and independently agree with integrity.CheckSnapshot.
Two CLI format controls reject the conflict before plugin loading. Full SDK race
passes. Disabling only the ownership predicate must fail all four rejecting
leaves, with [actual exit1](negative.log). [Development receipt](development-receipt.json)
preserves scope and source hashes.

Frozen [runner](run.py) includes all prior order/input/format/envelope/metadata/
child controls, full SDK race (67 top tests), 848 normal saved traces, six native
exports, eight worker exports, 37 required mutant failures and 209 offline CLI
invocations. [Reviewer](review.py) requires retained terminal supervisor exit0,
exact source/binary identities and every control. Qualification remains pending.
Full original/latest157 seeded/extended, faults, retention, import, public
admission, production collection and rollout gates remain open.

Frozen source `b9380dbd03799b9af53b6cf8dcc0781333e069c7` is confirmed live under `js-wf-replay-epoch-worker-20261010.service`.
[Launch identity](launch.json) records its process and retained invocation.

## Frozen epoch ownership qualification accepted

[Independent review](review.json) accepts source `b9380db`, 67 full SDK race
tests, 848 normal saved traces, eight SDK ownership and two CLI ownership
controls, all earlier replay controls, six native exports, eight worker exports,
37 required mutant failures and 209 actual offline CLI invocations. It verifies
exact Git inputs, all six binary identities and actual retained supervisor exit0.
[Retained proof](qualified/state.json) preserves the complete command matrix.
This rejects declared ownership contradictions; full original/latest157/fault/
retention/import/admission/rollout acceptance remains open.
