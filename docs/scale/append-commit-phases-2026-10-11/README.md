# Append commit phase diagnostics — 2026-10-11

`Protocol.Commit` now labels failures in context/plan validation, authority
observation and head checking, preservation checks, retained-graph verification,
node/payload/owned-payload grant verification, or root publication.
`AppendCommitError` exposes the phase and preserves its original error through
`Unwrap`. Root publication failures additionally record whether fallback
readback was attempted and its error, when present. The secondary readback error
is diagnostic only and is not unwrapped: the original publication error remains
the classifier used by existing conflict/unknown-outcome handling.

The change adds no transport requests, retries, context checks, deadline changes,
or publication validation rules. Exact witnessed commit readback still succeeds.
An unresolved publication or overtaken candidate remains unresolved under the
existing rules. The new labels are not retroactively attributed to the earlier
native signal failure, whose executable predates them.

## Controls and qualification

Fourteen component controls cover cancelled context, invalid plan, authority
observation, head conflict, invalid preservation, graph read, node and payload
grants, owned payload verification, rejected root publication, committed lost
reply resolved by exact readback, committed unavailable readback, caller
cancellation after commit, and conflict followed by unavailable readback. They
check phase/error identity, root outcome and secondary readback classification.
The last control explicitly verifies that a secondary deadline does not turn an
original definite conflict into a deadline error.

The six existing request-deadline recovery controls are also required. Client
qualification includes eight signal mutation cuts, four dedup-expiry recovery
cuts, aggregate binding budget, actual deadline and conflict classification.
The full 946-pin corpus replays in each mode without pin changes. Existing
simulation source and pin bytes are independently compared against `3db37a3`.
No seed family or pin is added; the inventory remains 163 families/946 pins.

`run.py` captures six actual SDK executions across protocol/client/pin normal
and race modes. `source-inputs.json` binds tracked Go/Python/YAML/module/pin inputs
plus the two new Go files. New evidence scripts are separate. Command receipts
retain source equality, actual exits, timing and executable hashes; executables
remain under `/home/exedev/js-wf-append-commit-phases-20261011`.
`review.py` independently checks these receipts, every required PASS name,
unchanged simulation inputs and the workflow Python blocks. CI runs all 20
protocol cuts in normal and race modes. No hosted CI completion is inferred.

`initial-diagnostic.log` preserves the uncaptured compilation error, two failed
fixture cuts and their correction. The invalid conflict fixture had constructed
an invalid empty root, and the owned-grant fixture compared an encoded authority
key against a plain token suffix. Corrected fixtures use a valid competing root
and actual fence intent membership. These development failures are diagnostic,
not final captured qualification.

Final review accepts all six actual SDK exits. Each mode reports 22 protocol,
22 client and 947 corpus named PASS results (991 total). It verifies 3859 source
inputs, 1169 unchanged simulation inputs, all 946 pin names and 21 compiled
workflow Python heredocs. See `review.json` for the accepted receipt.

## Scope

This qualifies error diagnostics and recovery behavior. It does not establish a
NATS cause for the earlier native binding commit deadline or pass that native
race. No native fixture was rerun merely to qualify these labels. Full latest
seed, canonical scale, fault, soak and adoption requirements remain open. Three
older-source full race campaigns were revalidated live and continue independently.
