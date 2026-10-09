# Bounded canonical continuation delivery

## Implementation

Registered internal graph continuation deliveries now open the owned checkpoint
and load only its anchor/suffix records. Payload references are reconstructed
from completion-owned edges and suffix declarations. The worker retains absolute
journal/SDK indices and reuses the captured checkpoint instead of rereading it.
Initial deliveries without a completed checkpoint still read their history;
unpublished first-pointer recovery can require that initial discovery scan.

The completion now owns a versioned worker metadata object alongside the SDK
frame. It captures child request identities, buffered child signal provenance,
and canonical signal-consumption progress. Promise and buffered child payload
edges are copied into the same append. Metadata has a 16 MiB limit, strict
schema/version/identity/anchor/frame hash checks, and exact owned-edge resolution.
Consumption at the checkpoint is checked against the pin's canonical consumed
count minus suffix consumptions, and its last native queue binding. Buffered
child records additionally match their retained canonical queue bindings and
frame bytes before restoring child selection.

An unreadable/corrupt metadata object fails closed with no prefix fallback.
Failed opens release their pin using an independent cleanup deadline. Missing
worker metadata also fails closed: older experimental/hand-written v5 frames
require a validated metadata migration. Public continuation admission remains
closed. Default ordinary graph deliveries retain their full-history path.

## Executed development evidence

- `final-prefix-race.log`: native R1/R3-domain controls create 131 encoded prefix
  entries before the checkpoint request, then make those bodies unreadable.
  Indexed resume reconstructs one anchor record and three references at absolute
  index 132, including promise bytes and worker metadata. Child request identity
  survives that reconstruction. Actual production delivery execution restores
  state/input/locals, appends at absolute indices, produces result `42`, and a
  separately fenced terminal duplicate does not reenter the handler. Both
  deliveries make zero blocked-prefix reads. Missing/corrupt metadata cannot
  authorize fallback. Existing invalid/unowned/staged/hash/epoch materialization
  controls and explicit old-prefix negative control remain.
- `bounded-native-race.log`: original published and unpublished handoff variants
  pass in both native configurations, retaining actual fenced scanner loop,
  repeated queue loss, exact event identity, internal two-stage large state and
  terminal duplicate/admission controls. Fixtures now publish their completion
  metadata through the production delivery append.
- `sdk-signal-race.log`: production SDK initial/stage and real partition-runner
  controls pass in both configurations. Buffered ordinary signals survive the
  first frame, and a later incoming signal is consumed once across the second
  boundary. Handler/effect counts, result `43`, absolute indices and one terminal
  record remain. The legacy global journal limit control passes too.
- `final-binding-race.log`: buffered signals and both live child promise variants
  pass after canonical consumption/binding validation. An actual async child
  starts before the first boundary; its 700 KB external terminal result is
  awaited either before the next frame or from its buffered signal afterward.
  The child's canonical generation is retired before the final stage. Repeated
  await restores the parent-owned result, with one child execution, one call per
  parent stage, two parent effects, result `43` and one terminal record.
- `child-signal-regressions-race.log`: existing forged child/selected-signal
  provenance controls and native canonical queue replay/5 MiB signal/source
  purge controls pass.

Earlier passing runs remain retained. `initial-race.log` failed only the old
materialization assertion expecting two payload edges; the third is now the
worker metadata edge. `initial-fixture-compile.log` retains a reused test variable
compiler error. `initial-prefix-setup-timeout.log` retains both 30-second setup
expirations while creating the larger native journal, before resume. The fixture
now allows two minutes for construction and verification; this is not a recovery
latency target. `initial-child-budget-assertion.log` retains a one-entry scan
fixture assertion that failed when the child catalog entry preceded the parent;
the child fixture now budgets both destinations.

`executed-review.py` checks exact terminal component passes and hashes inputs
observed at review. These are development controls, not a frozen current full or
extended simulation campaign. Earlier SDK/prefix controls precede the final
signal-binding checks; those checks are exercised in the final buffered/child
run. Shared simulation inventory remains 148 families/748 pins.

## Remaining acceptance scope

No physical prefix is removed here. Archival compaction, collection survival and
retention discovery of materialized pending children still require migration.
The prefix fixture measures records/reference reconstruction, not a scale/latency
or allocator benchmark. Full child permutations, worker/server process-kill,
limits, audit/offline/import/v5 CLI/deployment and public continuation admission
remain unqualified. Every original current-full/extended/runtime/native/scale/
actual24h/physical-drain/adoption/release gate remains required.
