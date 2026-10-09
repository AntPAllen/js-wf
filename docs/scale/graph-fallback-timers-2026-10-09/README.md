# Canonical graph fallback timers — 2026-10-09

Graph workers now admit explicit `-timer-backend fallback`. They retain the
canonical Start/Signal/terminal, timer and suspended repair loops, and select a
graph-aware fallback scanner instead of the legacy state-based scanner. Auto
selection remains rejected, and legacy retention/continuation admission remains
unchanged. A fully upgraded cluster is still required for native schedules.

The fallback decision reads canonical Start status before consulting a timer
hint. Retired, purging, terminal or superseded generations allow disposal of the
physical hint. Active generations defer inspection during delivery leases, then
validate the native invocation against a pinned canonical view. The timer's SDK
step, kind, deadline and clock domain must match the owned journal declaration.
No WF_STATE retirement marker, compatibility journal or input body supplies
authority. Pins are released with independent bounded cleanup; unknown reads or
release failures cannot authorize publishing or deletion.

Due timers publish a generation/step-tagged wakeup before deleting the hint.
The existing deduplication, partial cursor and publication-observation behavior
remains. Errors retain the uncertain hint; dry runs publish/delete nothing.
Clock domains continue to select their configured server clock. Worker timer
replay remains responsible for duplicate/cancelled wakeups.

## Executed component evidence

- `initial-normal.log`: native R1/R3-domain fallback worker CLI fixtures and
  admission controls pass. The fixture retains canonical reserved Start/Signal
  recovery, timer completion, repeated terminal restoration, journal stability,
  default/domain routing and joined shutdown assertions.
- `initial-model-import-cycle.log`: retained initial test compilation failure;
  the model fixture now uses external package `reconcile_test`.
- `final-race.log`: legacy fallback publication/deletion/retirement controls,
  eight modeled canonical decisions and native/fallback worker CLI fixtures pass
  under the race detector. Both timer backends exercise R1 and R3 domain. The
  shared fixture's diagnostic wording says "native timer completion" for both;
  the fallback wrapper explicitly passes `fallback` to provisioning and CLI.
- `final-scanner-race.log`: nine model decisions (including authority-read
  uncertainty), dry-run checks, existing partial-cursor fault cases, and a direct
  native R1 scanner test pass under the race detector. The direct scanner proves
  actual due publication/deletion despite forged legacy purge/state bytes, then
  terminal hint retirement from canonical completion. CLI timer completion alone
  would not establish which competing repair loop supplied its wakeup.

The direct native fixture is R1; the worker integration also covers R3 domain.
The model uses a seeded in-memory graph/source transport, with faults at the
lease/source/authority/publication/deletion boundaries. This is a component
control, not a new full tier-1 family or extended seed campaign. Production
GC, tombstone deployment, continuation/snapshot/import, lifecycle projection
fencing, full/extended qualification, original native/scale/actual24h/drain,
default adoption and release requirements remain open. The live frozen full
race at `9a1ccdc` excludes this migration.
