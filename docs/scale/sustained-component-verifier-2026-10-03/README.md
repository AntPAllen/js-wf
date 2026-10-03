# Strict sustained component-set verifier

`scripts/check-sustained-mutation-components.py` supports a targeted harness-only
retry without rerunning unrelated successful components. All six actual category
jobs must be uniquely identified and successful. Every component's complete
source-bound raw review is repeated. All tracked Go/fixture files, module inputs,
workflow bytes and selected mutation definitions must match an exact reference
revision. Only verification/documentation differences can be combined.

The existing whole-campaign checker remains strict. Component qualification
never promotes failed parent campaigns or clears independent matrix/24-hour
gates. Smoke duration, missing/duplicate categories, live/failed/skipped jobs,
Go/fixture/dependency/workflow/mutation drift and raw-review failures are rejected.
Fresh output paths prevent stale qualification files from being reused.

All 17 sustained harness/reviewer tests pass in 0.581 seconds. Component-set
positive controls use synthetic source/metadata and mocked raw reviews; they
are verifier tests, not workload qualification.

A separate real-input CLI control reviews the five accepted current-source
components, then rejects the original failed Start job. The command exits
nonzero and creates no qualification report. Its exact manifest, executed
checker, console output and rejected verdict are retained in a five-member
archive; every member was reopened and SHA-verified before atomic rename.
The raw component originals remain in their separately preserved archives.
This control qualifies no six-component set. The corrected Start-only retry
must finish and independently pass before that gate can clear.
