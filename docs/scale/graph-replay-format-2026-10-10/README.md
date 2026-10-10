# Explicit canonical graph replay format

Worker graph snapshots carry `Format: graph-v1` and validate that contract
before returning. CLI exports propagate it as `format: graph-v1`; SDK callers use
`ReplayOptions.Format = wf.ReplayFormatGraphV1`. Export validates that contract
before returning bytes, and CLI replay validates it before plugin loading.
Unknown nonempty formats reject. Empty format preserves previously exported
legacy/unversioned histories and their existing annotation validation.

The declared graph contract requires a canonical queue annotation and a hashed
owned body reference on every consumed signal, exact contiguous queue indices,
increasing source sequences and nonempty tokens. It verifies body bytes before
plugin loading and covers limit-rejected nested consumption. Every completed
checkpoint must carry worker metadata; removing both metadata fields cannot
silently select the legacy path when graph-v1 remains declared. Existing shared
metadata and child validation additionally bind frame/history/provenance.

SDK directed controls check 14 healthy/compatibility/missing-marker/schema/hash/
queue/limit cases. Three CLI controls require rejection before opening a
nonexistent plugin. Native operator fixtures now include owned worker metadata
beside SDK frames, so their exports satisfy the same graph contract rather than
claiming graph-v1 for a partial checkpoint representation.

Qualification follows from a frozen source: full SDK race, all saved traces,
CLI directed/legacy plugin and native R1/R3 domain v4/v5/v6 export checks, plus
all retained native bundles in both unversioned and explicit graph modes.
This format is an input contract, not authentication of arbitrary rewritten
bundles: deleting or rewriting the format declaration itself changes the
caller's requested compatibility contract. Import/rollout, external authenticity,
full native fault/retention/admission matrices, seeded/extended latest-source
campaigns, remaining cap variants, public admission, production collection and
all original broader requirements remain open.

## Frozen qualification live

Source `04f542bcf8ec9099feb07e509db97361c504a633` runs under
`js-wf-replay-format-20261010.service` with `RemainAfterExit=yes`.
[Launch identity](launch.json) records the actual process/invocation. Operational
`state.json` remains uncommitted while running. The retained sparse checkout,
six matching binaries, source inventories, raw event streams and CLI inputs
live under `/home/exedev/js-wf-replay-format-20261010`.

[Reviewer](review.py) requires an actually loaded terminal unit, matching
invocation and actual zero supervisor status; a not-found unit cannot satisfy
the gate. It checks the complete SDK inventory, 14 format controls, existing
62 metadata and 10 selected-child controls, 841 saved traces, CLI pre-plugin and
legacy controls, all six native export cases, eight worker export controls and
two required disabled-format failures. All 76 producer files are replayed in
both formats, including their missing-object/stage controls. Missing-marker
variants preserve both explicit graph rejection and unversioned compatibility.
The expected CLI inventory is 205 invocations. No passing result is inferred
from launch or partial progress.

The [development export command](export-development-receipt.json) passes after
an initial unused-import compile failure, preserved in its separate log. These
development results do not substitute for the frozen review or broader gates.
