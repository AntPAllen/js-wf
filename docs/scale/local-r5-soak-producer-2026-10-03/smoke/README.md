# Local R5 long-run producer: qualified journal smoke

Exact65f4b7f387917b72d3e7f18b76419b95f6a1b822 ran journal seed1/35s from
an isolated sparse checkout and retained race executable. Named test99.54s,
package100.569s,196 terminal invocations,2156 entries and one journal-leader cut.
The producer's strict row/history/audit/drain/explanation/fencing checks pass.

The unaltered original archive includes its compiled checkout, executable/build
settings, before/after hashes, actual command/state/environment, raw Go JSON,
all five physical stores and reports. Every copied member was reopened and
hashed before atomic rename. Independent review verifies4223 members,999 source
hashes against Git and actual executable race settings, then regenerates all
three reports identically from recorded scripts.

A relocated positive passes. Five consistently hashed negative controls reject
failed status, missing compiled source, a false normal-binary claim, substituted
seed and too-short named-test elapsed. Originals remain unchanged. Physical
stores are hash-verified; this review does not independently reopen them.

Reproduce from the repository root:

```sh
python3 scripts/review-tier3-soak.py --root docs/scale/local-r5-soak-producer-2026-10-03/smoke --output /tmp/local-soak-review.json
python3 docs/scale/local-r5-soak-producer-2026-10-03/smoke/check-portable-controls.py --root docs/scale/local-r5-soak-producer-2026-10-03/smoke --repo /home/exedev/js-wf --reviewer scripts/review-tier3-soak.py --output /tmp/local-soak-controls.json
```

This qualifies only the journal smoke path. Other producer profiles, actual24h
rows, complete matrices and the full release remain open. Starting a new row:

```sh
python3 scripts/run-tier3-soak.py --root /external/fresh-root --row journal
```

Default duration is24h with24h20m Go deadline. The execution metadata records the
live child PID; an interrupted live child retains its original root without a
final-archive claim. Do not restart solely because an observation expires.
