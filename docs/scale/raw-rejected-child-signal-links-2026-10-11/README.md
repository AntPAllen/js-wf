# Rejected child signal linkage — 2026-10-11

A failed terminal outcome can retain a rejected SignalConsumed operation inside
limit_entry. The independent raw auditor now applies the existing child-call,
owned-body/result, matching-generation start and retained terminal projection
checks to that nested operation before releasing the journal prefix.

Canonical rejected signals must match the next unconsumed queue binding, including
index/token/name/source sequence/body hash. Queue checks occur after the complete
forest census. Rejected arrivals are separate from consumed arrivals: they do not
advance signal_events, the consumed cursor count, signal FIFO or SDK promise/cache
state. Link counters count available witnesses of either retained operation.

## Regression

Baseline61fc95b plus the retained physical fixture accepted a rejected child
annotation for generation3 carrying an outcome for generation4. The command
exited1 because the regression required rejection. Baseline source, exact fixture,
command and log are retained; the baseline executable was not captured.

The new fixture uses real GraphStore writes on the in-memory transport and
independent physical reference checks before semantic auditing. JSON/protobuf ×16
controls =32 (12 positive/20 negative): inline/failed/external child outcomes,
with or without retained child witnesses, are valid. Missing call/annotation,
wrong child ID/outcome generation/input/start link/terminal bytes and wrong next
binding index/token/name/source sequence are rejected. Positive cases assert
witness counts, four parent entries, one terminal and zero cursor consumption.

These are compact retained-boundary fixtures; the configured maximum is not
exhausted. Actual capacity exhaustion, deployment configuration agreement and
original journal-cap proof remain separate gates.

## Verification

run.py records the broad race command and actual exit. native-run.py runs four
R1/R3 child/buffered-child flows with raw auditing as compatibility controls;
those native flows do not exercise actual limit rejection. Both final test
executables are captured live with hash, build metadata and command. Native
stores are ephemeral; no restoration or power durability is claimed.

source-inputs.json is captured before both final drivers. review.py checks its
unchanged hashes, actual exits, every exact new CI guard and four native start/
terminal witness receipts plus eight checkpoint frames. YAML parsing and local
workflow guards do not establish hosted CI acceptance.

Run from the repository root:

    python3 docs/scale/raw-rejected-child-signal-links-2026-10-11/review.py

Original publication/parent-source/lease causality, unavailable old generations,
retired original journal ownership, current complete161/894 qualification and
original scale/fault/soak/power gates remain open. Four frozen long campaigns
predate this code.
