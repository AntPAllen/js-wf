# Canonical signal mutation phases — 2026-10-11

Signal errors now identify input preparation/commit, ordered binding
preparation/commit, source-frontier update, or binding outcome readback. Client
reservation and binding errors retain their operation context. Native purge/reuse
fixture errors identify fresh versus live-handler release signals, workflow ID,
invocation, and parent-context state. `%w` preserves error classification.

This adds diagnostics; it changes no publication deadline, retry policy,
reservation identity, source ordering, or unknown-outcome recovery rule. The
previous active10 race failure remains unconfirmed: its older executable did not
emit these phases, so it cannot be assigned a phase retrospectively.

## Verification

Eight deterministic component cuts cover reservation and ordered binding:
prepare rejection, commit rejection, committed lost reply with exact readback,
and committed unknown outcome with unavailable readback. They assert the error
phase and `errors.Is` classifications, source/wakeup counts, retained input and
binding identity, recovery using the original token, and reference validity.
Absent reservations cannot be recovered; uncertain reservations do not publish
physical source messages. Exact commit readback succeeds. Recovery of retained
reservations creates one source and one bound wakeup, including already-bound
cases. These are component controls, not new seeded families or regression pins.

`run.py` captures normal/race component tests plus existing aggregate binding
budget, real deadline, and conflict-classification controls, then the entire
946-pin corpus in each mode. `source-inputs.json` binds selected tracked Go,
Python, YAML, module and pin inputs plus the new Go test. Evidence scripts in this
new directory are retained separately. `*-command.json` records actual driver
and SDK exits and hashes of retained executables under
`/home/exedev/js-wf-signal-phases-20261011`. `review.py` independently checks the
source bytes, executable hashes, actual exits and each required PASS name.

`initial-diagnostic.log` predates executable capture and is diagnostic only.
Final qualification is in the four captured command receipts and logs. CI runs
these component controls in both normal and race modes; local review compiles
all workflow Python heredocs. No hosted CI completion is claimed.

Final independent review accepted all four actual SDK exits: 15 client and
947 corpus named PASS results in each mode. It verified 3847 selected source
inputs, all 946 pin names in each mode, four executable hashes and 20 compiled
workflow Python blocks. See `review.json` for the accepted result.

## Scope

All 163 seeded families and 946 pin files remain unchanged. This qualification
does not establish a native server cause or close the full canonical purge/reuse,
latest full seed, fault, or soak requirements. No native fixture was rerun merely
to qualify these error labels. Four older-source long race campaigns were
confirmed live through their user service MainPIDs and actual processes; their
pending results do not qualify this source.
