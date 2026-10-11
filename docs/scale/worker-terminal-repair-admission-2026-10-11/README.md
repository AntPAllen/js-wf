# Active worker terminal repair admission — 2026-10-11

Worker.execute now admits every retained Completed/Failed outcome with shared
wf.DecodeOutcome before repair. Generation must match the actual invocation.
Successful terminals must have no error or unresolved request; failed terminals
must have an error and no inline/external result. Failed pending requests remain
valid. External successful result resolution and full program equivalence are
separate; this is not a whole terminal dependency audit.

## Controls

TestWorkerTerminalOutcomeBeforeRepair runs actual modeled worker dispatches with
legacy journals and canonical graph journals. It reuses21 existing envelope cases
and adds foreign/zero/absent/null generation, completed error, empty failed error,
failed inline result, valid failure, opaque rejected request and empty successful
result. There are31 cases per backend:62 controls (14 positive/48 negative).

Each negative requires no handler/effect calls, no outcome projection and an
unchanged journal. Each positive requires the exact original terminal bytes in
KV. The shared helper now also checks exact positive projection bytes in prior
terminal-order controls, preventing a failed repair from satisfying the test.
Canonical physical reference consistency remains checked.

Baseline10e22ab plus final fixtures exits1: all48 invalid outcomes are copied
into KV, while all14 valid outcomes pass exact projection checks. Original worker
source, command and semantic log are retained; baseline executable observation
was missed. This is not a compiler failure or a full baseline release campaign.

Final normal/race selections cover all62 new controls plus the prior102 metadata/
epoch/order controls. Exact current worker CI assertions pass against both logs.
Three actual normal/race/native executables are captured, with modeled process
parent PIDs matching Go drivers. All1152 prelaunch Go/module/workflow inputs
remain unchanged. Hosted CI execution is not claimed.

Native R1/R3 child/buffered-child continuation compatibility passes in66.770s
with independent raw SDK auditing: four start/terminal witness pairs and eight
checkpoint frames. These are healthy compatibility flows, not native malformed
outcome injection, retained-fixture restoration, full worker suite qualification
or power-loss proof.

## Review

```sh
python3 docs/scale/worker-terminal-repair-admission-2026-10-11/review.py
```

The corpus remains162 families/922 pins. Five older frozen race/scale campaigns
were revalidated live and predate this code. Separate maintenance paths, complete
terminal/result dependency resolution, full program/clock/promise/lease/protected
reader causality, full current qualification and original fault/scale/soak/power/
client/rollout requirements remain open. Public admission/import/online collection
remain disabled.
