# R5 server-clock peer skew with journal-leader kills

Clean-source35-second fixture smokes are launched at `a1e9387029a554eddcd5d52fed0545ef774aa714`:

- [Ahead36935066881](https://github.com/AntPAllen/js-wf/actions/runs/36935066881): actual server4 at+60s.
- [Behind36935069706](https://github.com/AntPAllen/js-wf/actions/runs/36935069706): actual server4 at−60s.

Each requires all five actual clocks at startup and before/after one confirmed journal-leader SIGKILL/restart, all six mixed workload cells, histories,invariants,independent controller conservative p99,all timer-clock origins and physical WF_RUN/64-consumer drain. The row guard and written fencing/repair/timeline reviewers must also pass. Original inputs/source are retained here. A queued/running handle is not verified acceptance, and35-second smoke never clears sustained/200-seed/24-hour release coverage.

## Strengthened clock-role smoke launches

- [Ahead36937359624](https://github.com/AntPAllen/js-wf/actions/runs/36937359624).
- [Behind36937362297](https://github.com/AntPAllen/js-wf/actions/runs/36937362297).

Both launch at `a12efce` with35-second inputs. They require skewed server4 to own both WF_RUN and WF_JRN before a cut,actual shifted timer-clock lookup evidence,and confirmed replacement leaders on unshifted peers before restart. All existing clock/controller/history/invariant/p99/drain gates remain. The original peer-only proofs cannot certify these stronger requirements. In-flight timer/effect/continuation combinations and sustained/full-matrix coverage remain open.

## Stronger smoke failures and observation correction

Both runs finished with failure. Ahead failed at96.02 seconds and behind at94.48
seconds. Both retained initial and pre-cut WF_RUN/WF_JRN leader observations on
the shifted peer4, then stopped during replacement observation, roughly6–7
seconds after the cut. The shared metadata lookup permits only three2-second
attempts and returned its attempt deadline before the clock fault's60-second
deadline. No replacement role or final controller audit was accepted.

The role observer now retries named transient transport failures until its
existing caller deadline, while permanent failures remain immediate. Focused
tests cover recovery after four failed reads, rejection of the old leader,
permanent error handling and deadline cancellation. The strict p99 gates are
unchanged. These failed runs establish an observation-budget problem; they do
not establish safety, timer behavior after clock transition, or a server cause.
Events, observed roles and hashes of every downloaded artifact are retained in
`role-smoke-failures/`. The downloads remain at
`/tmp/js-wf-clock-ahead-roles-36937359624` and
`/tmp/js-wf-clock-behind-roles-36937362297`.

Replacement smokes at clean source `0682654`: [ahead36938296826](https://github.com/AntPAllen/js-wf/actions/runs/36938296826) and [behind36938299478](https://github.com/AntPAllen/js-wf/actions/runs/36938299478). Both were authoritatively queued at launch; no acceptance is claimed. Exact source and inputs are retained.

## Corrected smoke outcomes

Behind36938299478 passes112.59s; independently rerun row guard and both event
reviewers pass. Full original artifacts and verification reports are retained
in `behind-corrected-smoke/`. It verifies196 invocations,2158 entries,one
skewed-owner cut,eight role observations,156 shifted clock lookups,168 audited
timer waits and physical drain. It remains shortened smoke evidence.

Ahead36938296826 fails130.55s at physical drain after its role transition and
controller audit. All logged workload p99 values are below30s, but two WF_RUN
messages remain through the30-second drain window. Their subjects/headers were
not retained, so the cause is unconfirmed. Events,roles,actual process operations,
last drain attempts and all original artifact hashes are retained in
`ahead-corrected-drain-failure/`. The raw download remains at
`/tmp/js-wf-clock-ahead-corrected-36938296826`.

A failed drain now captures bounded read-only raw retained-message observations:
at most512 sequence reads,10seconds total,2seconds per read,with explicit
truncation/errors and payload omission above4KiB. It uses the last successfully
observed stream range when the last metadata attempt times out. The diagnostic
runs after the gate has failed and cannot extend or satisfy the drain gate.
Compilation and existing role-wait regressions pass (0.359s); this new diagnostic
has not yet been exercised by a real failing cluster. No rerun is claimed here.
