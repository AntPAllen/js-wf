# R5 server-clock peer skew with journal-leader kills

Clean-source35-second fixture smokes are launched at `a1e9387029a554eddcd5d52fed0545ef774aa714`:

- [Ahead36935066881](https://github.com/AntPAllen/js-wf/actions/runs/36935066881): actual server4 at+60s.
- [Behind36935069706](https://github.com/AntPAllen/js-wf/actions/runs/36935069706): actual server4 at−60s.

Each requires all five actual clocks at startup and before/after one confirmed journal-leader SIGKILL/restart, all six mixed workload cells, histories,invariants,independent controller conservative p99,all timer-clock origins and physical WF_RUN/64-consumer drain. The row guard and written fencing/repair/timeline reviewers must also pass. Original inputs/source are retained here. A queued/running handle is not verified acceptance, and35-second smoke never clears sustained/200-seed/24-hour release coverage.

## Strengthened clock-role smoke launches

- [Ahead36937359624](https://github.com/AntPAllen/js-wf/actions/runs/36937359624).
- [Behind36937362297](https://github.com/AntPAllen/js-wf/actions/runs/36937362297).

Both launch at `a12efce` with35-second inputs. They require skewed server4 to own both WF_RUN and WF_JRN before a cut,actual shifted timer-clock lookup evidence,and confirmed replacement leaders on unshifted peers before restart. All existing clock/controller/history/invariant/p99/drain gates remain. The original peer-only proofs cannot certify these stronger requirements. In-flight timer/effect/continuation combinations and sustained/full-matrix coverage remain open.
