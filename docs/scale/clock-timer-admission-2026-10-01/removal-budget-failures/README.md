# Pending timer cut fixture removal budget failures

Both clean opt-in smokes at source7101d4892038bab2e92c278a5a6c4ed0537fdddb
failed the strict removal-before-duration admission gate:
[ahead36943700912](https://github.com/AntPAllen/js-wf/actions/runs/36943700912)
and [behind36943703099](https://github.com/AntPAllen/js-wf/actions/runs/36943703099).
The selected250ms Sleep retained its exact suspended tail at refresh. Docker
SIGKILL plus automatic container removal returned365.403ms and357.130ms later,
respectively, exceeding the conservative earliest due boundary by165.707ms
and158.771ms. These are fixture admission failures; neither run establishes
accepted clock-transition coverage or a runtime timing verdict.

`summary.json` derives intervals from the original cut evidence. Both directories
retain every downloaded artifact and failed job log compressed losslessly,
with original byte counts and SHA256 hashes. A failed cut leaves node four
removed, so its final `docker logs` lookup also fails; that secondary diagnostic
error does not explain the admission miss.

The new opt-in `first-wait-2s` profile gives timer-0 two seconds while retaining
seven250ms waits. Both candidate selection and retained-tail refresh require
750ms of remaining conservative duration. Removal must still finish strictly
before earliest due, and the final journal must corroborate the selected prefix.
Baseline clock rows keep all eight250ms waits. The controller guard requires
the explicit profile and validates the exact eight durations, actual clock
origins, deadline bounds, completions and returns. The raw<30s p99 gate stays
unchanged. Longer waits cannot replace pending-effect/reply/continuation cuts
or the full clock/fault matrix.

Focused selector/refresh race controls pass1.033s, including short wait rejection
and longer wait admission for both clock directions. All34 Tier3 Python tests
and both controller tests pass; controller mutation controls reject an unknown
profile and a declared long-wait profile with only short waits. Corrected native
smokes are required before accepting any opt-in timer cut.

Corrected35s smokes dispatched at c03994c4f9a58ef388069cbb7f498caf3f27c7a6:
[ahead36945292216](https://github.com/AntPAllen/js-wf/actions/runs/36945292216)
and [behind36945294731](https://github.com/AntPAllen/js-wf/actions/runs/36945294731).
Both are queued with no accepted native verdict. Their API launch records are
retained in the parent directory as `*-removal-budget-smoke-start.json`.
