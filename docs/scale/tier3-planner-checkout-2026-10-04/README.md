# Tier3 planner checkout without retained proof archives

Diagnostic run 37170797762's planner checkout takes about three minutes
(02:28:17–02:31:29 UTC); the full repository contains large retained broker and
executable proof archives. The planner only needs `scripts`, `.github` and root
files. Its checkout now requests those two directories with the documented
cone sparse checkout. Workload jobs keep their original full checkout.

The official [checkout v4 input documentation](https://github.com/actions/checkout/blob/v4/README.md)
provides `sparse-checkout`. Its [implementation](https://github.com/actions/checkout/blob/v4/src/git-source-provider.ts)
selects `blob:none` when sparse checkout is configured and no explicit filter
is supplied. The workflow uses that documented input without setting an
explicit filter that would override it.

An actual fresh remote filtered clone and sparse checkout materializes only
3.1 MiB including Git metadata; no `docs` directory is present. From that
isolated checkout, all five existing planner tests pass in **0.004 seconds**.
An actual planner execution with both clock flags generates **256 shards**,
covering exactly all **16 rows × seeds 1–200**, with unique complete ranges.
The workflow registry is present and the planner's artifact-name controls pass.
Every materialized tracked file matches the main worktree after execution.
The isolated clone is at the recorded baseline head with only the edited
workflow copied in; this is a postexecution source comparison, not a clean
committed-source or pre/post compilation ledger.

`proof.tar.gz` retains every materialized tracked input, before/after workflow,
exact sparse patterns, commands, complete matrix output, clone log, controls
and byte-count summary. Every member was independently read back and hash
verified. This qualifies the planner input reduction and complete planning
behavior only. Actual hosted checkout speed improvement is not yet measured;
no workload, history, runtime, matrix or 24-hour gate is qualified by this change.
The already-running diagnostic and campaigns retain their original revisions.
