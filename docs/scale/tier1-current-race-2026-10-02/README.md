# Incomplete local current-corpus race run and new100k campaign

The local full simulator race attempt ran source
`2617ce2c456bda1df24b5fb1668358bf9608f9fc`, with1,000 seeds and aggregate
coverage enabled. Its authoritative service is terminal/failed with exit1:
`js-wf-tier1-current-1000-coverage-20261002.service`. Go reaches its configured
18-minute whole-package deadline at1080.089s, while
`TestSeededWorkerResultBudgetReplay` is active for1m8s at seed377 and executing
JSON encoding. This is a package time-budget failure, not proof of an individual
workload deadlock. Do not claim a complete pass or infer a broker defect.

Independent partial review retains135 top-level passes, the two documented
trace-only skips and all234 source-inventory pins. Nineteen top-level tests
remain incomplete or unexecuted and are enumerated in`partial-review.json`.
No aggregate completion summary or accepted suite report exists. Originals,
source, inventories, timing, exact Go events and terminal service state are
retained; compressed bytes are verified with original SHA256 manifests.

An initial launch omitted the aggregate-coverage flag and was explicitly
stopped before accepting any evidence; the corrected handle above is the
authoritative attempted suite. Neither launch has been silently restarted after
an observation timeout.

[Current-source100k campaign36962606650](https://github.com/AntPAllen/js-wf/actions/runs/36962606650)
is queued at`58288751c49bbb40c10214319842190a3d5eaa60`, with the workflow's
normal300-minute Go deadline. This includes the new child-start budget, canonical
clock workloads and membership-session recovery. The launch snapshot is
retained; queued state supplies no acceptance. Review this handle before
launching another whole-suite campaign. The earlier independently reviewed
100k run remains scoped to57ecf06 and184 pins.

The same100k handle is now confirmed in progress; an additional API snapshot
is retained. It supplies no terminal acceptance result.
