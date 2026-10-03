# Exact-source full release campaigns dispatched

Both campaigns target `c4fed061bc614488d4f89b53b216b756490f7da0`, including
all six scanner prefix corrections, physical deletion-marker cleanup, the full
100,000-tombstone physical census gate, and mandatory raw-artifact qualification.

| Required campaign | Handle | Scope |
| --- | --- | --- |
| Full sustained Tier2 matrix | [37149506857](https://github.com/AntPAllen/js-wf/actions/runs/37149506857) | All 13 rows, seeds 1–200 per row, ten minutes per seed: 2,600 executions |
| Sustained semantic mutation challenge | [37149508529](https://github.com/AntPAllen/js-wf/actions/runs/37149508529) | All six categories; original ten-minute baseline and mutant gates |

Both are confirmed queued at the requested exact source. Dispatch/queue state
is not execution or acceptance. No older campaign was restarted or cancelled.
The older matrix at `9ac3ad4` and accepted mutation source `0f979d0` predate
subsequent runtime corrections, so they cannot clear these newer source gates.

The metadata archive preserves the observations, with every member reopened
and SHA256 verified. Future acceptance requires actual terminal conclusions,
all requested jobs/seeds and reviewed raw artifacts. These campaigns do not
qualify the independent five-node 24-hour full matrix or original million-timer
24-hour proof. Tier1 race/100k at runtime source `9ecc37c` remain separately live.
