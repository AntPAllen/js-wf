# Offline terminal admission

The pre-change dbcf749 reader checked terminal JSON and generation, but could
enter the initial handler with contradictory terminal kinds/results and missing
or changed terminal objects. The new test fails 37 leaf cases against that source:
each records both successful export admission and one actual handler entry.
Two already-protected empty identified failures and 18 positive leaves remain
controls. The baseline command was observed exit 1; its receipt was written
post-execution and no baseline executable was captured.

Production replay now checks the terminal kind and result shape before handler
entry, and verifies an external successful terminal result against supplied
object bytes. `ValidateReplayGraphHistory` uses the same guard before CLI plugin
loading. Successful user bytes stay opaque. Empty identity-free historical
terminal markers retain compatibility. This is admission of recorded metadata,
not comparison of an ordinary handler's returned result to the recorded result.
Existing continuation return-value validation remains separate.

The new SDK table exercises 19 cases across legacy identity-free, identified and
graph-v1 exports (57 controls). Ten CLI controls require terminal admission errors
before a deliberately nonexistent plugin can be opened. The CI terminal admission
row requires every new leaf in normal and race modes.

`run.py` executes the four-package normal/race admission suite, the complete SDK
race suite and a native R3 legacy continuation/plugin compatibility fixture with
existing CLI guards. Exact inputs, actual child executable SHA/size/exit/time and
outer command exits are retained. Binaries remain outside Git at
`/home/exedev/js-wf-replay-terminal-result-20261011`. The early normal replay-prefix
run passed; its manifest/receipt were captured afterwards, without its executable.

The workflow insertion was corrected after these Go executions. The original
workflow bytes are retained and bound by the pre-execution manifest; `review.py`
checks unchanged Go/module inputs, compiles all embedded workflow Python and runs
the final admission reducers against both captured logs. No hosted CI is claimed.

The native plugin fixture retains its original 90-second parent bound, real
worker/three servers/two continuation stages, result 60, no offline effects and
integrity checks. It uses legacy journals; its synthetic cancellation/limit tails
are compatibility controls. Temporary native stores/plugin files are cleaned by
the existing fixture and are not preserved or independently byte-audited here.
Canonical graph native matrices, full current simulation, deadline causes and
original scale/soak/adoption gates remain open. Public continuation stays disabled.

## Closed results

| Command | Actual exit | Named passes | Package test time |
| --- | --- | --- | --- |
| Baseline new SDK table | 1 | 18 positive + 2 existing rejections | 0.015 s |
| Normal admission, four packages | 0 | 498 | 0.352 / 0.224 / 0.109 / 0.100 s |
| Race admission, four packages | 0 | 498 | 2.532 / 2.771 / 1.145 / 1.147 s |
| Complete SDK race | 0 | 756 | 86.501 s |
| Native plugin + CLI guards, race | 0 | 37 | 19.417 s |

The reviewer binds 1229 inputs and ten captured SDK executions. All 13 embedded
workflow Python blocks compile, and both final admission reducers pass. The
incorrect first CI insertion and its actual syntax-check exit 1 remain recorded.
The seeded inventory remains 163 families / 946 pins. This change is not another
seeded family and cannot extend the scope of any frozen long campaign.
