# SDK Select request admission evidence — 2026-10-11

Production Select retains its narrow strict decoder and additionally uses
stepwire.Decode before normalized request comparison. Present null metadata can
no longer become typed zero values and consume a recorded signal.

## Controls and results

Baseline source5ff4d52 plus the retained regression exits1. Five optional null
case fields (timer_step, fire_at, clock_domain, child_type, child_id) return42,
consume signal7 and advance the cursor to2. Other nulls produced nondeterminism
instead of corrupt-journal admission. The original production source and actual
baseline log/command result are retained; baseline executable capture was missed.

Final direct contexts have16 controls: healthy/explicit-zero positive, four
unknown/duplicate-field negatives, root kind/cases null, null case object, and
seven null case fields. Rejection requires ErrCorruptJournal, index-1, nil data,
unchanged cursor and unconsumed signal; no path appends. Existing Select tests
also cover priority, retained losers, timer/promise reuse, reordered cases and
lost completion acknowledgement.

Full race wf and stepwire suites exit0 (see race.log for actual package times).
Both actual race executable SHA/build/commands were observed. Normal exits0 for
the exact current SDK CI selector. Those short normal executables were missed;
normal source/log evidence is not binary-bound. Hosted CI execution is unclaimed.
The exact parsed workflow Python guard passes against both retained logs.

All1146 prelaunch Go/module/workflow inputs match each command's manifest. The
regression was formatted after race closure, before normal launch; the reviewer
requires its retained race bytes to format to the final source exactly. No
runtime semantic edits occurred between the commands.

## Reproduce review

```sh
python3 docs/scale/sdk-select-request-admission-2026-10-11/review.py
```

run.py executes the full race suites; normal-run.py executes the current focused
normal selector. Running these again replaces their evidence files.

## Scope

Direct Context API admission complements offline Replay preflight. These tests
are not whole worker before-handler evidence, real-cluster fault qualification,
full current162-family qualification, full I4 program causality or power-loss
proof. No seed families/pins changed; public continuation admission remains off.
