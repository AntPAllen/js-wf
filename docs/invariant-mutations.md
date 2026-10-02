# Focused production mutation checks

Run from the repository:

```sh
python3 scripts/check-invariant-mutations.py --output /tmp/js-wf-invariant-mutations
```

The runner uses Go overlays to change production code without editing the
checkout. Each selected fixture must first pass unmodified. A mutant counts as
detected only if that exact test fails with the required semantic evidence.
Compilation failures, skipped tests, suite timeouts and unrelated failures do
not count. Two mandatory runner controls verify rejection of a compile error
and an unrelated test failure. Source anchors must occur exactly once; a source
change that invalidates an anchor fails the runner.

| Mutation | Production change | Required detection |
| --- | --- | --- |
| Missing journal CAS | Remove the expected subject sequence publish option | Two acknowledged winners in one gated append race |
| Independent worker leases | Give each worker a private lease key for the same invocation | Second worker incorrectly acquires while the first holds ownership |
| Reversed retirement order | Purge the invocation before its journal and terminal tombstone | Crash-boundary purge retry cannot recover the invocation |
| Missing determinism guard | Disable step kind/name/input comparison | Changed replay step is accepted without the required divergence error |
| Missing run message ID | Remove the enqueue deduplication publish option | 64 equal-ID concurrent enqueues retain 64 messages instead of one |
| Skipped start reconciler | Return without scanning or repairing starts | A retained invocation's absent wakeup remains unrepaired |

The CAS fixture runs 1,000 races and two journal-leader restarts. CAS, lease and
enqueue fixtures use real three-node clusters; retirement uses a real
single-node crash-boundary fixture. Replay and start repair use production code
with seeded in-memory transports. This is stronger than supplying a forged bad
history, but it is **not the full mixed-workload six-mutation chaos release gate**.
The independent full matrix, seed-count and soak requirements still apply.

The output directory contains `report.json`, unmodified and mutated Go JSON
test logs, negative-control logs and any modeled failure traces. The report
records the commit, production source hashes, selected tests, timings and
classification. A single category can be selected with `--mutation`; this does
not count as the complete six-category pass.

The `invariant-mutations` GitHub workflow runs manually or when relevant
production code, selected fixtures or the runner change on `main`. It always
uploads the output directory, including on failure. A survivor, fixture failure
or missing expected semantic marker fails CI.


## Live mixed determinism challenge

```sh
python3 scripts/check-invariant-mutations.py --mixed-determinism --output /tmp/js-wf-mixed-determinism
```

This optional mode selects only I4. The production worker runs four shorts,
three eight-timer waits, two sixteen-signal workflows and one six-child/twelve-
grandchild fan-out on the same R3 file-backed process cluster. Admission requires
four distinct short effects and durable suspension of the timer, signal and
fan-out parents. Grandchild effects remain pending until replacement. The journal
leader is SIGKILLed; its exit signal is verified and it restarts on its retained
store. One replacement handler renames a recorded pending step. The intact
runtime must retain its prefix, advance the fencing epoch, write a divergence
failure and execute the changed effect zero times. All 28 invocations reach a
terminal state and pass the retained-state audit.

The compiled guard-removal mutation must instead execute that changed effect
once and complete it with result42. Detection additionally requires the actual
admission, SIGKILL and complete-cohort markers. A baseline failure, timeout,
skip, build error or unrelated failure cannot count. The runner now verifies
that its unrelated-failure control actually executes the named failing test,
and records the fixture source hash. Signal attempts use a three-second budget
and retry uncertain outcomes with the same idempotency key inside the unchanged
150-second fixture deadline; worker metadata initialization is bounded too.

This advances one category of the mixed mutation requirement. The other five
mixed categories, more fault combinations and the full matrix release gates
remain open. The existing default command still runs all six focused categories.
The `mixed-determinism` workflow job retains the optional mode's evidence.
