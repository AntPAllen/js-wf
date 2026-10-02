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

This advances the determinism category of the mixed mutation requirement.
The modes below provide other category fixtures; more fault combinations and
the full matrix release gates remain open. The default command still runs all
six smaller focused categories.
The `mixed-determinism` workflow job retains the optional mode's evidence.

## Live mixed lease exclusion challenge

```sh
python3 scripts/check-invariant-mutations.py --mixed-leases --output /tmp/js-wf-mixed-leases
```

This mode uses the same live 4/3/2/1 parent mix and six-child/twelve-grandchild
cohort. After confirmed suspension and a real journal-leader SIGKILL, four distinct
replacement short effects hold their leases. A challenger on the surviving node
tries to acquire the target invocation under another worker identity. The intact
lease implementation must return `ErrHeld`. The private-per-worker-key mutant
must actually return a new lease; that lease is released, the held effects resume,
and all28 invocations complete with their expected results and immutable prefixes.
Only then does the named test fail with its lease-admission escape marker. Build,
startup, timeout and unrelated cohort failures cannot count as mutation detections.

This detects the broken lease exclusion contract in a live mixed fault workload.
It does not claim that the mutated journal necessarily violates I2: journal CAS
provides a separate defense. The CI `mixed-leases` job retains both executions and
negative controls. This covers lease exclusion; the full sustained chaos release
gate remains open.

## Live mixed CAS challenge

```sh
python3 scripts/check-invariant-mutations.py --mixed-cas --output /tmp/js-wf-mixed-cas
```

After the shared mixed admission and actual journal-leader SIGKILL, two production
append calls read the same target tail and are gated before their CAS publications.
Both attempt a matching completion at logical index2. The intact runtime accepts
one and rejects the other, then the full28-invocation cohort completes and passes
its raw-state audit. The CAS-header-removal mutant must acknowledge two distinct
sequences whose retained raw entries have the same index, and the raw-state
checker must reject that exact target for the duplicate. Its detection stops at
retained corruption; the fixture does not delete the bad acknowledged entry to
force a completed mutant cohort. Raw receipts, all three fixture/gate source hashes
and semantic markers are retained. CI runs this as `mixed-cas`.

This covers CAS corruption; the original full chaos/invariant release gate
remains open.

## Additional mixed modes

Select exactly one mode per runner invocation, always with `--output DIR`.
Each mode records hashes for the shared fixture and its transport helper.

| Mode | Baseline proof | Required mutant evidence |
| --- | --- | --- |
| `--mixed-enqueue` | 64 concurrent equal-ID calls retain one actual dispatch; all28 complete | At least64 distinct retained dispatches without message IDs; all28 still complete and audit |
| `--mixed-start-repair` | A durable29th start with all initial dispatches dropped is repaired and completes | Scanner does no work; original28 complete and audit while the orphan has no dispatch, journal or state |
| `--mixed-purge` | Retention interrupted before signal deletion resumes idempotently; reused ID has a fresh journal/new-generation outcome | Invocation disappears while journal/state/marker remain; actual purge retry returns ErrNotFound |

All three require actual mixed admission, confirmed journal-leader SIGKILL and
retained-state checks. Enqueue filters raw dispatch reads by physical sequence,
partition subject and invocation body. Start repair reads the original invocation
receipt and actual generation-tagged repaired dispatch. Its readiness checks use
bounded attempts; an expired attempt is never a liveness-failure proof. Purge
injects its cut through the real JetStream adapter and preserves the deliberately
broken mutant state; it does not patch the invocation back into the stream.

The workflow now runs the smaller six-category gate plus all six mixed modes.
These are focused mixed fixtures with one admitted leader kill. They do not
replace the plan's sustained whole-chaos mutation campaign, full seed counts or
24-hour matrix. Accepted original artifacts and independently checked receipts
are linked from [implementation status](implementation-status.md).
