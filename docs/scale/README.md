# Subject cardinality measurement

The standalone `cmd/wf-scale` harness starts three separate NATS 2.15.0
processes with file storage and three replicas. It calls `provision.Ensure`,
then writes one one-byte message per distinct ID to each of `WF_INV` and
`WF_JRN`. At every checkpoint it checks both stream message and subject counts
through each node, reads each server process's Linux `VmRSS`, and times five
`Stream.Info` calls per stream and node. The original JSON report is
[`subject-cardinality-2026-09-28.json`](subject-cardinality-2026-09-28.json).

| Subjects in each stream | Node 0 RSS | Node 1 RSS | Node 2 RSS | Median `Stream.Info` latency across six node/stream pairs |
| ---: | ---: | ---: | ---: | ---: |
| 0 (provisioned baseline) | 22.6 MiB | 22.1 MiB | 22.2 MiB | — |
| 100,000 | 107.4 MiB | 119.2 MiB | 122.7 MiB | 0.46–0.77 ms |
| 1,000,000 | 553.3 MiB | 575.7 MiB | 599.9 MiB | 0.20–0.63 ms |

Both streams had exactly the reported number of messages and subjects on all
three nodes at each checkpoint. The 1M checkpoint thus held 2M distinct
subjects per server across `WF_INV` and `WF_JRN`. The numbers are whole-process
RSS, including NATS and the other empty provisioned stores, measured three
seconds after publishing. They are not a stable per-subject cost or a bound for
larger deployments. The message payload is deliberately tiny, so this isolates
index-heavy usage more than real workflow payload usage.

After the VM grew to 15 GiB RAM and 35 GiB disk, the same harness completed
fresh three-node runs through [5M](subject-cardinality-5m-2026-09-28.json) and
[10M](subject-cardinality-10m-2026-09-28.json) subjects per stream. These
checkpoints again matched the exact message and subject counts on every node.
The 10M tier retained 20M distinct subjects per server across the two streams.

| Subjects in each stream | Run | Node 0 RSS | Node 1 RSS | Node 2 RSS | Median `Stream.Info` range |
| ---: | --- | ---: | ---: | ---: | ---: |
| 3,000,000 | 5M run | 1,448.7 MiB | 1,630.3 MiB | 1,494.8 MiB | 0.138–0.253 ms |
| 5,000,000 | 10M run | 2,181.9 MiB | 2,177.9 MiB | 2,292.4 MiB | 0.205–0.623 ms |
| 10,000,000 | 10M run | 3,365.2 MiB | 3,393.1 MiB | 3,573.9 MiB | 0.116–0.427 ms |

The 10M run published its final 5M IDs per stream in 3m2s with 96 concurrent
publishers. Each server held a full file-backed replica; the retained stores
used 2.9 GiB of disk together. These values are one run's whole-process RSS
and stream-info timings, not a capacity guarantee for larger payloads or live
workflow traffic.

Run the same tiers on a host with enough memory for three full replicas:

```sh
go build -o /tmp/wf-scale ./cmd/wf-scale
/tmp/wf-scale -root /tmp/wf-scale-10m -counts 1000000,5000000,10000000 \
  -workers 96 -min-available-mib 2048
```

Use a fresh root for every run. The harness writes `report.json` after each
completed checkpoint and retains the file stores when a root is specified.
The optional memory guard cancels publishing if Linux `MemAvailable` falls below
the requested threshold; the 2 GiB guard did not fire in the 10M run. It was
also exercised with a threshold above available RAM and stopped before the
first checkpoint.

## Live workflows at cardinality checkpoints

Add `-live-workflows` to run actual production client, worker, SDK and journal
operations after each background checkpoint. `-live-input-bytes` sets the exact
JSON input length; inputs larger than the client's inline limit use `WF_BLOB`.
For example:

```sh
go build -o /tmp/wf-scale ./cmd/wf-scale
/tmp/wf-scale -root /tmp/wf-scale-live-1m -counts 1000000 \
  -live-workflows 1000 -live-input-bytes 1100000 -min-available-mib 2048
```

Three workers serve all 64 partitions through pinned peers. Sixteen concurrent
clients start workflows whose durable step verifies input length and SHA256.
Each result must match and remain byte-identical when read through all three
peers. The cohort audit checks actual retained invocation identities/input
hashes, journals and terminal state, exactly four entries per invocation, input
spill counts, and a drained `WF_RUN`. Effects follow the runtime's at-least-once
contract; their execution count is reported. These are no-fault client-start-to-
result latencies, including the first consumer startup, rather than fault-recovery
latencies or a throughput release gate.

Earlier live cohorts remain retained. Reports distinguish background subject
counts from total subjects/messages and verify totals through each node. The
one-byte background messages are opaque capacity data; the invariant audit
explicitly covers only real live cohorts. `live-<checkpoint>-audit.json` retains
that cohort's snapshot and expected results. Audit JSON is compact because
pretty-printing changes journal payload bytes. Reports include build revision,
modified-source state, terminal success/failure status, and RSS after each phase.
Use a fresh explicit root to preserve all stores and evidence.

The [three-process smoke](live-cardinality-smoke-2026-09-30/report.json) passed
100 and 200 background subjects per stream, with twelve 1,100,000-byte inputs
after each checkpoint. Both cohorts had twelve spills, twelve terminal journals,
48 entries, and twelve effect executions. The second phase retained both cohorts,
with 224 invocation messages/subjects and 296 journal messages on every node.
This binary was built from modified source based on `df72ed0`; it is a runner
smoke, not the million/ten-million-subject proof. The retained race contract
covers small inline inputs followed by spilled inputs and rechecks serialized
evidence with the invariant checker; it also rejects totals omitting prior cohorts.

The [clean-source million-subject live run](live-cardinality-1m-2026-09-30/report.json)
at `151735c` completed 1,000 workflows with 1,100,000-byte inputs, all spilled,
with 4,000 audited journal entries and 1,000 immutable terminal results. Its
start-to-result p99/max were 1.280/1.418 seconds, including first-consumer startup.
After the live phase server RSS was 1,728–1,867 MiB, greater than the
544–605 MiB index-only checkpoint; input storage and live runtime activity
matter for sizing. The retained file stores together occupy 3.5 GiB. The run
coexisted with the separate million-timer campaign on this VM, so its timings
are observations under that load. The 10M live requirement remains open.

The [clean-source inline run through 5M](live-cardinality-inline-5m-2026-09-30/report.json)
completed 1,000 64-byte-input workflows at each of 1M and 5M. The respective
start-to-result p99 values were 503 and 136 ms; final server RSS was
1,982–2,055 MiB. Both retained cohorts remained present in the verified final
counts (5,002,000 subjects per stream and 5,008,000 journal messages). Raw
cohort audits and checksums are retained alongside the report. Large payloads
at 5M/10M and all live traffic at 10M remain pending.

## CAS append throughput

`cmd/wf-cas-bench` uses the regular three-node in-process fixture with file
storage and three replicas. It calls the production `journal.Append` API, which
reads the subject tail before a CAS publish. Provisioning and final stream
checks are outside the timed intervals. The default workload writes 10,000
entries to one journal, then 100 entries to each of 1,000 concurrent journals.
All acknowledged appends are checked against the final `WF_JRN` message and
subject counts.

Three default runs on this VM recorded:

| Workload | Run 1 | Run 2 | Run 3 | Median |
| --- | ---: | ---: | ---: | ---: |
| One hot journal, appends/s | 2,603 | 3,096 | 3,258 | 3,096 |
| 1,000 concurrent journals, aggregate appends/s | 13,886 | 14,326 | 14,097 | 14,097 |

A separate run with 10,000 concurrent journals and 10 entries each completed
100,000 appends at 13,033 appends/s. The raw reports are
[`cas-throughput-2026-09-28.json`](cas-throughput-2026-09-28.json),
[`cas-throughput-repeat-2-2026-09-28.json`](cas-throughput-repeat-2-2026-09-28.json),
[`cas-throughput-repeat-3-2026-09-28.json`](cas-throughput-repeat-3-2026-09-28.json),
and [`cas-throughput-10k-concurrent-2026-09-28.json`](cas-throughput-10k-concurrent-2026-09-28.json).

```sh
go run ./cmd/wf-cas-bench -output /tmp/cas-current.json \
  -baseline docs/scale/cas-throughput-repeat-2-2026-09-28.json
```

The optional baseline comparison rejects a workload or runtime mismatch and
fails if either rate falls more than 20% below its baseline. The new
`cas-throughput` CI workflow builds the fixed `4fa3119` production reference
and candidate with the same current benchmark harness. The baseline journal,
provisioning, fixture and dependencies remain at the fixed revision; a source
diff guard rejects any production overlay. Two jobs cover hot writes through
the leader and through a follower separately, each running both revisions
sequentially on one isolated Ubuntu 24.04 runner. Three rounds alternate baseline/candidate order; the median rate of each
workload must be at least 80% of its baseline median. Relevant main pushes and
pull requests trigger the job; it also supports manual dispatch. The pinned
reference is changed explicitly in workflow source, never refreshed automatically.

Every sample must retain exactly 110,000 messages across 1,001 journal subjects
and match the fixed workload and Go/NATS versions. Bad/missing reports, process
failures, timeouts, incompatible versions and either throughput regression fail
the job. The artifact preserves six raw JSON reports, process logs, binary hashes,
production source revisions, shared harness sources/checksums, client/leader
placement before and after the hot sample, execution order, host metadata and
the median comparison. Missing/mismatched placement or a changed observed
leader rejects a controlled sample. Historical reports without placement retain
their original pinned-node semantics.
Changing Go or NATS versions requires selecting a compatible reference through
an explicit source edit. Branch protection is configured separately from this
workflow.

A local same-source paired control at `4fa3119` passed with median candidate /
baseline ratios of 0.947 for the hot journal and 1.050 for parallel journals.
Both independently built binaries used the same Go sources; this validates the
runner/comparison path, not a performance improvement. The
[summary and raw reports](cas-throughput-paired-2026-09-30/summary.json) retain all
six full workloads. The [first hosted CI run](https://github.com/AntPAllen/js-wf/actions/runs/36764395969)
passed with hot/parallel ratios of 0.991 / 0.988; its
[summary and raw reports](cas-throughput-paired-ci-2026-09-30/summary.json)
reproduce both medians and all exact counts. Runner load, disk
variance and thermal effects can still affect a paired comparison; a failure
requires inspection of the reports rather than automatic threshold relaxation.
The checker tests include the exact 80% boundary, independent hot/parallel misses,
median outliers, missing fresh output, invalid rates/counts and runtime mismatch.

One additional default run returned
`ErrStale` at hot-journal index 4,189 despite one writer and no injected fault.
Raw file blocks retained by all three replicas ended at index 4,188, with no
4,189 entry; the server-side reason for the wrong-last-sequence reply remains
unknown. `journal.Append` now rereads the subject tail after that reply and
retries the same CAS up to twice when the tail has not advanced. It still
returns `ErrStale` for a newer tail and returns `ErrUnknown` if repeated
rejections cannot establish a winner. An injected reproduction covers both
outcomes, and the 10,000-round contender proof still passes. A later benchmark
and 30 diagnostic hot-only repetitions passed, but those runs do not identify
the server-side cause. A separate single-writer test killed the stream leader
after the tail read but before the CAS publish and passed ten runs, so an
ordinary leader change at that point does not reproduce the anomaly.

## Live large inputs

The opt-in test starts 1,000 distinct workflows with JSON inputs just over
1 MiB. Run it with:

```sh
WF_LARGE_INPUT_SCALE=1 go test ./integration \
  -run '^TestLargeInputsAcrossLiveWorkers$' -count=1 -timeout=12m -v
```

Each input has a unique
content hash, so the run exercises 1,000 Object Store uploads rather than one
shared object. Six workers load and validate the full input, return its ID,
and drain all 64 run partitions. The test checks every result and the final
invocation, journal, and run-stream counts. Set `WF_LARGE_INPUT_COUNT` to a
smaller number for diagnosis.

The full local run passed: 1,048,584 bytes per input, 1,048,584,000 total
input bytes, 20.61 seconds to start all workflows, 20.93 seconds to obtain
all results, and 24.70 seconds for the complete test. The three replicated
file stores temporarily used about 3 GiB. A repeat also checked the third
replica's Object Store listing for exactly 1,000 distinct input objects and
passed in 20.28 seconds, with all results obtained in 16.24 seconds and
worker shutdown in under one millisecond. This is a live large-payload check
at 1,000 invocations; it does not establish the plan's 100,000-ID
large-payload or faulted cardinality target.
The manual `large-input-scale` workflow runs this proof on GitHub Actions.
Its first run at `c7113ba` passed in 14.65 seconds of Go test time: starts
took 9.22 seconds, all results were checked by 10.21 seconds, and worker
shutdown took 134 microseconds.
