# Subject cardinality measurement

The standalone `cmd/wf-scale` harness starts three separate NATS 2.15.0
processes with file storage and three replicas. It calls `provision.Ensure`,
then writes one one-byte message per distinct ID to each of `WF_INV` and
`WF_JRN`. At every checkpoint it checks both stream message and subject counts
through each node, reads each server process's Linux `VmRSS`, and times five
`Stream.Info` calls per stream and node. The JSON report is
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

Run the same tiers on a host with enough memory for three full replicas:

```sh
go build -o /tmp/wf-scale ./cmd/wf-scale
/tmp/wf-scale -root /tmp/wf-scale-10m -counts 1000000,5000000,10000000 -workers 96
```

Use a fresh root for every run. The harness writes `report.json` after each
completed checkpoint and retains the file stores when a root is specified.
The 5M and 10M tiers were not run on this 7.7 GiB VM: extrapolating the
observed 1M RSS across three processes approaches or exceeds its RAM, and the
measurement would risk an out-of-memory kill. Those tiers remain required by
the plan.

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
fails if either rate falls more than 20% below its baseline. It is not yet a CI
gate: a dedicated benchmark runner and a baseline from that runner are needed
for a meaningful regression check. One additional default run returned
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
