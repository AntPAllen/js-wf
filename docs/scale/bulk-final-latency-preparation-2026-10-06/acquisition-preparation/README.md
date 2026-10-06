# Bulk latency acquisition candidate

The explicit retained chunked R1 memory-cursor walker is now available to audit
visitors. It requires a caller deadline and retains the existing scanner's source,
sequence/gap/recovery and bounded cleanup checks. Source replication is unchanged.
This transport API alone does not check workflow invariants.

The final latency candidate requires a separately verified complete integrity
cohort, captures all four INV/STATE/JRN/SIG source high-water/count/byte/consumer
cuts, scans each exactly, preserves invocation order and projects only causal
journal payload fields. Signals and child terminal timestamps come from retained
server metadata. It calls the already-verified shared causal reducer. Unexpected
cohorts, count/index gaps, missing timestamps/terminal records, budget exhaustion,
changed final source cuts, lingering cursors and canceled/error results fail with
no partial samples. An inclusive3GiB application-data charge covers projections,
slice growth and output samples conservatively; it is not an RSS guarantee.
Decoder/SDK/map/GC overhead and process memory limits remain separate.

Snapshot-marked invocations use the existing logical journal and point timestamp
queries under their original20-second context, and fallback counts are retained.
Bulk server-clock rows are not enabled; they keep their controller receipts.
Bulk is not yet selected by any matrix or soak launcher.

Three race control groups pass: retained-walk admission/cancellation, projection
cohort/coordinate/budget controls, and exact causal payload projection/error
controls. integrity1.017s / integration1.064s. Actual fresh160-real-workflow
bulk-vs-frozen/serial/parallel/cached sample comparison and deadline/cleanup checks
are prepared. Large-cohort throughput, process memory, snapshot fallback,
protobuf/purge/reuse and fault qualification remain pending. Original20/60-second
retained limits and six-minute final-stage deadline remain unchanged.
