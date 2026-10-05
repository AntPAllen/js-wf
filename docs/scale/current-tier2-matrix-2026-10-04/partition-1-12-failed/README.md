# Failed Tier2 partition shard 1–12

Run 37149506857 / job 111287268311 at checkout
`c4fed061bc614488d4f89b53b216b756490f7da0` failed during seed 1 after 405.90 seconds.
Seeds 2–12 did not execute in this shard. The parent/full matrix is unqualified.

Earlier checkpoint audits through batch 40 pass. At batch 50 / invocation cutoff
1,400, the retained audit reports 1,400 invocations, 1,130 journals, 8,745 entries
and 1,129 terminals, then fails:

> wf.jrn.matrixshort.seed-1-batch-49-5: terminal state missing: nats: key not found

The subsequent fault context cancellation is a consequence of the failed audit.
Request/state publication timing, storage/replication, and server cause have not
been independently established. No failure is erased or converted to a pass.

Artifact 11319330127 `matrix-partition-1-12-10m` contains nine original files:
client history, dispatch, operations, latency/fault records, test JSONL, and all
three server logs. Its authenticated metadata size/digest verify:
17,531,161 compressed bytes / 250,161,725 expanded bytes,
SHA-256 `59dcbf915da1bc9701f543f3088eeb8d111300c5d5b2177f95261ed884d57181`.
All unique regular canonical ZIP members are read and preserved. Run/job/artifact
binding and checkout log agree. Complete raw ZIP, expanded files, job log and
metadata are preserved in two archive parts and read back.

The executed workload binary, full source inventory, physical stores, and an
independent history-model review are unavailable here. This is preservation and
localization of a failed shard, not a successful row qualification.
