# Worker-clock replica proof capture correction

Run 37157123048 at `98a9254a6674fef5e413e985dd0b2ff4f1c4fba5`, job
111303440191 (`worker_clock`, requested seeds 1–13), is **failed**. Only seed 1
executed. Its named workload and package passed (842.12/843.157 s), but the
post-run reviewer rejected one retained `MATRIX_CLOCK` snapshot. It has five
file replicas and four distinct followers; node 3 reports `current=false`,
`lag=1`, while the other followers are current. No later snapshot in that
observation establishes five-current readiness. This seed and shard remain
unqualified; the failed job is not converted to success.

The workload completed 107 batches, 2,996 invocations and 33,183 journal entries
with 19 periodic clock observations (plus initial/final proofs). Largest logged terminal/progress type p99 values
are 5.248717886/0.743965008 s. These passing assertions do not override the
missing readiness proof or clear a matrix gate.

## Correction

`captureTier3WorkerClocks` previously saved the first successful metadata reply.
A publish acknowledgment confirms quorum, not instantaneous follower catch-up.
The fixture now polls for five distinct current file replicas within a separate
two-second bound, saves the successful snapshot, and revalidates all clock
samples against the final observation time. Wrong configuration fails
immediately; persistent lag, offline/duplicate peers and absent cluster metadata
cannot produce a successful proof. The four-second clock freshness bound and
all latency gates remain unchanged. No production runtime or reviewer changes.

The exact rejected metadata is retained as
`integration/testdata/worker-clock-lag-one-37157123048.json`. Focused normal and
race tests pass (0.115/1.135 s). A compiled omission of the catch-up wait fails
the actual-snapshot regression with `calls=1`. The unchanged reviewer reproduces
the original rejection on unchanged raw inputs, and its existing stale-replica
negative control remains passing. These tests establish the fixture correction;
a fresh full ten-minute execution is required before qualifying it.

## Complete original evidence

[Failure manifest](failure/manifest.json) records the retained job/API/logs,
raw report artifact, original store archive and manifest, original-member
readback verdict, actual rejection replay, corrected fixture/test sources,
patch and compiled-control logs. Both hosted artifacts are retained:
11286419973 (2,941,447 bytes) and 11286566824 (107,605,310 bytes).
All 3,960 members of the uploaded store archive independently SHA-verify.
Its physical stores have not been independently reopened as brokers.

The outer proof archive is split into numbered 25 MiB parts. Reassemble in
lexical order before extracting; the manifest records each part's size/hash
and the complete gzip archive hash. Every outer archive member was read back
and SHA-verified before atomic publication, as were every part and the
reassembled archive. Executables/source copies and store files are originals,
not a re-executed reconstruction. Focused local tests used the recorded dirty
fixture patch over `577d560`; no local executable provenance gate is claimed.

The remaining live matrix jobs are not restarted. A targeted replacement case
can establish the corrected capture behavior; it does not by itself qualify
the failed 13-seed shard, 200-seed row or full Tier3/24-hour/five-VM gates.

## Targeted replacement dispatched

[Run 37161589577](https://github.com/AntPAllen/js-wf/actions/runs/37161589577)
requests exactly worker-clock seed 1 for ten minutes at corrected source
`070dd954a63eb8d36dc40040805baaf5cc00f0aa`. The API confirms that exact head,
queued status and planner job 111315971795. [Dispatch originals and scope](replacement-dispatch/)
retain the zero-exit command output, authoritative metadata and readback hashes.
Dispatch is not a pass and clears no gate. It does not rerun the historical
30-second-TTL worker-kill mismatch or replace the full matrix campaign.
