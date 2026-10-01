# R5 mid-fan-out all-server restart race smoke

Final native race test PASS78.46s, public Go test2json package PASS79.487s.
Eight batches,224 terminal invocations,2,473 journal entries and one actual
five-server SIGKILL/restart. All six workload raw enabling/terminal p99 gates,
histories, retained invariants and physical stream/all64-consumer drain pass.
Largest terminal p99 is16.420112204s; largest progress p99 is11.353572556s.
Production sync_interval is2m. The effect barrier is test-only.

Cut parent tier3-1-batch-7-3 was suspended at stream sequence2260 with six
unique requested children and all six unfinished. Actual grandchild effects
were held through R5 recovery. Every container was killed/removed before any
replacement started, using retained stores and stable published endpoints.
The exact recovered parent prefix matches the cut byte-for-byte in the native
check and structurally in the JSON verifier. Final retained journals show the
same six terminal children and exactly two completed grandchildren per child,
with twelve unique grandchild identities.

All15 fencing events match worker counters and are heartbeat ownership
confirmation failures with missing stream responses during the confirmed
all-server outage. Exact fenced-delivery traces and later invocation acks are
retained separately; all15 invocations reach observed terminal state after
fencing. No exact server-side missing-response mechanism is asserted.
All44 acknowledged repair attempts (3start,34signal,7suspended) have checked
source/decision explanations. All37 Python tests and integration vet pass;
artifact controls reject terminal or mismatched cuts, missing children,
changed prefixes, late snapshots and incomplete final trees, in addition to
rolling/partial restarts and invalid node/timestamp evidence.

The shared barrier retains the native R3 method via a callback; optional cut
and final-tree artifacts are added for the R5 row. Binary source is9ce8dee
plus the retained patch; hashes of the tested Go files and binary are recorded.
The first smoke before final-tree JSON retention also passed78.19s/79.248s,
196 invocations and2,165 entries; its original events are preserved separately.
The final smoke verifies the added artifact contract.

This is one35-second smoke. It does not establish the ten-minute row, the full
24-hour matrix or arbitrary fan-out cardinalities. Raw large artifacts are
losslessly compressed. Both original server-store roots remain outside Git.
