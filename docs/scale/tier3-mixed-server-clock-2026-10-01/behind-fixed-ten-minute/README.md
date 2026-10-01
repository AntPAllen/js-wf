# SDK fix sustained behind-clock proof

Run36941535795 at clean source55bd33e5bd933d0b2f7869de774e777514b97807
passes723.32s with83 batches,2324 invocations,25,601 retained journal entries
and19 actual shifted-source peer4 SIGKILL/restart cuts. All three current
artifact reviewers independently pass against original downloads.

The controller audit verifies1992 timer waits with no early completion,
195 actual all-five clock observations and116 clock-role observations.
Worst terminal p99 is13.427461733s; worst progress p99 is10.864556460s.
Histories,invariants,strict p99,all immutable terminals and physical
WF_RUN/64-consumer drain pass. All93 repair records match final counters:
92 acknowledged,one uncertain; no fencing records occur.

This verifies one sustained R5 row after the SDK fix. It does not admit
every pending timer at every cut or all effect/continuation combinations,
and does not clear200-seed or24-hour full-matrix release gates. The new
opt-in timer-cut path is later source and not covered by this evidence.
Original artifacts are retained; files over16KiB use deterministic gzip.
Hashes refer to original uncompressed downloaded bytes.
