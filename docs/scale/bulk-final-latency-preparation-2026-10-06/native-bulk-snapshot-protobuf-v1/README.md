# Protobuf snapshot latency oracle

Accepted source `adf84cf2ca1fc152611a728024c6d4b5062c3898`. Actual race SDK PID 2662291 closed, SHA256 `91454ef42f1f0dc173b6bb02aa2c3acde4711e36a8944a0cd765257f3d96abc2`. Actual protobuf-v1 worker writes, 160 real workflows, PASS50.73s. Frozen/shared/serial/parallel/cached/bulk samples match; deadline negative controls reject partial success.

All160 unpurged snapshot fallbacks match the original samples. After a three-entry prefix purge, complete logical integrity remains160 invocations/journals/terminals and768 entries; both point and bulk reject missing server timestamps without partial samples. Compaction cannot reconstruct original server timestamps.

Full 2809-member/32579782-byte archive SHA256 `9b7eedbf330c36ba7dc2c01a9f8dfabca149058be8553eaaed492d2c52fcdaef`, all members/parts/concatenation verified. Selected Git Go/module inputs, actual executable dependencies, source before/after and closure verified; external compiler inputs not exhaustive. No large-cohort, memory-bound, transport-fault or24h qualification; bulk remains opt-in.
