# JSON snapshot latency oracle

Accepted source `c674e8b128448cc32877ae454428c9c9336c20f8`. Actual race SDK PID 2643617 closed, SHA256 `0040fab4ab985ce2019593aadbbd1a86920ab615ddaadac0a183e72b108f5f8b`. Native 160-workflow test passed in 49.19 seconds. All frozen/shared/serial/parallel/cached/bulk samples match; original deadline failures reject partial success.

All 160 snapshot manifests before purge preserve exact frozen-point samples; bulk executes 160 point fallbacks with identical samples. After one three-entry prefix is purged, independent complete logical integrity remains 160 invocations/journals/terminals and 768 entries. Both point and bulk reject absent server timestamps with no partial samples. Snapshot logical entries cannot substitute for original server timestamp evidence.

Full 2805-member archive, 32498174 bytes, SHA256 `e27aadea19f52b504eef956a3e9f0db5a64d43639769d7e604ec3273735f11be`, all members/parts/concatenation read back. Selected Git Go/module inputs, source before/after, actual executable dependencies and closure verified. External compiler inputs are not exhaustive. No large-cohort, fault or 24h qualification; bulk remains opt-in only.
