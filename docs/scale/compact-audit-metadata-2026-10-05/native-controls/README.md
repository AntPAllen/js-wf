# Compact coordinates: current R3 native race controls

Executed2c4995e, actual SDK616708 SHA256
`8c481fa2fb05d555643d4d9d5e64cefd55f96f158a1f05aa37e642a216af4552`.
Four native tests pass: compaction/cohort/freshcorruption56.09s, journalcorruption
13.87s, statevalues11.89s, actual64delivery metadata/timestamp contract4.39s.
The shared comparator checks candidate reports and corruption errors against the
unchanged point oracle; terminal/state/tombstone/orphan/cohort cases remain exact.
Real bound SDK delivery receivers match modern SDK coordinates, including time,
and the candidate metadata path asserts zero allocation, proving fast path use.

Independent selected Git Go/module source/body and before/after binding, observed
SDK bytes/buildVCS/race and process closure verified. Current R3 server library
is linked into the observed SDK; no separate external server-byte observations
are claimed. Capture is not exhaustive compiler/toolchain provenance. Complete
closed source/SDK/stores/logs/review archive read back and split into parts.

Defaults remain SDK-based. These controls do not qualify400k capacity, full fault
matrix, legacy behavior or24h soak. Explicit compact consumer-leader-loss and
cancellation controls are prepared as separate native cases.
