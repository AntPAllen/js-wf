# Compact metadata legacy2.11.17 native race comparison

Executeda4a1b05, race nativePASS49.91s. Three actual2.11.17 server processes
620724/620725/620726 observed inside native test; each executable SHA256
`dc3a94debfc18ee9762c783d941db36a8360955d9686c86bd660a9acc63701aa`
matches retained standalone legacy binary and its2.11.17 main-module metadata.
All observed SDK/server PIDs closed. Selected Git Go/module before/after and
actual SDK byte/race/VCS bindings independently reviewed.

Shared full and captured-cohort comparator includes the explicit compactmetadata
reader against the point oracle, including compaction, freshcorruption, terminal
values, tombstones, corrupted snapshot and orphan journals. Existing public APIs
also checked. Complete1057-member archive read back and split into two parts.
Source capture is not exhaustive external compiler/toolchain provenance.

No default adoption, fullmatrix/24h qualification or400k capacity claim.
