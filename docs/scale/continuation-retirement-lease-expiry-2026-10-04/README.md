# Retirement/reuse, manifest loss and lease expiry across server SIGKILL

The focused race case passes57.78s. It verifies the actual held lease and configured
12s TTL before the fresh manifest cut, reaps all three servers with SIGKILL status,
holds every server down13.000311269s, then starts distinct replacement PIDs.
The fault controller completes its bounded outage even when heartbeat failure
cancels the SDK publication. Original30s startup,15s publication and60s scenario
budgets remain; the production TTL is unchanged.

Raw logical journal completion advances from captured lease epoch54 to terminal
78 across11 records. Generation1→3, two reclaimed retired objects, exactly three
effects, two terminals and shared/survivor/fresh blob references are checked.
Fresh initial handler executes three times for repair while the recorded effect
executes once in the fresh generation. Exactly one manifest loss is required.
The existing state-leader/full-cluster cuts share the factored process helper.

Actual live race SDK/env/all build-info and629 unchanged selected local inputs
verify;628 match base `40511a4`, with the exact modified process test retained.
SDK SHA256 `d4969b9ef8b9fec1ee4bd23d0b292382562f308ba479fc2d221deda1880bdf51`.
All438 archive members and two parts read back; compressed43202242bytes,
SHA256 `e33b177b844e6a62050fdcb657bbbf3da3f2c254b6b0a10237be0d3694f4b97c`.
Complete original SDK/process stores/logs, selected pre/post ledgers, exact test
inputs/overlay, launcher and archive reviewer are preserved. Concatenate parts
and verify the manifest before restore; use a fresh output root for execution.

This is a selected local source superset, not a hermetic toolchain/modules proof.
Stores were not independently reopened; integrity/GC retain named-test scope.
It qualifies this combined lease-expiry cut only. Worker SIGKILL, active-writer
GC, other limit/TTL cuts, p99, final-source full matrices and24h remain open.
