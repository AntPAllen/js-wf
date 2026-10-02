# Go workload passes; artifact duration-profile label is stale

Run36972576844 atca0c6b0 is terminal/failed in CI. Original Go test passes744.60s
(package745.628s):26 batches/728 invocations/8009 entries/19 faults, with final
latency/history/state/drain assertions passing. Worst workload terminal/progress
p99 is8.746/9.908s. Every admitted timer now lasts2s. The source mistakenly logs
first-wait-2s and the artifact checker consequently expects later250ms waits.
It rejects the actual2s observations as incorrect controller duration. Original
failed events/artifacts are retained without changing their profile labels.

Current source logs all-waits-2s and checks every observed duration against2s;
legacy first-wait-2s remains supported for historical artifacts. Negative controls
also reject all-waits profiles with short waits. Original CI is not upgraded to
success; fresh current-source artifact acceptance remains required. No unchanged
native rerun was launched solely to change the historical status.
