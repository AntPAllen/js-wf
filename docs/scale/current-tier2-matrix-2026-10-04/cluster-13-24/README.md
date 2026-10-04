# All-server-kill seeds 13-24: independently accepted

Parent 37149506857, job 111287267105, artifact 11308433649 execute
`c4fed061bc614488d4f89b53b216b756490f7da0`. Twelve complete 600-second seeds qualify
28252 invocations / 311390 journal entries /
228 all-server fault events, each recording nodes [0, 1, 2] and node -1.
Executed source kills all three processes before restarting any, preserving
ports/stores, then waits for workflow replica catch-up before recording heal.
These semantics retain their source-bound named-test scope; process/store files
were not uploaded for independent reopening or signal inspection.

Raw recorded scheduled/kill/heal ordering, exact 30-second cadence, faults,
per-invocation enabling/observed timestamps and all terminal/progress quantiles
verify against the bound job log. Worst terminal/progress per-type p99:
18.295491603/12.456790034 seconds under the original distributed-verification 30-second
fault gates. Both clocks start at enabling events, including outage time.
All terminal timestamps satisfy the five-minute post-final-heal deadline.
All three independently rebuilt Start/Signal/Await models give exact `Ok`
for 36336 operations, with 45 dependencies matching executed Git.

The complete archive preserves original ZIP, expanded raw evidence, actual model
executable, sources/build information, metadata/logs and reviewer scripts.
`manifest.json` records all member hashes and ordered archive parts. Original
inputs, every member, parts and concatenated archive SHA256 are read back and
verified by the preserved producer. Concatenate parts in numeric order, verify
hashes and extract into a fresh directory. `independent-review.json` inside the
archive records acceptance and scope.

Native workload executables and physical stores were not uploaded. Final
integrity/drain assertions retain executed named-test scope. This qualifies
these executed-source shards; full cluster200/current-main matrices, million-
timer physical drain and actual 24-hour full-matrix qualification remain open.
