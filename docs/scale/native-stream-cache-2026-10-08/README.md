# Shared native SDK stream cache guard

The direct blob adapter reproduces the same pinned nats.go v1.54.0 Stream.Info cache pointer race found in the graph adapter. An unguarded R1 test starts four actors sharing root/blob reads and authority/object census on the same live native Port. The race report is retained in `development-failure/` together with exact test source. The reproduction uses `GORACE=halt_on_error=1` to capture the first actual report without repeated trace output. It does not establish a server rollback/partition cause or change an old failed campaign verdict.

`internal/natsstream.Guard` now provides the shared implementation for both adapters' owned SDK stream handles. Individual Info, cached-info, GetMsg, GetLastMsgForSubject and Purge calls are protected; canceled/deadline waiters return before entering the SDK. Whole protocol operations and conditional publications remain concurrent at the server. The wrapped handle must not also be used outside the guard, and the exported wrapper only claims protection for the listed methods. It does not change stream configuration, envelope schemas, logical heads, generations, ownership or SDK dependencies.

The R1/R3 direct-adapter control runs four actors/eight rounds of simultaneous root/blob/read/census operations, checks live physical bytes after actors join, retires the root, then verifies zero objects/chunks. Existing graph six-actor publication/collection and waiting-context controls still apply. The first corrected focused race control passes; its earlier-source log is retained in `precursor/`. Frozen final package regression is pending at this commit. Both CI workflows include the shared helper's changes and run its control package.

This is shared adapter component work, not canonical runtime graph migration, reader retention, compaction/import, complete native history/deployment/domain/crash/power-loss/partition/scale qualification or any original full simulation/native/24h/million-drain/default-adoption/release gate.

## Final package regression and input timing

Atd6ef52f, complete packages pass normal/race: shared guard1 group0.024s/1.035s, direct blob adapter30.621s/52.258s, graph adapter31 groups36.519s/133.317s, under count1/5m per package/twoGoCPU/512MiB. Exact direct package groups and native concurrency logs are retained in `regression-review.json`. The guard's waiting-context test moved out of graph package, so the graph group count falls from32 to31 without coverage removal.

The first commit staged only file renames because an add command still named removed paths. The subsequent integration commit completed the exports/imports and CI changes. A pre-run Git inventory assertion failed against that intermediate commit, while the original normal regression proceeded on constant working source bytes. All70 selected files were checked against completed Git during the normal run and remained unchanged through both closed runs. This is standard local regression, not a pre-compilation/process/dependency admission claim. No original test was restarted due to an observation timeout.

The original full126 normal100k SDK2827904/user unit remains confirmed live at074bcfc under unchanged300m budget, with no restart or terminal acceptance. Its frozen source predates these adapter changes.
