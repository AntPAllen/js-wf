# Deterministic checkpoint compaction — 2026-10-09

## Shared model and discovered defect

The new `graph_checkpoint_compaction` Tier-1 family drives the actual journal Start, checkpoint publication/confirmation and compaction APIs over the seeded ownership transport. It has fourteen modes: normal JSON, protobuf, definite/confirmed/unconfirmed compaction CAS replies, append/reader/collector/retirement races, definite or ambiguous payload upload, and definite/confirmed/unconfirmed checkpoint reader release. Generated transport events and scheduler decisions must replay exactly. Each seed selects one mode; this is 14 directed fault scenarios, not arbitrary concurrent fault permutations.

The model found that unconfirmed checkpoint reader release returned an unclassified raw transport error. Checkpoint confirmation now wraps it as `ErrUnknown`, preserves the underlying cause and stops before compaction. The seed 3 original failure log and partial trace remain preserved. No uncertain confirmation is treated as successful.

## Assertions

The test independently inspects the actual application offset after the returned decision. Confirmed lost ACK and unconfirmed committed compaction must have the new offset; definite drop, failed payload preparation, racing heads and unconfirmed reader release must leave compaction unpublished. A dropped reader release retains its durable pin. Fresh retries validate current authority, preserve every original logical record, retain a readable checkpoint through collection and end with zero physical objects after retirement. Retirement races additionally preserve a pinned old history while rejecting new active views. Both encoded entry formats run through the same ownership path.

## Executed evidence

Final normal passes 1,000 contiguous schedules and exact replay in **91.930s**, covering all 14 modes and 1,666,258 generated transport events. An earlier 1,000-seed run before the additional actual-offset assertions passes **45.174s**. The first race run stopped at Go's default ten-minute watchdog (**600.026s**) after reporting 900 completed schedules; it is a failed incomplete campaign. A fresh run of the unchanged 1,000 schedules uses a 30-minute test-process watchdog and remains running. Its live `final-race.log` is deliberately excluded from this commit; the review records its session handle. Production/publication/recovery gates are unchanged. No race 1,000 acceptance is claimed.

Journal checkpoint index/archive regressions pass normal **3.378s** and race **53.669s**. All 762 saved regression cases replay normal **6.526s** and race **61.777s**. Fourteen newly generated traces are copied byte-for-byte into the shared corpus; all 748 prior hashes remain unchanged. AST inventory now finds 149 seeded families. Successful pinned replay is not complete 149-family qualification.

The first final normal attempt could not save pins because Go tests run from the package directory and the output path was relative. Its failed output is retained; the corrected command uses an absolute output directory. It did not fail a model invariant. Original generated pin copies are removed after byte equality with the corpus is verified.

Commands: `SIM_COVERAGE_SUMMARY=1 SIM_PROGRESS=1 go test [ -race ] ./sim -run '^TestSeededGraphCheckpointCompactionReplay$' -count=1 -v`; pin generation adds an absolute `SIM_GRAPH_COMPACTION_ROOT`; corpus normal/race uses `-run '^TestPinnedRegressionCorpus$'`; selected journal normal/race uses `-run '^(TestGraphCheckpointIndex.*|TestGraphCheckpointArchive.*)$'`. Source manifests, inventory, prior/new pin ledgers, failed artifacts and executed review accompany this development evidence. No frozen independent qualification is claimed.

## Remaining requirements

Complete current 149-family normal/race and extended 10000/100000 qualification; multi-fault permutations and arbitrary interleavings; production-worker lease/Signal/Start/retirement/collection cut recovery; malformed v6 cursors, import and deployment compatibility; native publication uncertainty, OS process/storage/power loss and scale/resource limits; offline/history adoption and public admission. Every original broader native matrix/24h/million-drain/dependency/default-adoption/release gate remains open. Public continuation admission and production online collection remain disabled. Frozen 148-family/748-pin qualification excludes this addition.
