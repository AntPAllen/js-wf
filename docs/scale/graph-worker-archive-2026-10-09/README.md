# Worker compaction and execution after collection — 2026-10-09

## Implementation

Only explicit new v6 archive stores invoke compaction. The worker first confirms/publishes the checkpoint and durably appends its continuation suspension. It then calls `CompactCheckpoint` with the new exact tail before renewal/enqueue. The existing delivery retains its original reader until cleanup and performs no further append after relocation. The next delivery opens the new cursor and restores owned references from its bounded checkpoint/suffix. Errors propagate without certifying a successful handoff. Default v5 behavior and public continuation admission remain unchanged; collection remains disabled in production.

## Native controls

R1 and R3-domain scenarios each exercise state, buffered ordinary signals, resolved child promises and buffered child results. Initial/next/finish are actual SDK handlers through production worker execution; child invocation/source and Signal intake are real native operations. Each workflow publishes two checkpoints, and the worker automatically compacts both to a three-record live suffix while retaining full logical history.

Between independent stage deliveries, the test runs the native collector, verifies the v6 offset/populations and exactly one new inspection pin, audits every logical record, and checks that original encoded-entry receipts with the same hashes but different physical identities are unreadable. Positive removed-receipt counts are logged at every boundary. Closing the inspection pin precedes the next delivery.

Both child variants retire the child, collect again and require its original terminal payload/entry receipts to be unreadable before finishing the parent. The parent still resolves its independently owned 700,000-byte result from a resolved promise frame or buffered child signal. Initial/next/finish call counts are each one; parent effects are two, child calls one, final result `43`, one terminal record, absolute journal indices and zero legacy journal records. Full logical audit is checked afterward.

## Execution and evidence

Normal eight-case archive execution passes **40.928s**; the first isolated R1 state case also passes. Existing v5 SDK and actual partition-flow regression groups pass race **50.529s**. The first complete archive race campaign remains failed at **263.948s**: both state cases and all four child cases pass, while both signal cases fail at their 30-second outer contexts. Only those two signal cases are rerun with the corrected archive fixture deadline. The corrected two-case Signal race run passes **59.753s** (R1 **24.48s**, R3 **34.23s**). All eight scenarios have passing race coverage across those two runs, with no skips. This does not certify a clean complete current-source race campaign; the executed review maps each case to its exact log.

A fixture compilation initially assumed a promise carried a child invocation sequence; the test now reads the actual canonical retirement identity before pinning it. The compile failure is preserved.

The first archive race campaign uses the previous ordinary SDK fixture's 30-second outer context for state/signals. R1 signals hit a context timeout after both collection/audit boundaries. Archive cases now use the existing child fixture's one-minute context, since they additionally run collection and complete history audits between stages. Production 15-second continuation publication, lease settings and recovery acceptance gates are unchanged. The original campaign remains preserved; its failure is not credited as a passing campaign. This is component development evidence, not frozen-source/full shared qualification.

Commands: native archive normal/race `go test [ -race ] ./worker -run '^TestNativeGraphContinuationArchiveCollection$' -count=1 -v`; existing v5 race `-run '^(TestNativeGraphContinuationSDKFlow|TestNativeGraphContinuationSDKPartitionFlow)$'`. Logs, source hashes and executed review accompany this report.

## Still required

Compaction uncertainty/lease/head/collector/retirement interleavings and shared seeded recovery integration; complete failure/limit/pending-child/state-projection proof; autonomous partition execution with collection; physical process/storage/power faults; resource/scale qualification; offline/history/import/deployment compatibility and public admission. Every original current-full/extended/native matrix/24h/million physical-drain/dependency/default-adoption/release gate remains open. The frozen 148-family/748-pin full normal/race qualification excludes this addition.

Only the test fixture deadline changed between the race campaigns; production and all other observed inputs remained unchanged. Separate before/after input manifests preserve that distinction.
