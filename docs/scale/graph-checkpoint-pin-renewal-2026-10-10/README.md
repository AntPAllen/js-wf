# Checkpoint traversal reader renewal

ReadCheckpoint now calls the existing half-life RenewIfNeeded guard before indexed anchor/declaration reads, before frame payload loading, before prefix/suffix traversal and in its per-record callback. Renewal keeps the exact pinned forest and uses fresh authority/head checks. An already expired local reader still rejects before attempting renewal; no inactive snapshot is resurrected. A single stalled read can still exhaust its remaining lifetime and fail closed; this change does not promise recovery from arbitrary pauses.

A directed production GraphStore/in-memory transport control advances its private collection clock one second per entry payload read with PinTTL4 seconds. The read-budget rejection, pin cleanup, fresh retry publication and indexed confirmation complete across57 virtual seconds and28 actual reader renewals. An ordinary fixed-clock sibling passes too. Virtual time is injected synchronously; this is not native latency or a new seeded family. Archive transport cost is measured with clock advancement disabled so it is a separate experiment.

Disabling only the five checkpoint renewal calls makes the timed control fail on expired authority before reaching its expected injected read-budget boundary; the fixed-clock sibling passes. Raw exact mutant and output are retained. Existing checkpoint ownership/generation/frame/index race controls pass30.322s, including JSON/protobuf expiry cases. All853 unchanged saved regressions pass10.656s. These are development checks, not frozen source qualification.

## Remaining actual-cap work

The actual100000 native gate is still failed. Its15-second worker context covers first prefix verification/publication, suspension append, complete archive prepare/commit and dispatch. First verification reads all records before any prior checkpoint pointer; archive relocation and grant validation still grow with the retained prefix. This reader renewal fixes one lifetime issue but does not remove that work or the15-second deadline.

A bulk publication design must preserve generation/tail binding, original and relocated grants, independent receipts, archive physical reclamation before resume and the unchanged global terminal slot. It needs bounded native calls, live reader renewal, and safe intent lifetime handling during relocation. Renewal alone cannot extend prepared intents, and changing reader pins during a captured-head relocation can invalidate its original-head CAS. Longer wall-time alone cannot satisfy these constraints. A bounded resumable maintenance design or an equally verified prefix-proof approach needs explicit crash/unknown-outcome/collector controls before another multi-hour actual-cap rerun. Public continuation admission and production collection remain closed/off.

Running full158 race at0404fc0 and compaction qualifier at2de6dfa exclude this later change. Their results remain separate. All original plan requirements remain in scope.

Additional JSON/protobuf expiry controls pass under race in1.153s and verify that rejection leaves both the canonical root head and the expired pin expiry exactly unchanged. [Development review](development-review.json).
