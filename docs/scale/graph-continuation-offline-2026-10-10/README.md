# SDK canonical continuation offline replay

New internal admission control: `TestNativeGraphContinuationOfflineReplay`.
R1/R3-domain × live v5/archive v6, fresh workers and lease heartbeats per SDK
delivery, state carried through two production checkpoints, a real canonical
signal suspension, completion and duplicate delivery. Archive cases require
physical absence of original prefix receipts after both checkpoint collections.
The production `wf` executable exports both suspended and completed bundles;
exports must match pinned owned snapshots and leave no readers. All native
servers stop before the separately compiled matching race CLI/plugin replays
both bundles using a dead NATS URL. Unknown stage and missing frame objects must
fail with their specific SDK errors. Offline callbacks must not write the effect
marker. Live handler counts must be initial=1, middle=1, finish=2, effects=1.

This is a new functional fixture with a 120s case watchdog, a 180s CLI/plugin
build watchdog and a 10m package watchdog. It changes no recovery latency gate,
public continuation admission or production collector setting. It does not
qualify import, rollout, general malformed histories, child replay, faulted
collection, the actual 100,000 entry boundary or broader original plan gates.
No acceptance is claimed until actual terminal results are independently reviewed.
