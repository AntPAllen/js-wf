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
## Closed qualification

Corrected frozen source `85f2629afc393fa1a6de81f63b2219ca347557e2` passes
all four cases under race; package118.459s, actual child/supervisor exit0.
R1 live17.81s/archive27.90s, R3 live26.98s/archive38.21s. Both archive cases
physically reclaim six original entry receipts after the first checkpoint and
eight after the second. Each offline suspended replay has13 records and waits
on `signal:gate`; completed replay has18 records/result60. Both exported frames
retain state23 and locals7/30. All four cases require the precise unknown-stage
and missing-object errors, no effect marker and zero owned readers.

[Executed independent review](review.json) binds3,309 exact Git inputs, equal
before/after inventories, actual commands, race metadata and retained hashes
for the worker binary, packaged CLI and plugin. It separately inspects all12
export/negative-control files, frame hashes/identity/state and contiguous absolute
record indices/sequences. [Raw evidence](qualified/events.jsonl),
[closed state](corrected-state.json), [actual supervisor exit](supervisor-exit.txt)
and [reviewer](review-corrected.py) are retained. Binaries and native stores remain
at `/home/exedev/js-wf-offline-replay-corrected-20261010`.

The [initial failed run](failed-first/README.md) remains separate and unaccepted;
its JSON-formatting and relocated-receipt fixture assumptions were corrected
without changing production code. The independent complete156 race campaign at
d504a33 remains live and excludes later production export changes. This result
closes the named four-case healthy SDK canonical continuation offline boundary,
with the broader requirements above still open.
