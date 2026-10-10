# Worker archive intent renewal — 2026-10-10

The worker now schedules journal compaction renewal when one third of the
configured IntentTTL remains. Scheduling uses the journal authority clock, after
an unconditional lease renewal. Each renewal scope batch also rechecks ownership.
Staging and verification freeze until all grants adopt the new expiry; private
verification progress survives. Expired intents cannot be revived. Default
IntentTTL remains 60 seconds; the whole handoff parent remains 15 seconds.
Renewal setup still enumerates the isolated namespace under its request context;
that enumeration is not a per-scope memory budget.

32 JSON/protobuf maintenance controls and seven existing archive-repair controls
pass under race (12.537s). New healthy cases advance the shared virtual clock
41 seconds during staging, record verification or node verification; successful
renewal is followed by a further 20 seconds, crossing the original expiry.
Each still requires all six staging and ten verification batches, independent
root retention checks, reader cleanup and next-delivery execution exactly once.
Cancellation/replacement after the first renewal batch leave retention at zero
and admit no next stage. Fixture lease TTL120 prevents the deliberate 41-second
advance from confusing intent renewal with owner expiry; replacement advances
past that lease separately. This is controlled time, not wall-clock latency.

Removing scheduling fails exactly the ten new cases. Exact mutant bytes are
saved compressed; production was restored before the positive worker run.
`sources.json` and compressed production files fix the tested source bytes.
`review.py` checks those hashes and terminal results. Common pins use a separate
restored-source run; development race-pin logs with overlapping writes are
excluded from acceptance.

Commands:

```sh
go test -race ./worker -run '^TestGraphContinuation(MaintenanceLeaseAndCancellation|RepairsArchiveBeforeStage)$' -count=1 -v
go test ./sim -run '^TestPinnedRegressionCorpus$' -count=1 -v
python3 docs/scale/graph-worker-intent-renewal-2026-10-10/review.py
```

This adds runtime renewal scheduling, not process recovery: the worker still
does not persist/resume descriptors. Public graph continuation admission remains
closed and production collection remains off. No actual100000-entry qualification,
new native scheduling qualification, full current campaign or original rollout
gate is claimed. Historical first-checkpoint/majority/soak failures remain open.
