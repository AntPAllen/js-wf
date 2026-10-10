# Separate compaction intent lifetime — 2026-10-10

GraphConfig and NativeGraphConfig now accept `CompactionIntentTTL`. Zero inherits
the normalized `IntentTTL` (default 60 seconds); negative values reject admission.
Ordinary append grants still use IntentTTL. Prefix compaction's initial expiry,
automatic one-third-remaining renewal trigger, and requested renewal expiry use
CompactionIntentTTL. Native opening forwards it in both authority modes, as does
the private continuation-limit profiling adapter.

This makes lifetime budgeting explicit without selecting a new production
default. Longer lifetimes retain abandoned grants for longer and can delay
collection after an abandoned compaction. Deployments must use the same budget
across writers/recovery adapters. Saved descriptors retain their original expiry;
new configuration cannot revive expired authority. Worker leases, request bounds,
source-head fencing, and independent final verification are unchanged.

## Evidence

- Eight JSON/protobuf lifetime cases cover default, inheritance, longer and
  shorter compaction budgets. They inspect append grant expiries, initial staging
  expiry, exact automatic renewal threshold and target, and expired stored input.
- Negative direct/native configuration rejects. A real R1 server verifies
  native forwarding and inheritance in legacy and indexed namespaces.
- Forty binding/renewal cases and 36 stored CAS/recovery cases pass with the
  new controls in race 23.701 seconds. Restored lifetime/native selection passes
  race 2.064 seconds after a deliberate mutation.
- Replacing initial compaction expiry with append TTL fails four lifetime cases.
  The source is restored exactly before the restored controls.
- R1/R3 indexed fresh-worker recovery passes on restored source (race 17.589
  seconds); all 853
  regression pins pass (10.026 seconds).
- The seeded grant-cost experiment adds a 60-minute-lifetime case, triggered
  with 20 minutes remaining. All 100,020 grants renew at assumed 1 ms per operation
  in 11m42.104752496s of model time (race wall time 70.742 seconds).

The last result demonstrates a budget that works under assumed costs, not native
latency. These are grants on a 12-record source, not 100,000 journal entries.
Native total cost, interruption during a large renewal, and actual full-entry
qualification remain open. The short-lifetime counterexamples remain unchanged.

An initial test incorrectly expected raw ResumeCheckpointCompaction to enforce
the clock at reconstruction. Its eight failures are retained in
development-raw-resume-expectation.log and excluded. The revised test exercises
ResumeStoredCheckpointCompaction, which performs that expiry check. Raw recovery
does not grant an expiry extension; renewal continues to reject expired input.
The first worker run overlapped mutation compilation and is retained separately;
only the restored rerun is accepted.

## Reproduce

```sh
go test -race ./journal -run '^TestGraphCompaction(SeparateIntentLifetime|NegativeIntentLifetime|CheckpointBindingRenewalAndResumption|StoredCheckpointCASAndRecovery)$' -count=1 -v
go test -race ./journal -run '^TestNativeGraphCompactionLifetimeForwarding$' -count=1 -v
go test -race ./internal/graphpublication -run '^TestGraphCompactionOwnedRenewalCost/seed42/owned100000-1ms-60min$' -count=1 -v
go test -race ./worker -run '^TestNativeGraphIndexedStoredRenewalFreshWorkerRecovery$' -count=1 -v
go test ./sim -run '^TestPinnedRegressionCorpus$' -count=1 -v
python3 docs/scale/graph-compaction-lifetime-2026-10-10/review.py
```

Next: measure total native renewal cost with real owned grants and interruption,
using an explicit lifetime budget and bounded worker batches. Then qualify the
original 100,000-entry workload. Admission and collection remain disabled.
