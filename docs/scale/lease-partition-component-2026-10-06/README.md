# Bare lease KV partition component

Diagnostic reduction of the reproduced seed6 minority lease-store stall. Fresh three-process NATS2.15 fixture, single R3 file-backed `WF_LEASE` KV with history1, production12sTTL and1mLimitMarkerTTL. Public stream-leader stepdown placement admits node2as current leader. Six writer goroutines sharing a majority-only client perform Put/CASUpdate/CASDelete cycles over192keys with50ms cycle pacing. After30s warm traffic, node2is cut for10s; an actual KV write must acknowledge on the majority while routes are4/4/0. The unchanged35s whole-cut reference bound observes route8/8/8 and replica-current recovery, with local publicjsz snapshots.

No SDK workflow, journal, worker or reconciler executes. Result `recovered=false` identifies a component reproduction; exit0 means the experiment completed and its controls were exercised, not a native matrix pass. `recovered=true` is a non-reproduction at this selected traffic profile. Both outcomes remain separate from full200/24h gates and causalTier1 reproduction.

Preparation: helper race compile passed. Actual retained diagnostic remains pending.

    python3 scripts/run-lease-partition-component.py --root /tmp/js-wf-lease-partition-component-20261006

Initial setup attempt at698634a expired before bucket creation/observations; no partition or recovery result. Full failure is retained under `initial-setup-failure`. API-readiness attempts and bucket setup now have explicit short bounds and stage labels, and the majority-probe request deadline is strictly the ten-second isolation end. Whole120s helper and35s recovery reference remain.

## Verified fixed-key control

Executed46772ba recovered after14.016s wholecut /4.015s after routes heal.4200complete cycles acknowledged,60transactions errored, and an acknowledged majority-cut probe hasrevision9721. Final local snapshots on all three servers showlast_seq12613; the current lease leader reports both replicas current. Independent review binds1948 Git/current/retained/before/after sources,1494external inputs and all3actual NATS binaries equal to the original matrix byteSHA24759ea... . R3/12sTTL/history1/file configuration is read back. Complete3523member archive verified. [Control evidence](fixed-key-control/). This profile is a non-reproduction, not a matrix acceptance or cause.

A selectable fresh-key profile uses a new subject every cycle so expiry and subject retirement occur while traffic continues. The fixed192-key control refreshes its heads well within the12sTTL; this differs from the original matrix’s distinct invocation identities. Fresh profile preserves all other actor pacing/configuration/topology/fault timing. Actual fresh-key run remains pending.

    python3 scripts/run-lease-partition-component.py --root /tmp/js-wf-lease-partition-component-fresh-20261006 --key-profile fresh
