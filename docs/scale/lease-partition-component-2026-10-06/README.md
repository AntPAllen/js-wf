# Bare lease KV partition component

Diagnostic reduction of the reproduced seed6 minority lease-store stall. Fresh three-process NATS2.15 fixture, single R3 file-backed `WF_LEASE` KV with history1, production12sTTL and1mLimitMarkerTTL. Public stream-leader stepdown placement admits node2as current leader. Six clients on majority-only URLs perform Put/CASUpdate/CASDelete cycles over192keys with50ms cycle pacing. After30s warm traffic, node2is cut for10s; an actual KV write must acknowledge on the majority while routes are4/4/0. The unchanged35s whole-cut reference bound observes route8/8/8 and replica-current recovery, with local publicjsz snapshots.

No SDK workflow, journal, worker or reconciler executes. Result `recovered=false` identifies a component reproduction; exit0 means the experiment completed and its controls were exercised, not a native matrix pass. `recovered=true` is a non-reproduction at this selected traffic profile. Both outcomes remain separate from full200/24h gates and causalTier1 reproduction.

Preparation: helper race compile passed. Actual retained diagnostic remains pending.

    python3 scripts/run-lease-partition-component.py --root /tmp/js-wf-lease-partition-component-20261006
