# Bare lease KV partition component

Diagnostic reduction of the reproduced seed6 minority lease-store stall. Fresh three-process NATS2.15 fixture, single R3 file-backed `WF_LEASE` KV with history1, production12sTTL and1mLimitMarkerTTL. Public stream-leader stepdown placement admits node2as current leader. Six writer goroutines sharing a majority-only client perform Put/CASUpdate/CASDelete cycles over192keys with50ms cycle pacing. After30s warm traffic, node2is cut for10s; an actual KV write must acknowledge on the majority while routes are4/4/0. The unchanged35s whole-cut reference bound observes route8/8/8 and replica-current recovery, with local publicjsz snapshots.

No SDK workflow, journal, worker or reconciler executes. Result `recovered=false` identifies a component reproduction; exit0 means the experiment completed and its controls were exercised, not a native matrix pass. `recovered=true` is a non-reproduction at this selected traffic profile. Both outcomes remain separate from full200/24h gates and causalTier1 reproduction.

Preparation: helper race compile passed. Independently reviewed control and reproduction results are below.

    python3 scripts/run-lease-partition-component.py --root /tmp/js-wf-lease-partition-component-20261006

Initial setup attempt at698634a expired before bucket creation/observations; no partition or recovery result. Full failure is retained under `initial-setup-failure`. API-readiness attempts and bucket setup now have explicit short bounds and stage labels, and the majority-probe request deadline is strictly the ten-second isolation end. Whole120s helper and35s recovery reference remain.

## Verified fixed-key control

Executed46772ba recovered after14.016s wholecut /4.015s after routes heal.4200complete cycles acknowledged,60transactions errored, and an acknowledged majority-cut probe hasrevision9721. Final local snapshots on all three servers showlast_seq12613; the current lease leader reports both replicas current. Independent review binds1948 Git/current/retained/before/after sources,1494external inputs and all3actual NATS binaries equal to the original matrix byteSHA24759ea... . R3/12sTTL/history1/file configuration is read back. Complete3523member archive verified. [Control evidence](fixed-key-control/). This profile is a non-reproduction, not a matrix acceptance or cause.

A selectable fresh-key profile uses a new subject every cycle so expiry and subject retirement occur while traffic continues. The fixed192-key control refreshes its heads well within the12sTTL; this differs from the original matrix’s distinct invocation identities. Fresh profile preserves all other actor pacing/configuration/topology/fault timing. The actual fresh-key run reproduced the minority stall; see below.

    python3 scripts/run-lease-partition-component.py --root /tmp/js-wf-lease-partition-component-fresh-20261006 --key-profile fresh

## Fresh-key component reproduces minority catch-up stall — 2026-10-06

At recorded source4a13612, bare realR3 lease KV traffic using a fresh subject every cycle acknowledged6282 complete Put/CASUpdate/CASDelete cycles, with84 transaction errors. A majority write acknowledged revision11857 during the confirmed4/4/0 route cut. Routes healed after10.000s and final public views show8/8/8, but the minority remained non-current at the unchanged35.002s whole-cut boundary: majority local last_seq23426 versus minority11754, and leader-reported minoritylag6006. Repeated snapshot/peerstate catch-up warnings occur in the minority server log. The experiment exited0 with recovered=false; this means a valid component reproduction, not a passing native matrix.

Independent review binds1949 selected Git/current/retained/before/after source files,1494 dependency files, the helper executable and all3actual NATS binaries to the original matrix server bytes. R3/file/history1/production12sTTL configuration, stable per-peer IDs, route counts and local store state are checked. Complete3524-member archive has48,426,792 bytes and SHA256f08e4584c9bb843972aba9383529bfec7baee218203987fa957119b1833e9bb4. [Fresh-key evidence](fresh-key-reproduction/).

This reproduces the symptom without workflow SDK, worker, journal or reconciler execution. Fresh-subject traffic is a candidate trigger compared with the recovered192-key control; protocol cause remains unconfirmed. A diagnostic expiry-disabled control is the next discrimination. Production settings and original35s matrix gate remain unchanged; no causal Tier1 or fullmatrix qualification is claimed.

## Expiry control

`--expiry-profile disabled` sets only the diagnostic bucket's TTL to zero. Default `production` retains the production12sTTL. Fresh keys, Put/CASUpdate/CASDelete operations, actor count/pacing, history, marker setting, server binaries, topology and timing stay the same. This tests whether expiry is necessary for the observed stall at this traffic profile; one outcome cannot establish a protocol cause or general recovery guarantee. Configuration and selected profile are retained with each result.

    python3 scripts/run-lease-partition-component.py --root /tmp/js-wf-lease-partition-component-no-expiry-20261006 --key-profile fresh --expiry-profile disabled

## Fresh-key expiry-disabled control recovers — 2026-10-06

Executed2da7cb3 uses the same fresh subjects, six writer goroutines, Put/CASUpdate/CASDelete operations, pacing, R3/file/history1/1m marker setting, exact NATS bytes and cut/heal timing. Only diagnostic MaxAge is zero instead of12s. It acknowledges3978 complete cycles/54 errors and majority-cut probe9685. All3public local heads equal11935 and the actual leader reports both followers current. Recovery12.009s aftercut/2.008s afterheal. Independent review binds1953 selected source files,1494 dependencies and actual helper/3server executable captures; complete3529-member archive verified. [Control](expiry-disabled-control/).

This supports investigating expiry/subject-retirement interaction at this traffic profile; neither the single failed production-TTL run nor the single successful no-expiry control proves causation or reliability. Production leases remain12s. Repeat the fresh-key production-TTL component to check repeatability before introducing further controls. Native200/24h and causalTier1 gates remain open. Original24h process still active and rawbatch4009 reached7h57m35; hosted37500390198 has completed seed setup, with all200actual test jobs queued.
