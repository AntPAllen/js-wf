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

## Production-TTL fresh-key stall repeats — 2026-10-06

Second fresh fixture executeddd50e7b: production12sTTL acknowledges6440 complete cycles/60 errors and majority-cut probe11935. Routes heal at10.000s and allfinal peer views show8/8/8; minority remains non-current at35.001s whole-cut. Current leader reportslag6660, majority local heads23998 versus minority11736, with repeated snapshot/peerstate catch-up warnings. Independent review binds1954 source files/1494 dependencies, the helper and all3actual original-matrix NATS binaries, exact configuration/routes/local identities/state and all3529complete archive members. [Repeat evidence](fresh-key-repeat/).

Two production-TTL fresh-key runs now reproduce the component symptom, while one fixed192-key and one fresh-key MaxAge0 control recover within the original bound. This narrows a practical diagnostic path to expiry/subject-retirement interactions; it still does not establish causal server protocol behavior or a Tier1 reproduction. Next diagnosis should capture Raft debug term/index and catch-up decisions in this short component, retaining actual executed NATS bytes and preserving the original bound. Production configuration, matrix gate, original24h handle and hosted queued run remain unchanged.

## Raft debug capture

`--raft-debug` starts the same pinned NATS executable with its `-D` logging option on all three servers. Normal fixture constructors keep their existing logging. The component retains actual server argv/executable hashes and complete node logs. This changes diagnostic logging and may affect scheduling; it does not change the traffic, bucket settings, isolation or recovery bound. The helper race build passed before execution.

    python3 scripts/run-lease-partition-component.py --root /tmp/js-wf-lease-partition-component-raft-debug-20261006 --key-profile fresh --expiry-profile production --raft-debug

## Raft debug reproduction exposes conflicting terms — 2026-10-06

The fresh production-TTL component at `0ede3c4` reproduced the stall with `-D` enabled on all three captured NATS executables. It acknowledged 6,282 complete cycles and majority-cut probe 11,836, with 49 transaction errors. After routes healed, all peers reported eight routes, but the minority remained non-current at the unchanged 35-second cut boundary. The current leader reported lag 6,447; majority local sequence heads were 23,615 versus minority 11,835.

Independent review verifies 1,955 selected source files, 1,494 dependencies, actual helper/server bytes and argv, stable peer IDs, configuration, routes and local store state, plus every member of the complete 3,530-member archive. [Evidence and indexed log excerpts](raft-debug-reproduction/). The server bytes match the original failed matrix run.

Logs show the new leader repeatedly offering term-2 entries where the minority requests term-1 entries at the same indices. The minority logs WAL repair to term 1/index 3,550 and later backs down through indices to 3,537 while snapshot/peerstate warnings recur. This identifies the observed conflict-resolution path, but does not establish why those conflicting entries accumulated or prove a general protocol defect. The pinned file-store expiry code submits subject marker messages through the stream leader's clustered proposal path. A candidate explanation is expiry-generated proposals on the isolated former leader combined with slow rollback; this remains a hypothesis.

The next diagnostic retains production 12-second MaxAge but disables subject markers, to distinguish the marker/proposal path from plain TTL expiry. Production marker settings and lease TTL remain unchanged. No native matrix, causal Tier1, or full qualification gate is promoted.

`--marker-profile disabled-markers` sets only the diagnostic bucket's LimitMarkerTTL to zero. Default `production` keeps one minute. The actual stream configuration and profile are retained.

    python3 scripts/run-lease-partition-component.py --root /tmp/js-wf-lease-partition-component-no-markers-20261006 --key-profile fresh --expiry-profile production --marker-profile disabled-markers --raft-debug

## Production-TTL subject-marker control recovers — 2026-10-06

The fresh-key diagnostic at `dc0fa46` retains the production 12-second MaxAge and disables LimitMarkerTTL, with debug enabled. It acknowledges 4,194 complete cycles, 60 transaction errors and majority-cut probe 9,901. Recovery is 14.019 seconds after cut and 4.018 seconds after heal; all local stream heads equal 12,583 and the current leader reports both replicas current. Independent review binds 1,956 selected source files, 1,494 dependencies, the helper and three actual original-matrix NATS executables/argv, local state/routes and every member of the complete 3,528-member archive. [Control and log excerpts](marker-disabled-control/).

The control repairs one conflicting entry (leader term 3 versus minority term 2 at index 2,778, followed by repair to 2,777) and logs one snapshot warning before recovery. In the production-marker diagnostic, repair repeatedly backs through many conflicting term-1 entries. This supports the subject-marker/proposal path as a candidate source of the long recovery, while plain MaxAge expiry still operates in the successful control. The SDK's marker-disabled configuration also disables message TTL support and sets duplicate_window to 12 seconds instead of 120 seconds; those derived differences are retained and prevent a claim that every stream field was held equal. No message-ID deduplication or per-message TTL is requested by the diagnostic writers.

Production `WF_LEASE` keeps its required one-minute LimitMarkerTTL and 12-second lease TTL. The original plan explicitly includes LimitMarkerTTL; dropping it solely to pass a fault test would change the requested contract. A causal proof still needs the isolated minority's uncommitted entries tied to expiry marker proposals and a recovery fix that preserves the contract. Native matrices, causal Tier1 and full24h gates remain open.

## Offline WAL diagnosis

The read-only decoder checks the pinned NATS 2.15 uncompressed file-record format and HighwayHash checksums, then decodes append entries and stream operations. A separately built adapter calls the pinned server's own `decodeAppendEntry`, `decodeStreamMsg` and `decodeMsgDelete` functions for differential comparison. The adapter is added to a fresh source copy through a build overlay; it never changes the original server executable or module cache. No broker or file store is started.

The review restores the complete failure archive to a fresh directory, binds committed helper templates and selected dependency source inputs, compares all retained append records with the upstream decoder, rejects a checksum-corrupt control, and checks that the restored fixture and inputs remain unchanged. Initial exploratory decoding found expiry markers in the isolated minority's divergent tail; the bound review below must pass before promoting that result as complete evidence.

    python3 scripts/run-lease-raft-offline-review.py --root /tmp/js-wf-lease-raft-offline-bound-20261006

This reads a retained tail, rather than a complete pre-cut history. The majority WALs were compacted at shutdown. It does not prove a recovery fix, all possible protocol behavior or a causal Tier1 reproduction.

## Offline WAL confirms expiry-generated divergent minority tail — 2026-10-06

At recorded review source `640c5a9`, a complete verified copy of the production-marker debug failure was decoded without starting a broker or opening an original store. The minority retains 368 term-1 append records at WAL sequences 3,168–3,535, each carrying commit index 3,167 and the former leader's peer ID. Their 5,882 operations are exactly 2,933 `MaxAge` subject-marker writes and 2,949 stream delete operations. All record timestamps and all marker message timestamps fall strictly within the confirmed route-isolation interval. The shutdown log installs the minority snapshot at term 1/index 3,167. The marker headers contain `Nats-TTL: 1m0s` and subject rollup; the delete operations have `no_erase=true`.

Every file-record checksum passes. All 369 retained append records across the three copied WALs match the pinned NATS server's own append, stream-message and delete decoders. The reference adapter is compiled against an unchanged fresh copy of the server source; 91 selected server files match the captured failure inputs. Independent review also binds 1,958 Git/current/retained/before/after selected sources, 1,452 decoder dependencies, binaries, unchanged copied fixture, a rejected corrupt-checksum control and all 7,566 full-archive members. [Complete offline evidence](offline-wal-review/). The full archive is 73,842,752 bytes, SHA256 `91bab3f1e294b856e56f5ea76f32bc26571618626b3e1e8ed40ca3e5faae046e`.

This confirms expiry-generated proposals in the isolated minority's divergent tail in the bare component. The detailed logs already show the new leader's term conflicts and slow rollback through that tail, while the marker-disabled TTL control repairs one conflict and recovers. This is concrete server-path evidence, rather than a workflow SDK invariant failure. The original matrix seed6 tail still needs the same direct inspection before assigning its complete causal verdict. No recovery fix, complete protocol correctness proof, causal Tier1 reproduction, native matrix or full24h acceptance is claimed. Production TTL and markers remain unchanged.

## Original seed-6 WAL review

The `matrix-seed6` case restores the complete original failed campaign archive, selects its recorded lease Raft group through each peer's store metadata, and applies the same checksum and upstream-decoder comparison to all retained records. It checks the minority's retained tail separately from its older committed history. Original server source files must match both the captured failure inputs and the reference decoder source copy. Timestamps are checked against the recorded cut and the ten-second isolation scheduled in the captured native test. The failed fault record has no successful `healed` timestamp; this is not presented as a recorded actual heal time.

    python3 scripts/run-lease-raft-offline-review.py --root /tmp/js-wf-matrix-seed6-offline-bound-20261006 --case matrix-seed6

This preserves the original failed native verdict and does not resume the seed, rerun the matrix or open any original media.

## Original matrix seed6 confirms expiry-generated divergent lease tail — 2026-10-06

The bound offline review at `8b1a2c8` restores the original failed seed6 archive (executed native source `300a36a`) into a fresh directory. All 3,546 retained lease append records match the pinned NATS decoders and pass file-record checksum checks. The minority retains nine term-1 records at WAL sequences 2,688–2,696, all carrying commit 2,687. Their operations are exactly 19 `MaxAge` marker writes and 32 `no_erase` stream deletes, written within the first 2.04 seconds after the recorded minority cut. All marker timestamps also lie within the scheduled ten-second isolation. The retained minority snapshot is named `snap.1.2687`.

Eight retained indices (2,689–2,696) overlap the majority WAL, with term 2 on the majority versus term 1 on the minority and different record bodies. This directly confirms the expiry-created divergent tail and term conflict in the original matrix failure, beyond the bare-component reproduction. It does not reconstruct records already discarded during rollback or compaction. The original failed fault record's `healed` timestamp is zero; no actual successful heal timestamp is inferred from it.

Independent review binds 1,960 selected sources, 1,452 decoder dependencies, 91 server files equal to the original captured failure inputs, unchanged copied media, both decoder binaries, the rejected corrupt-checksum control and every member of the complete 10,205-member archive. [Original-seed offline evidence](matrix-seed6-offline-wal-review/). Archive: 100,332,214 bytes, SHA256 `5b5c4b6f24a025f2e0791f33f402077144bf5a4e42599e9dcea78e0486999041`.

This confirms the observed failure mechanism on the server lease-replication path; it is not a demonstrated workflow SDK invariant failure. The next candidate investigation is obsolete modern catch-up callbacks entering catch-up state after their subscription is canceled, with requests then directed to a progress inbox that ignores negative responses. That state-machine hypothesis needs a deterministic upstream regression and a contract-preserving fix experiment before qualification. Production TTL and LimitMarkerTTL, the 35-second native recovery bound, the original failed200 campaign and the running24h handle remain unchanged. No native pass, causal Tier1 reproduction or fullmatrix acceptance is claimed.

## Direct server regression identifies obsolete catch-up callback defect — 2026-10-06

At executed `f090279`, a synchronous state-transition regression in the pinned NATS server package checks queued callbacks from canceled or replaced catch-up subscriptions. Upstream NATS passes the two legacy-message cases and fails both modern-message cases: a canceled callback creates fresh catch-up state, and a superseded callback cancels the replacement state. A source-overlay candidate rejects callbacks whose subscription is no longer the active catch-up subscription. It passes all four cases under the race detector. Both upstream and candidate also pass ten selected existing catch-up controls covering committed-entry preservation, term handling, rollback, progress windows and quorum accounting.

Independent review verifies exact subcase verdicts and all ten control names, four actual SDK executable/argv/start-time captures, unchanged 598-file upstream source copy, the single guard change, 1,963 selected Git/current/retained/before/after sources, 2,665 dependency inputs and every member of the complete 5,261-member archive. Go-generated testmain handling is documented in the evidence. [Regression and controls](raft-callback-regression/). Archive: 88,648,830 bytes, SHA256 `a439593180d8f1574de65d2b469c2a7d53e52c387c32f20b90ff6b38926d1c3c`. During review, importing the captured patcher created one extra Python bytecode cache file; the inventory guard caught it, no archived file changed, that reviewer-created file was removed with a retained hash record, and the complete inventory was reverified with bytecode writes disabled.

The candidate leaves replay, timers, WAL truncation/commit rules, lease TTL and marker configuration intact. Its rationale is that an obsolete catch-up callback carries a progress reply inbox that does not process negative catch-up requests. This links a reproducible server state defect to the previously observed slow-retry path, but does not yet prove that the candidate heals the real component or original ten-minute SDK seed6 case. These are NATS direct-state regressions with embedded fixture admission, not the requested seeded workflow Tier1 simulation. The candidate is an experimental overlay only; production dependency/server behavior remains unchanged. Next: exercise the real fresh-key production-TTL/marker component with a separately captured candidate server binary, preserving the 35-second bound, then the original SDK case if that passes.
