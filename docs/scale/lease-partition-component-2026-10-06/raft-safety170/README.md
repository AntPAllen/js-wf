# Original 170 Raft controls: failed comparison

Both original source-bound race executables ran every 170 pinned upstream `TestNRG` case once, at 2CPU/2GiB, with the original20m timeout. Orchestrator source7a48413; compiled NATS source/fixtures f090279. Upstream:169passes/onefailure. Strict candidate:166passes/fourfailures, including two race reports. Neither profile qualifies. No test was skipped.

The candidate breaks `TestNRGNewEntriesFromOldLeaderResetsWALDuringCatchup`: a valid contiguous modern entry from the completed catch-up subscription is discarded (index remains2 instead of3). The strict guard needs a bounded exception for a contiguous entry from the known current-term leader after catch-up completes. This does not permit a stale callback to create or replace catch-up state.

Two race reports identify unlocked `addPeer` calls in upstream test setup while asynchronous account-status handlers read the peer map. `addPeer` explicitly requires its lock. The membership-removal failure is retained without assigning a confirmed cause. Upstream separately fails `TestNRGCandidateDontStepdownDueToLeaderOfPreviousTerm`.

The original collector also rejected a live cleanup launcher whose command text contained the fixture path. Native failures preceded that secondary collection error. A subsequent closed storage capture preserves the complete unchanged root and original error; it does not repair native qualification. The independent review checks actual SDK argv/hash/birth/cwd/environment, all170 case names and native outcomes, pinned selected Git sources, parent compiled dependencies, unchanged module source, parent S3 proof links, and every complete archive member.

The complete archive is stored by content hash in S3; its inventory/hash/readback receipt are in Git. Restore to a fresh destination before native inspection. The original failed campaign, default official NATS dependency and all full-matrix/24h gates remain unchanged.
