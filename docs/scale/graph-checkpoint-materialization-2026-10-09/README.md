# Canonical checkpoint promise payload ownership

The worker checkpoint completion append now decodes its staged owned frame and
adds every referenced promise outcome payload to that completion's owned edges.
The source must already be in the delivery's verified reference map with the
exact declared hash; staged matching bytes cannot grant source authority. The
existing graph publication protocol checks the pinned source ownership while
publishing the frame and copied payload edges in one append. On success the
in-memory resolver moves those references to the new completion index.

Frame identity, anchor epoch/index, stage and locals hash are checked before
materialization. Ordinary completions retain their previous reference handling.
No legacy object store or child source is consulted to repair a missing edge.

## Executed development controls

- `final-materialization-race.log`: native R1 and R3/domain controls execute the
  production graph delivery append. Two promise aliases share one previously
  owned payload; the completion retains exactly two payload edges (frame and
  result), and the resolver records the completion index. Missing ownership,
  staged-but-unowned bytes, mismatched source hash and wrong frame epoch all
  fail before append. After publishing the checkpoint pointer, the three
  earlier encoded journal bodies become unreadable. An explicit old-prefix
  payload read fails; both the current resolver and a fresh pinned view read
  the result through the completion edge with zero blocked-prefix reads.
  Indexed frame lookup also succeeds without those earlier bodies.
- `initial-race.log`: the preceding ownership controls and existing production
  SDK initial/named stage flow and actual partition-runner controls pass in
  both native configurations under the race detector. Handler/effect/result
  counts, terminal duplicate and zero legacy writes remain unchanged. This
  initial materialization run precedes the added staged-unowned negative case;
  the final ownership run includes it.

`executed-review.py` verifies required terminal passes and observes current input
hashes. These are development controls, not frozen complete/extended simulation
qualification. The fixture promise outcomes verify payload ownership transfer;
they do not represent a live child workflow or qualify child failure cuts.

## Remaining scope

The worker still loads the full graph prefix and reference map on delivery.
Bounded resume must reconstruct references directly from the checkpoint-owned
edges and suffix, preserve child/signal provenance and absolute append indices,
and reject unresolved references. Archival prefix compaction and collection
survival through physical prefix removal are not proven here. This transfers
promise result edges only; it does not establish all pending-child or frame
materialization contracts.

Public graph continuation admission, full kill/limit/audit/offline/import/v5
CLI/deployment integration and every original current-full/extended/runtime/
native/scale/actual24h/physical-drain/adoption/release gate remain open.
