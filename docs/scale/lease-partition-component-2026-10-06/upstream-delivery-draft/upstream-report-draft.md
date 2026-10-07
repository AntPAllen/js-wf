# Draft: obsolete modern catchup callbacks can recreate or cancel catchup state

This is a local submission draft. No upstream issue, comment or pull request has been posted.

## Observed defect

With NATS server v2.15.0, a queued callback from an unsubscribed catchup inbox can still enter `processAppendEntry`. The obsolete-subscription guard applies to legacy entries or entries behind the local index, allowing modern entries ahead of the local index through. A callback from a canceled subscription can recreate catchup; a callback from a superseded subscription can cancel the active replacement. The callback carries the former catchup progress reply inbox. Negative catchup responses sent there are ignored by the progress handler, leaving recovery to retry timeouts.

A synchronous in-memory Raft fixture isolates the callback-state defect without network timing. It admits real catchup, retains its subscription, cancels or replaces that subscription, then invokes the queued callback directly. Upstream passes both legacy cases and fails both modern cases. The complete original failed SDK seed-6 lease WAL separately confirms expiry-generated divergent entries and majority/minority term conflicts. This is direct server state coverage, separate from the workflow seeded transport model.

## Proposed bounded change

Reject callbacks whose subscription is no longer the active catchup subscription. Preserve valid trailing modern entries after catchup completes when the explicit current leader/term and previous term/index match the local log. That bounded exception cannot trigger a new catchup request or reset the log from an obsolete progress inbox. Active replacement catchup continues to reject callbacks from the old subscription. Replay (`sub == nil`) and ordinary append-entry delivery retain their existing paths.

An earlier unconditional obsolete-subscription rejection broke the unchanged `TestNRGNewEntriesFromOldLeaderResetsWALDuringCatchup` control. That strict candidate is not proposed here. The patch includes the four original callback cases plus eight trailing-entry variants: valid contiguous, ahead, behind, wrong previous term, wrong leader, higher leader term, older leader term and legacy. All twelve pass with the bounded candidate, as do eleven unchanged upstream catchup controls, under race.

## Real-cluster evidence

Production lease settings are retained: R3/file storage, MaxAge 12 seconds and LimitMarkerTTL one minute. Six writers use fresh subjects and acknowledge Put/CASUpdate/CASDelete transactions while one peer is route-isolated for ten seconds. The original whole-cut replica-current bound remains 35 seconds.

The bounded candidate acknowledges 5,877 complete cycles and recovers all three replicas in 30.033 seconds from cut / 20.031 seconds from heal. It repairs 785 divergent WAL entries over 18.779 seconds. The original normal ten-minute SDK seed-6 partition case with that exact server binary also passes all nineteen cuts, slowest 21.754 seconds, 1,680 invocations and three independent operation-history models. These are individual experiments, not a recovery distribution or complete protocol proof.

## Broader safety scope

The earlier full 170-case upstream/strict comparison failed: upstream 169 passes; strict candidate 166 passes. The strict candidate's trailing-entry regression prompted the bounded change. Two race reports identify unlocked peer-map mutations in original test setup; those native failures remain failures, and the membership-change failure is unresolved. The refined original 170-case comparison is now complete: upstream passes all 170; the bounded candidate passes 169 and fails only `TestNRGEvictPeers` with the same unlocked test-setup peer-map race. The original trailing-entry regression and membership-change test pass. This refined full-suite native verdict remains failed. A separate test-only correction adds eight lock/unlock pairs around seventeen peer additions in the two tests with observed race reports; it changes no assertions, cases or timing. At `bbf33a0`, upstream and bounded race binaries both pass the two corrected tests and their ten original subcases, with no skips or race reports. Independent review proves the exact 17 additions are the only setup change and verifies complete source/binary/store preservation. A full 170-case run with the setup correction remains separate.

The twelve direct cases establish follower callback behavior; they do not exhaust all candidate/leader transitions or NATS disk/Raft behavior. Full native workflow matrices, official dependency adoption and 24-hour acceptance remain separate gates.

## Reproduction and evidence

In the consumer repository, `scripts/run-raft-catchup-contiguous-controls.py --root ABSOLUTE_FRESH_DIRECTORY` compares upstream, strict and bounded race binaries with the original module/dependency source inventories. `scripts/run-lease-partition-component.py --root ABSOLUTE_FRESH_DIRECTORY --key-profile fresh --raft-debug --server-profile obsolete-catchup-contiguous-candidate` selects the real production-settings component explicitly. The bounded full-suite profile is `scripts/run-raft-catchup-safety-controls.py --root ABSOLUTE_FRESH_DIRECTORY --parent-root RETAINED_DIRECT_CONTROL_ROOT --guard-variant contiguous`.

The standalone proposed patch applies to the pinned v2.15.0 Raft source and the official RC.2 commit `d564fd6982a44cc47c4228b12f7a9b6c9f722a8c`. RC.2 was inspected and patch-checked only, never compiled or run. Native evidence uses the retained consumer module file, selected compiler/dependencies and source-bound binaries; a standalone upstream-checkout test command has not been independently qualified.

Canonical records in `docs/scale/lease-partition-component-2026-10-06/`: `raft-callback-regression/`, `matrix-seed6-offline-wal-review/`, `raft-safety170/`, `raft-contiguous-controls/`, `contiguous-component/`, and `contiguous-native-seed6/`, and `contiguous-safety170/`. Each qualified scope has a complete content-addressed S3 archive, inventory and full-body readback receipt. Failed scopes remain preserved. The default dependency remains official v2.15.0.

## Complete peer-locked comparison

At b33e795, both actual race binaries run all170 original tests with only the qualified eight lock pairs. Upstream170passes; contiguous169passes/one TestNRGCheckpointInstallSnapshotAbortDuringWrite/RemoveOrphan failure: writer returned before dios refill: <nil>. No race or skip. The original drain consumes only currently available server I/O permits; a late in-flight release is a source-level hypothesis requiring deterministic controls. Native historical cause remains untraced; this full-suite gate remains failed. See ../peer-locked-safety170/.

## Deterministic snapshot-fixture late-permit control

At cb48979, both stock/refined original drain variants fail both original snapshot subcases under the same controlled late permit return (4095/4096 held). Both variants reserving all4096 permits pass with and without the injection. Six actual race/count1/3m/2CPU/2GiB binaries, all original assertions/sleeps/deadlines retained, no races/skips; complete independent source/native/archive review. snapshot-test-reservation.patch is a test-only correction and applies to exact official v2.15.0. Historical full170 cause remains untraced; no production change/default adoption. Combined peer-locking/reservation/full170 remains pending. Full proof: ../snapshot-reservation-controls/.
