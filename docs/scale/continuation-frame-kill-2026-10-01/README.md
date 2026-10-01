# SIGKILL around checkpoint frame storage — October 1, 2026

The existing Linux publication-kill fixture now shares its contract with two
earlier real R3 cuts: before the frame Object Store Put and after that Put
acknowledges, both after checkpoint StepRequested is durable and before its
StepCompleted. A child worker is held at the exact cut by a test-only result
transport wrapper; the parent independently verifies journal and object state
before sending and attesting actual SIGKILL.

Both cuts have exactly seven journal records, with a finish_v1 checkpoint
request at index 6 and no runtime manifest. The prospective frame is verified
against invocation generation, anchor index 7, epoch, hash and SDK position 6.
Before Put the named object must not exist. After acknowledged Put another
pinned client must read precisely its expected bytes. WF_JRN has seven live
records and WF_SIG retains its one buffered signal at either cut.

The suspended scanner must not claim an incomplete checkpoint as a committed
continuation. It reenqueues zero candidates. A replacement pinned to another
node recovers through the original unacknowledged runs, replays the initial
handler and pending request, commits a frame under its higher epoch and returns
46. It executes no duplicate prefix effect. Buffered signal, state 23, locals
45, immutable cross-peer outcomes and the raw-state audit all pass. The entire
confirmed pre-kill journal prefix must remain byte-identical. Full staged
offline replay verifies 12 SDK entries with no effect callbacks.

In after_frame, the original confirmed object has no completion reference.
It remains byte-identical after replacement, and the published replacement
frame uses a distinct content-addressed name. With child dead and replacement
loop joined, quiescent collection must delete exactly that one orphan. The live
checkpoint still reads with identical bytes; later full reconstruction/offline
replay and peer reads succeed. This is quiescent collection, not online GC.

## Evidence

- `six-cuts-race.log`: both new cases and all four existing publication cases
  pass under race in 101.236 seconds. New recovery samples are 13.136 and
  13.132 seconds; each finishes with 16 logical entries under epoch 19 after
  epoch 1. Existing publication cases retain their archive-denying guards.
- `effect-replay-mutation.log`: a compiled SDK overlay reruns a RunOnce callback
  despite its recorded completion. The before_frame contract fails on two
  prefix effects in 16.267 seconds. This is a semantic failure after recovery,
  not a compile failure or a timeout. `effect-replay.patch` retains the edit.
- `vet.log`: integration vet exits zero without diagnostics.
- `SOURCE_SHA256SUMS`: tested fixture, production paths and shared guard.
- `production-boundary-ci.log`: both hosted raw-journal and continuation
  100,000-entry jobs at 02d5d19 passed in run 36810716129.

No production runtime source changes. These are individual process-death and
orphan-lifecycle proofs, not a release p99 distribution. Suspension/handoff and
combined/server fault cuts, seeded process-death/GC coverage and full final-source
matrix/24-hour soak remain open. Independent mixed seed 12 at 7e414b2 failed the
latency gate; its retained proof is separate. Seed 65 and online GC remain open.
Existing million timers and full Tier 1 runs continue without restart.
