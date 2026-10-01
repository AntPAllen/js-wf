# Offline continuation replay — October 1, 2026

`wf.ReplayWithContinuations` replays the full logical history through named
functions. Every checkpoint is rebuilt and verified before restoration; it does
not trust a frame in place of auditing the prefix. Historical epochs, panic
counts and signal cursors bind each boundary. Buffered signals survive restore,
SDK positions remain absolute, and recorded effects never rerun. Completed
return values are checked against inline or hash-verified object outcomes.

## Scope

- SDK controls cross two boundaries with a later signal and panic attempt,
  verify original locals/state, replay all steps and stop at each recorded
  continuation suspension without invoking its future function.
- Missing objects, unknown stages (before user code), changed locals, changed
  rebuilt state, malformed suspension, incomplete checkpoint, stage panic,
  terminal mismatch and missing/corrupt terminal objects are checked.
- A compiled production overlay removes checkpoint rebuilt-hash/byte
  verification. An injected SDK state-application defect is then concealed by
  restoring the trusted frame, and the dedicated control fails. No overlay is
  applied to the worktree. This is not a compile-failure negative control.
- Every existing seventeen-mode integrated worker schedule now audits its full
  retained journal offline, without another effect callback. No modeled
  transport event was added; pinned seed 42 must remain byte-identical.
- A real R3 three-node graceful worker-replacement contract reconstructs its
  archive plus live journal, loads its two frame objects, replays 2,018 SDK
  entries across both boundaries, compares the immutable result and verifies
  that the two recorded effects did not rerun.

## Limits

This does not reenact individual historical worker deliveries or external
cancellation/panic timing. Failed/pending histories report replayed errors/waits.
Worker SIGKILL and all crash-cut repair, retirement/reuse, integrated runtime
semantics/limits and GC proofs, and final-source release matrix/soak remain open.
The independent mixed seed 65 latency miss is unchanged. Online GC is unsupported.

Final logs and source checksums below describe this proof's exact scope.

## Final evidence

- `focused-race.log`: all five focused replay controls passed in 1.057 s.
- `sdk-race.log`: full final SDK race suite passed in 12.882 s.
- `model-100k.log`: 100,000 schedules and choices, 21,347,572 transport events,
  virtual maximum 1,000 ms, 113.826 s Go test time. All seventeen worker modes
  verify full offline history and terminal value with no additional effect.
- `model-race.log`: worker replay campaign and existing pinned corpus passed
  under race in 23.178 s; exact first-ten and cross-process seed-42 checks pass.
- `full-sim.log`: complete final simulator suite passed in 85.988 s.
- `real-race.log`: R3 three-node graceful replacement, raw-state integrity and
  full offline archived-history audit passed in 25.889 s; two frames/boundaries,
  2,018 SDK entries consumed and no extra effect. Final worker read one frame
  and zero archived prefixes before the separate full audit.
- `trust-frame-mutation.log`: compiled source overlay fails the rebuilt-state
  defect control with err=nil and middle=1, demonstrating concealed divergence.
- `vet.log`: vet exited zero with no diagnostics.
