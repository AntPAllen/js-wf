# Delivered native timer sources in Tier1

The old model hid a native source immediately after target delivery, making
retained delivered sources impossible to represent. Source presence is now
independent of delivery: an opt-in transport fault retains the delivered source,
and another restores it after delivery without replaying the target. These are
explicit modeled boundaries, not an explanation of NATS file-store behavior.
Default behavior preserves all older traces.

The new119th workload executes production timer publication and both TimerScan
and SuspendedScan. Across24 combinations it covers retention on delivery or
restoration after delivery, normal/drop-before-commit/lost-delete-reply cleanup,
Completed/Failed terminal journals, and both scanners. Active journals preserve
the delivered source. Dry runs do not delete it. Terminal reconciliation deletes
only its observed generation/sequence while a newer generation remains present.
Repeated scans and virtual-time advance do not replay the retired target.
This qualifies runtime reconciliation with terminal journal evidence; it does
not substitute a receipt for that evidence or repair the failed collector run.

The focused workload passes1,000 seeds under race and replays its first ten
traces exactly. Seed42 is pinned and separately replayed from disk. Two actual
compiled overlays fail semantically: restoring the old delivered-source absence
assumption gives `lost active timer`; removing the production generation guard
gives an incorrect dry-run removal count. Neither is a compiler/global timeout
failure. Exact overlays, logs and source hashes are preserved.

Full current-worktree1,000 validation passes163 top-level tests with only the two
allowed trace-only skips,265 pins, and all119 workloads covering exact1..1000:
119,000 bodies,121,034 schedules,1,794,015 choices,26,910,977 transport events.
Compiled test inventory, source AST workload inventory and regression inventory
are checked by the suite guard. The report names parent8b2f267; the recorded
modified-source hashes identify the tested additions in this commit. Simulator
vet passes. The initial prototype duplicate-type compiler failure is retained
as rejected fixture evidence; final positive/control runs follow its correction.

`originals.tar.gz` preserves every file from the completed local evidence root.
Every member is SHA256-compared on readback and listed in `manifest.json`.
Current119-workload100k qualification remains required; the running118-workload
campaign retains its exact older source scope. Full matrix/soak gates remain open.
