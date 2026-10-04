# Independently accepted worker-clock seed 15

[Focused run 37166976657](https://github.com/AntPAllen/js-wf/actions/runs/37166976657),
job **111332551350**, completes successfully at exact
`c2e8c0122c15ef2d31ffa11c24ccd53c203a233f`. This qualifies only its requested
600-second worker-clock seed 15: **2,884 invocations, 31,915 journal entries,
19 periodic faults**, all ten checkpoint audits, final history/integrity/drain
assertions and terminal/progress type p99 **5.148231271/0.402368402 seconds**.
Named test/package take 927.04/928.082 seconds including setup and recovery.

Independent review regenerates every row/event/fencing report, confirms all
21 clock proofs and 105 actual broker clock messages for five worker identities,
and matches all **761** pre/post source inputs to executed Git. A separately
compiled and retained Go reviewer executes all three production history models:
**3,708 operations, all Ok**. All **45** actual local model/module inputs match
executed source before compilation and remain unchanged after review. The generic
Python review's scope explicitly excludes its own Porcupine rerun; the separate
`history-model-proof.json`, executable, helper and log establish that extra check.

## Checkpoint observations

Every checkpoint succeeds on its first attempt. Attempt durations rise from
2.783 seconds at batch 10 to **18.782 seconds at batch 100**; batch 90 takes
17.361 seconds. Exact nanosecond durations are in `summary.json`, with full raw
start/completion/deadline/partial-report records in the archive. The bounds remain
three 20-second attempts inside 60 seconds. This successful diagnostic does not
establish the cause of the earlier deadline failure, and does not clear that
failed shard or full parent **37164231641**. Full-row, 16×200 matrix and actual
24-hour requirements remain open.

## Archive layout correction and controls

This focused original-store archive puts fixture contents at its root. Range
archives and raw uploads use a nested `tier3-mixed-journal` directory. The original
reviewer rejects the flat original archive; the corrected reviewer accepts it
only for a single captured-profile seed with exact original-store artifact
identity. Missing nested raw/range inputs remain rejected. Thirteen combined
shard/full-matrix controls pass in 0.047 seconds; the substituted raw artifact
name is rejected. The initial layout rejection and initial mock-fixture failures
are retained alongside final successful controls. Original evidence is unchanged.

## Preservation limits

Original store artifact **11290857183** has a canonical tar of **108,320,313
bytes**, SHA256
`24005344b8929baca56ef51f88a713aa27ed0d79fbc1e95443d73238fd63bf17`.
All **3,963** original and restored members hash-verify. The three actual clock
executables and physical stores are retained in that original archive; stores
are **not reopened**. Raw JSON artifact **11289934313** is also preserved.

The repository's compact `proof.tar.gz` contains **233 independently read-back
and hash-verified members**: complete raw JSON uploads, API metadata/logs,
original producer manifest and canonical digest, final and initial independent
reviews, controls, exact reviewer/model sources and the supplemental Go model
executable. It excludes the large original clock executable/store tar because
local disk is constrained. That tar remains unchanged under
`/dev/shm/js-wf-clock-audit-success-37166976657/original-stores/` and in the
[GitHub original-store artifact](https://github.com/AntPAllen/js-wf/actions/runs/37166976657/artifacts/11290857183),
whose recorded expiry is **2027-01-02T01:06:05Z**. The RAM copy cannot survive
VM reboot. Restore the original GitHub artifact and verify the recorded digest
and complete producer manifest before further physical-store analysis.
