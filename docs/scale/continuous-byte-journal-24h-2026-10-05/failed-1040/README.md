# Corrected continuous-byte 24-hour soak: failed checkpoint 1040

Executed clean `b2d7011`, original 24h journal-leader profile, seed 1,
normal 2GiB/GOMAXPROCS=2, explicit routes, streaming state, original 20s attempt /
60s total / three-attempt audit budget. Native test failed after 7,986.85s at
checkpoint 1040 / 29,120 invocations. The supervisor is terminal; no unchanged rerun.

Attempts 2 and 3 returned 321,442 / 321,543 journal Next calls with zero journal
Next errors, reading 48,253,410 / 48,269,108 payload bytes. Attempt 3's maximum
Next wait was 0.204s. Both attempts reached journal consumer deletion. Attempt 1
also had consumer creation/iterator timeouts during the scheduled fault.
Checkpoint 1030 passed on its second attempt after the first timed out.

**Inference:** attempts 2/3 differ from the old SDK refill stall. The recorded
trace has no WF_STATE.Keys call. Recorded source obtains the fresh initial KV
watch after journal scanning and before Keys; WatchAll/initial updates are not
traced. The state snapshot phase is therefore the next diagnostic target, but
these traces alone do not prove why it timed out or a NATS defect. The failure
stack was captured after the audit returned and does not capture its prior wait.
Journals/Entries/Terminal zero are deferred report counters, not empty storage.

The million candidate and sustained rollout overlapped on the VM; no isolation
or resource-contention causality is claimed. Complete failed originals are kept,
with source-before/after equality and selected files checked against recorded Git.
Native stores have not been reopened. No 24h or full-matrix qualification.

## Preservation

Independently read and hash-verified all **6,249** original files; **1,318** selected source inputs bind to executed Git. Outer archive: 18 members / 224,209,548 bytes / 9 parts, all read back. SHA256: `a2f5d75d735197807b9a2b512aec2c3fb58feda61e6ef6b13b1d81562c5b3c2a`. The outer archive contains the full original archive once, plus review and observations.

See [independent review](independent-review.json), [archive verification](archive-verification.json), and [executed reviewer](executed-review.py). The native verdict is authoritative; preservation does not qualify a failed test.
