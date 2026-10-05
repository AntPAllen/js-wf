# Sustained protobuf-to-JSON worker rollout: latency gate failed

Executed clean `ab5cd71`, original ten-minute worker_kill cadence, seed 1,
normal 2GiB/GOMAXPROCS=2, explicit opt-in protobuf-to-json rollout. Native test
failed after 775.92s: matrixshort terminal p99 33.910941280s exceeds the original
strict 30s target; progress p99 23.510797574s. Other five cells were below 30s.
The named test reported 14 batches / 392 invocations / 4,345 entries / 119 faults.
Those are failed-run observations, not accepted sustained qualification.

The supervisor and SDK are terminal. Complete failed originals and the final
live SDK observer records are retained; selected source files are checked against
recorded Git and source-before/after. SDK observer coverage is not claimed to
include every generation. Raw wire/history/fencing review has not promoted this
failed parent. The accepted 35s smoke remains limited to its recorded scope.

This overlapped the runtime soak and million candidate on the VM; causality is
unconfirmed. Original latency targets remain. No unchanged native rerun, no
sustained/default-profile/full-matrix/24h pass, no stores reopened.

## Preservation

Independently read and hash-verified all **6,531** original files; **1,334** selected source inputs bind to executed Git. Outer archive: 13 members / 48,387,499 bytes / 2 parts, all read back. SHA256: `36edbf9f01f0d144569677c922e8dc6af1264918f48570a3f8b4f5f83351f15f`. The outer archive contains the full original archive once, plus review and observations.

See [independent review](independent-review.json), [archive verification](archive-verification.json), and [executed reviewer](executed-review.py). The native verdict is authoritative; preservation does not qualify a failed test.
