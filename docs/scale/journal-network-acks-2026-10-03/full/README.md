# Accepted full thousand-case real network journal recovery

The actual retained normal executable at exact
`eacff61ee832e75e59ae96bd8aa6d1747f28151d` passes the default 1,000 cases:
500 committed publications whose acknowledgments are dropped by a TCP relay,
500 connections cut before publication, one confirmed journal-leader stop and
same-store restart, and 3,000 raw receipt comparisons through three pinned peers.
Named test/package elapsed are 6.800/6.809 seconds; observed execution wall time
is 6.875795364379883 seconds. This uses three in-process real NATS servers;
the leader stop is the fixture's `KillNode`, not an OS process SIGKILL.

Independent review regenerates the full-size scope from raw Go events and
1,011 original fixture files. The 7,968-frame transcript matches final forwarded
byte counts, contains each committed publication's exact payload/CAS bytes,
and contains no escaped publish acknowledgment. Absent branches contain no
publication. All retained identities, sequences, initial/retry receipts and
three complete peer censuses/receipt arrays match. Midpoint metadata confirms
the original leader changed after its stop. Both recovered branches retain
exactly one entry per subject, with no unexpected sequence or payload.

All 608 captured Go/module/reviewer input files match Git byte-for-byte and are
unchanged before/after execution; copied source bytes match too. The executed
binary is retained and its SHA/build settings independently rechecked:
`a0c264dcd192814ef80077bdee773f5d7d6ea532f196c4faad4334cacca4b849`.
It is a normal build; Go test executables carry no VCS build stamp here, so
provenance uses the captured compilation inputs and commands.

The first raw reviewer incorrectly demanded immediate per-case removal from
the proxy's active map. Physical socket close can precede bookkeeping cleanup.
The corrected reviewer requires the complete connection transcript and zero
final active/buffered counts instead. Its prior rejection reproduces from
unchanged originals; corrected review accepts without rerunning workloads.
An injected escaped acknowledgment, with adjusted forwarded counters, is
rejected; the two-case control cannot clear the full-thousand flag. Actual
reviewer bytes and controls are retained separately from the executed source.

The 1,633-member archive (25,512,459 bytes) reopens and SHA-verifies every member
before atomic rename. It retains executable, full source ledger/copies, driver,
commands, raw receipts/transcript and independent verdict. Physical broker stores
are not retained/reopened. The earlier failed counter-assertion trial remains
rejected and unchanged. This clears the Phase 2 thousand real-network append
recovery gate only; full matrix, five-VM and 24h gates remain open.

The `journal-network-acks` manual CI workflow runs the same default-size fixture
and verifier, retaining its actual binary, compilation inputs and original raw
artifacts. No hosted pass is claimed from adding that workflow.
