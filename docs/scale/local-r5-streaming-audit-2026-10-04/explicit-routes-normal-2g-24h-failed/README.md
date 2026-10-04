# Actual24h journal attempt: failed at129 minutes

Executed95b63c0, normal2GiB/GOMAXPROCS2, combined streaming/state audits and
explicit all-peer route seeds. Named test fails after7751.58s at batch1050 /
29,400-invocation cutoff: all three original20s attempts exhaust the original60s
budget. Last successful checkpoint1040 audits29,120 invocations/321,244 entries.
The first failed trace shows205 journal pulls then2,869 serial GetMsg calls
(9.461s, three errors). Second/third attempts also exhaust their original budgets.
This identifies the client fallback path exercised, not a confirmed server cause.

The native million-timer loading burst began during this attempt and overlapped
the failed audit. Resource contention is a possible contributor; sole causality
is unconfirmed. No unchanged24h rerun or deadline expansion follows this result.
The next controlled investigation targets bulk-reader interruption recovery.

All1,266 before/after source inputs match executed Git; actual retained SDK SHA
`a5beec29e9a7909cd19904020b119e737bba0d1327ef7316e4fcbe2da3d47e39` and every Go build-info field verify. All6144
original archive members/current files, 7 parts and concatenated digest
read back. Size:159,694,631 bytes; SHA256 `21a69633399470bf5713fa035832d157845d6a35d048f0237ae6d26ed70e6365`.
Concatenate sorted originals parts, verify the manifest, then extract fresh.
SDK/stores/source/traces/logs remain retained; stores were not independently
reopened. Full24h/full-matrix qualification remains open. The first preservation
review selected too few source files by omitting392 sim/testdata inputs; its
script/log are preserved, corrected review verifies unchanged original bytes.
