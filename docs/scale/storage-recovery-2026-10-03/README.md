# Lossless storage recovery — 2026-10-03

The VM exposes a 35 GiB root disk. At inspection it had about 560 MiB free.
No local broker/workflow test process or Docker container was running.

Unused Docker build cache was pruned (643.9 MB). Final images and all evidence
stores were retained. The completed inline five-million cardinality fixture
was then losslessly archived with the existing production evidence utility.
Its accepted report is byte-identical to the retained report under
`live-cardinality-inline-5m-2026-09-30`, SHA256
`5a25cd93d6b93e9fe5f6364cc60e1854d96f21210bd19cc3e656417483b0026b`.

The archive lives at
`/tmp/js-wf-scale-live-inline-5m-20260930/broker-store-archive`.
Each file was compressed into a temporary file, fsynced, decompressed and
SHA256 checked, atomically published, and durably recorded in the manifest
before removing its raw copy. The utility rejects open descriptors into the
fixture and checks the report remains unchanged.

- 2,543 original files; 1,565,275,909 raw bytes.
- 552,860,703 compressed bytes; 1,012,415,206 bytes recovered.
- Manifest SHA256:
  `6412ac26751a841a7f2e358b28c7f54daac382a028309c3332487fda5bc8e178`.
- An independent full restoration into
  `/dev/shm/js-wf-inline5m-restoration-20261003` verified all original bytes,
  restored permissions and timestamps, and passed every recorded file hash.

The compressed manifest, archive summary and restoration verdict are retained
here. This changes evidence storage representation, not its qualification or
runtime behavior. Original failed million-timer stores remain untouched.
Free root space after these operations was approximately 2.1 GiB; this alone
does not establish capacity for the full 24-hour matrix.

## Completed restoration duplicate removed

At 23:12 UTC, the canonical archive and completed temporary restoration were
checked again before removing only the restoration directory. All 2,543 members
passed compressed and decompressed SHA256/length checks; restored bytes, modes
and nanosecond timestamps also matched the manifest. Manifest and accepted
report hashes matched the values above. No process had an open descriptor into
the duplicate, and its complete file inventory matched the manifest.

The [verification and removal script](verify-duplicate-removal.py) and
[verdict](duplicate-removal.json) retain the checks and exact paths. Removal
recovered 1,565,275,909 raw bytes (approximately 1.46 GiB) of RAM space:
`/dev/shm` free space increased from about 1.8 to 3.2 GiB. Root disk free space
remains about 956 MiB. The canonical compressed archive, accepted report and
failed original stores were retained. The script records a successful audit
before deletion and a completion verdict afterward; it requires the original
duplicate to exist and is not an idempotent maintenance command.

## Completed local R5 proof expansion removed

At 23:52 UTC, all 5,000 members of the accepted local ten-minute R5 archive
were checked against both the archive manifest and expanded files. The retained
local archive and all three committed parts independently matched SHA256
`9cfbc99123cec3a47e2264ca23f8dd843075dd9ca4cd743747d8bb17f1b28abf`.
After checking open descriptors and confirming no local Docker containers,
only the redundant `fixture` directory and `integration.test` under
`/tmp/js-wf-local-journal-tenm-20261003` were removed. The canonical archive,
manifest, reports and source worktree remain. Re-extract the retained archive
before any future use of its removed expanded files.

The first inventory audit stopped before deletion: the source tree contains a
Git worktree pointer and Python caches outside the archive manifest. Its
[rejected script](rejected-local-proof-inventory-audit.py) is retained to record
that guard; the source tree was then excluded from removal. The executed
[corrected audit](verify-local-proof-duplicate-removal.py) and
[verdict](local-proof-duplicate-removal.json) record the final exact paths.

This recovered 207,073,280 allocated bytes (about 197.5 MiB) across 3,842
expanded files; free root space increased from about 739 to 944 MiB. Failed
stores remain unchanged. This affects storage representation only, with no
change to the [original single-row qualification](../local-r5-journal-ten-minute-2026-10-03/).
It does not establish sufficient disk capacity for a 24-hour soak.
