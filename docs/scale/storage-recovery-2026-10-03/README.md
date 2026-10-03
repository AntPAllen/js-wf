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
