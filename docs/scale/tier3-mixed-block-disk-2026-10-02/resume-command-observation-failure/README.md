# Sustained row stops at first stall: resume observation boundary

[Run 36946199913](https://github.com/AntPAllen/js-wf/actions/runs/36946199913)
at 31619b5430a5a44cc5978947d1c739b9dc0065a1 fails in 67.63s at its first
five-second cut, after eight mixed batches. The sync returns at
01:00:04.103383072Z, 5.024089128s after suspension. The controller observes
`dmsetup resume` CLI exit at 01:00:04.105958044Z, 2.574972ms later. The kernel can
resume blocked I/O before the CLI process exits; requiring sync return after
that exit was an invalid causal assertion. The run is incomplete and does not
clear the sustained row. Every original downloaded artifact and failed log is
retained compressed with original byte/hash manifests.

The fixture now records resume-command start separately. Both command start and
sync return must be at least five seconds after suspension. Sync cannot precede
resume-command start, and both sync and successful command return must precede
confirmed R5 heal. The dirty sync is still required to stay blocked throughout
the full five-second interval. The native capability test checks these same
boundaries. Historical proofs without the new field retain their stricter old
comparison and cannot be promoted from this failed row.

The Python controls now accept a sync inside command execution and reject an
early resume or a sync before command start. All 37 Tier3 tests pass. Integration
compilation succeeds with native opt-in tests skipped; a corrected sustained
native run remains required. No physical hold or recovery target is reduced.
