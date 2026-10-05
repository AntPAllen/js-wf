# Direct callback delivery windows

Explicit `scanConsumeDirectWindowsThrough` removes the batch relay goroutine
and channel. SDK Consume still has its bounded8MiB client buffer and unbuffered
callback delivery channel; the scanner receives and processes each message in
its own goroutine.4096-record/2s windows retain one cursor between windows.
Context cancellation stops the callback; outer cleanup retains the original
remaining-deadline join, or independent2s without a caller deadline.

The common scanner now accepts a delivery window callback. The original Fetch
and byte adapters use a channel-backed window that still drains MessageBatch
before reading Error. Sequence bounds, next-message leader oracle, wrong-stream/
order checks, confirmed replicated-leader replay proof, two-resume limit and
invariant reductions remain in that shared body. Public defaults do not select
the direct callback candidate. R1 recovery is not qualified by this change.

Race unit controls pass1.034s: existing batch/byte/callback/lifecycle tests,
differential channel/direct complete/gap/cutoff/backward/wrong-stream/visitor/
transport controls, cursor continuity across windows and direct cancel/heartbeat/
semantic-error preservation. Native R3 full/cohort/compaction/corruption/state
and large-payload refill/hole/cutoff/cancellation comparisons are prepared.
A preparation text transformation failed and was corrected before native launch.
No native/default/capacity/legacy/fault/24h claim yet.

The previous closed R1 profile copy (998 files /3580391120 bytes) was reclaimed
only after complete pushed canonical base-plus-delta/current-byte verification,
closed SDK/five servers and all visible task descriptor checks. Originals and
complete canonical evidence remain retained.
