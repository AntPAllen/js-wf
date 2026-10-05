# Bounded retained bulk-read resumption

The opt-in retained bulk reader now recreates an interrupted consumer at the
first sequence not yet accepted by the visitor. It permits at most two fresh
cursors, retains the original stream bounds/cutoff and audit context, and uses
one stable identity per cursor for uncertain-create retries. Cleanup covers
all created or uncertain names within one bounded cleanup context. The existing
replication configuration and 20-second attempt/60-second total audit limits
are unchanged.

Leader next-message reads still resolve gaps and short successful tails; after
repeated transport interruptions the existing leader-read fallback remains.
Metadata, ordering, visitor, and other semantic failures remain fatal.

## Verification

- The new large-tail regression failed against the prior implementation: after
  accepting sequences 1–128 it attempted a point read at 129. The corrected
  implementation visits all 4,096 records exactly once, starts the replacement
  at 129, performs zero point reads, and clips the captured 5,000-record stream
  at the 4,096 cutoff. The point-read rejection models the cost-sensitive path;
  it does not measure NATS throughput.
- Race controls pass, including repeated interruption bounded to two resumptions,
  leader resolution of omissions/deleted records, semantic errors, uncertain
  creation identity/cleanup, and cancellation immediately after a visited prefix.
- Native high-water/hole comparison passes: 34,993 retained records, identical
  point/bulk digest; point 2.192 seconds, bulk 0.323 seconds; zero consumers left.
- Native streaming audits pass with and without snapshot state: actual consumer
  leader library shutdown with 1,488 pending messages visits all 2,000 entries;
  cancellation stops at 128. All retain the original 20-second attempt budget.

The actual compiled normal test executable, live `/proc` identity/hash, full
build information, test logs, source patch, before/after inventories, and 633
tracked Go/module source inputs are retained in the compressed proof. See
`verification.json` for exact inventory/member counts and archive SHA-256.

This is focused reader evidence. Native leader loss can recover transparently;
these native tests do not establish that the new resumption branch was taken.
That branch is exercised by the regression controls. The race executable,
exhaustive compiler dependencies, and native temporary stores are not preserved.
No R5/large native interrupted tail/full matrix/24-hour gate is qualified.
The previously failed soak remains failed; its concurrent timer-loading overlap
and unconfirmed resource/server cause remain recorded.
