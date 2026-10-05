# Full R5 interrupted byte-bounded reader gate

The continued clone's 640 pre-restart files match the previous published
`r5-100k-profile` post-run ledger. It reopens the original 100k completed
invocations / 1.2M-entry cohort under the original logical NATS identities with
fresh Docker resources. Normal executable, GOMAXPROCS=2, GOMEMLIMIT=2GiB,
explicit route seeds, R5 file streams and verified R5 read cursors. Each complete
invariant/state audit retains its original 20-second budget.

| Case | Full audit | Journal cursor starts | Leader point reads |
| --- | --- | --- | --- |
| Byte-bounded baseline | 17.824323109 s | 1 | 0 |
| Explicit-error interruption | 15.905977579 s | 1, 129 | 0 |
| Short-success interruption | 13.955524402 s | 1, 130 | 1 |

Each case completes exactly 100k invocations, journals and terminals / 1.2M
entries. The visitor checks every sequence against its ordinal, admitting
**3.6M exactly-once journal visits** across the three cases. Both interrupted
cases deliberately discard the first real pull's suffix after 128 visits; one
reports a synthetic timeout, the other reports nil. The short-success leader
oracle visits sequence129 before the replacement cursor starts130. Neither
case reproduces a natural NATS/server/TCP fault.

The fixture asserts the precise cursor starts, point-read counts, interruption
admission and zero remaining invocation/journal audit consumers after each case.
Unlike the comparison-only mode, interruption mode requires **every complete
report**; deadline misses fail the named test. Named test passes60.17s including
startup/cleanup. Sequential timings are gate observations, not evidence that
interruptions improve performance.

## Preserved evidence and boundaries

Live SDK `/proc/PID/exe` hash and build information; all five container binary
copies/build fields/inspect records; 639 selected Go/module source bytes and
matching before/after hashes; producer, logs, three CPU profiles, phase reports,
independent result assertions, clone continuation and post-run store ledgers.
This is not an exhaustive compiler-input inventory. Container binary captures
are from their `/nats-server` paths, not server-process `/proc` descriptors.

Focused byte-transport and retained-scan race controls pass1.065s; the race
executable is not separately captured. Clone bytes stay at the recorded VM path
and are not duplicated in this archive; original input store bytes remain in the
earlier failed-campaign archive. The immutable original failed stores are not
used by this run. Archive member/part hashes are independently read back.

The ongoing native million-timer diagnostic shares this VM during these tests;
these are not exclusive-host performance measurements. Production/default
readers remain unchanged. Natural transport/leader-loss and semantic corruption
controls for this candidate precede a controlled adoption decision. Full matrices,
final-source qualification and the actual 24-hour gate remain open.
