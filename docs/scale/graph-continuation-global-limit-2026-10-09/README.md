# Graph continuation global limit and terminal slot

Four native race cases pass on b3680f8 production code plus the source-hashed
new fixture. Each case reconstructs the worker and its reader/runtime state for
every independently leased delivery, resumes through two SDK-generated
checkpoints, then accepts an enabling signal near the global entry cap.

| Domain fixture | Archive compaction | Test duration |
| --- | --- | ---: |
| R1 | disabled | 14.99 s |
| R1 | enabled | 22.36 s |
| R3 | disabled | 24.11 s |
| R3 | enabled | 36.19 s |

The package passes in 98.711 s with actual exit zero. Each case has its own
two-minute context; the nine-minute package watchdog accommodates all four
independent cases. This is a functional limit check, not a latency benchmark.

## Assertions exercised

- The production default is still 100,000 entries. The fixture lowers only the
  private worker budget to 16, using the same guard exercised by ordinary workers.
- Two checkpoints preserve absolute anchor index 9 and SDK position 8; the
  pre-signal journal has 13 entries and suspends at absolute index 12.
- The signal consumes index 13, its pending completion fits at index 14, and
  the reserved `Failed` terminal occupies index 15. Exactly 16 global entries
  remain; sequences and epochs are monotone across reconstructed workers.
- Initial and middle stages each run once. The next `must_not_run` request is
  retained as the rejected declaration; its physical effect runs zero times.
- Both archive cases verify actual canonical retained offsets after each
  checkpoint, reduced live forest counts and zero remaining reader pins.
  Full logical history remains readable through archive/live ownership.
- The canonical terminal error equals `journal.ErrTooLong`, matches WF_STATE
  bytes, is stable from fresh clients/stores on every peer, and survives another
  terminal delivery without rerunning a stage or effect.
- `WF_JRN` remains empty. Public graph continuation registration is still
  rejected; the fixture installs its stages internally for migration checks.

## Negative control

A read-only Go overlay changes the production append guards to count only
`len(records)` from the resumed suffix, retaining the original absolute append
indexes. The R1 archive case **fails as required** in 21.244 s, actual exit 1:
it records `limit reset across checkpoints <nil> 1` with exactly one forbidden
effect and unchanged initial/middle call counts. This confirms the fixture
detects resetting the entry budget after a checkpoint. Production files were
not edited; the recorded mutation reconstructs exactly from their Git bytes.

[Executed review](executed-review.json) verifies all four PASS markers,
actual exits, test source hash, production source/Git bytes and the negative
overlay hash and semantic failure. Raw logs and exact commands are retained.

This qualifies the lowered-budget functional boundary. The actual 100,000-entry
graph continuation boundary, SIGKILL/restart/lease-fencing combinations,
public continuation admission, complete current 156-family/all-841-pin/extended
qualification, production collection and every original broader plan gate
remain open. The live frozen full151 race and full155 normal campaigns use older
source cuts and do not include this new ordinary test.
