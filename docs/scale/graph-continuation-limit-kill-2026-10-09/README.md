# Native graph continuation limit recovery after SIGKILL

Three native R3-domain/archive cases pass race on d66f65e production code plus
the source-hashed Linux fixture. The process helper runs the real partition
worker with internal continuation registration and a test-only 16-entry budget.
It blocks immediately after a successful production `journal_append` event;
the controller verifies that committed prefix and sends a real SIGKILL.

| Committed cut | Prefix entries | Recovery through exact delivery ACK | Old/terminal epoch |
| --- | ---: | ---: | --- |
| SignalConsumed at 13 | 14 | 16.541532 s | 30 / 34 |
| StepCompleted at 14 | 15 | 15.119449 s | 29 / 34 |
| Failed at 15 | 16 | 12.564666 s | 29 / 29 |

All cases meet the **unchanged strict <30-second recovery target**, measured
from just before SIGKILL through canonical terminal visibility, WF_STATE repair
and the successor's successful ACK of the *same* killed run sequence with a
higher delivery count. Each cut was run sequence 4, delivery 1; its successful
successor ACK is sequence 4, delivery 2. No alternate wakeup or already-visible
terminal result can substitute for that ACK.

The actual package exit is zero in 160.104 seconds. Per-case contexts are two
minutes; the seven-minute package watchdog accommodates all three cases. The
actual lease TTL is checked at 12 seconds; AckWait remains the runtime default
of 13 seconds. The killed owner retains its active lease: an immediate premature
acquisition must fail with `ErrHeld`. No server was killed or restarted here.

## Assertions and retained evidence

- Kernel process status must report SIGKILL; no graceful worker release occurs.
- The enabling signal survives; exactly 16 global entries remain and the
  reserved Failed terminal occupies index 15 with the rejected `must_not_run`
  declaration. The physical effect is absent from the durable handler log.
- The acknowledged cut prefix is byte-equivalent at the decoded record level;
  sequence/index/epoch ordering is preserved and initial/middle stages do not
  rerun. Nonterminal cuts finish under a higher successor epoch.
- The existing Failed cut keeps its exact terminal and original epoch; recovery
  repairs its projection and ACKs the killed delivery without rewriting history.
- WF_STATE bytes match the canonical failure and fresh graph clients/stores on
  all three peers return the same limit error.
- [Executed review](executed-review.json) verifies actual exits/PASS markers,
  source hash, three committed cut receipts, exact ACK metadata, raw retained
  prefix/final records, handler logs, strict timing and epoch conditions.

An initial after-signal prototype passed in 52.798 seconds with raw recovery
16.508993 seconds. It did not yet require ACK of the exact killed delivery;
that result is retained separately and is not used for the stronger acceptance.
A verifier initially expected an incorrect textual error; its expectation was
corrected to the actual `journal.ErrTooLong` value (`journal exceeds 100000 entries`).
The fixture's private lower budget reuses that production sentinel and does not
change the public 100,000-entry cap.

This qualifies three lowered-budget worker-process cuts on native R3/archive
storage with real queue redelivery. The actual 100,000-entry graph boundary,
combined server restart/partition/storage/VM faults, complete current Tier-1 and
extended suites, public continuation admission, production collection and every
original broader plan gate remain open. The full frozen151 race and frozen155
normal campaigns remain live on older source cuts.
