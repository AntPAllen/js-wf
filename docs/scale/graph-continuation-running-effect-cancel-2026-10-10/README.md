# Restored running-effect cancellation — 2026-10-10

New native controls resume through two SDK checkpoints, enter a third RunOnce
in the finish stage, then cancel through the real canonical client. They cover
R1/R3 domain × live/archive checkpoints × cooperative error/late success.
Required results: original prefix unchanged, exact cancelled invocation outcome,
one terminal and cancellation consumption, one effect declaration, initial/next/
finish called once, three effects entered, and an independently fenced duplicate
leaves the entire history unchanged. Late success cannot change cancellation.

The first R1/live/cooperative control failed at the two-minute functional watchdog.
[Failure receipt](failure-receipt.json) and [original output](failed-legacy-poll.log)
preserve actual exit 1. The running cancellation poll consulted legacy WF_SIG;
canonical cancellation owns a graph queue binding as well as a compatibility pointer. The change checks a validated
binding for the exact invocation and fixed cancellation key. A reservation alone
is insufficient. Legacy polling remains in its existing path.

Development verification and frozen eight-case qualification are pending. The
[runner](run.py) uses a separate sparse source checkout and retained supervisor;
[reviewer](review.py) requires terminal supervisor exit 0, exact Git inputs,
race binary identity and every expected case/output. Functional watchdogs are
not latency target definitions. Full faults, uncooperative effects, retention,
import, admission, latest-source seeded/extended and original gates remain open.

## Fixture correction

Both initial controls incorrectly expected the cancelled running delivery to
write its terminal immediately. Production intentionally abandons that handler
and returns it for retry; the next delivery drains durable cancellation before
user code. The corrected fixture performs that retry and accepts context.Canceled
from the running delivery only. It closes the best-effort notification subscription
before the running effect, requiring recovery through the canonical durable poll.
The original failures prove the fixture mismatch; they do not prove the effect
remained running until the watchdog. Trace evidence is retained separately.

The corrected control also removes and confirms absence of the WF_SIG cancel
pointer after acknowledgement. With notification disabled, only graph-owned
binding recovery can detect cancellation. Both original development failures
were fixture failures, not proof that cancellation failed to stop the effect.

[Corrected development result](development.log) passes with actual exit 0 for
R1/live/cooperative error, core notification disabled, WF_SIG pointer removal
confirmed and the required retry. The frozen eight-case matrix remains pending.
