# R5 mixed worker reply-isolation row

`TestFiveContainerMixedWorkerRepliesIsolatedFortyFiveSeconds` runs five actual
worker processes, each pinned to its own proxy/server endpoint with discovery
ignored. Five real Docker NATS nodes retain R5 stores and production2m write sync.
At+5s and once per minute, a seeded acquisition handoff holds a selected delivery.
The parent reads its journal while held and rejects completed/failed invocations;
an admitted nonterminal prefix/tail/time is retained. Server replies to that
worker are then held for45s while client requests continue to reach the server.
The cut is confirmed by held-reply/request counters; overflow fails the fixture.
After resume, a fresh successful worker PING and reply traffic confirm recovery.
Typed fencing must belong to the exact selected delivery. Final selected journals
retain the original prefix and completion, with terminal time after cut.

## Verified smoke

35s workload race PASS73.04s /74.087s package, including the full45s reply hold:
4 batches,112 completions,1,233 entries. Histories, retained invariants, raw p99
and physical WF_RUN/all64-consumer drain pass. Worst terminal p99 is4.774754804s;
progress p99 is2.008533663s. The one fencing event matches the original acquired
delivery and precedes terminal completion. All five graceful final counters match.
All78 repairs are acknowledged (18start,49signal,11suspended) with checked decisions.
The gate remains raw enabling-event latency; no route heal-time exception applies.

All52 Python tests and vet pass. Controls reject blocked requests, missing held
replies, overflow, stale PING, wrong PID/delivery, proxy bypass, already-completed
cut targets and changed prefixes. Common process validation is shared with pause;
the original pause proof still passes. Traffic traces are bounded diagnostic data.
Future hosted runs upload text stack diagnostics as well as JSON/JSONL/log files.

## Excluded first smoke

`excluded-terminal-duplicate` passed the former delivery-only checks at71.28s /
72.324s package,112 completions and1,239 entries. Timeline review showed an
already-terminal duplicate was selected. It establishes reply-hold and lease
fencing behavior, but cannot establish recovery of an unfinished invocation.
It is retained as excluded coverage. The final gate requires nonterminal cut
admission and original-prefix recovery, and rejects such targets.

Large raw files are losslessly compressed. Binary/source hashes identify local
runs. Original closed stores remain in the /tmp roots. This is one smoke; clean
ten-minute and full24-hour matrix gates remain open.
