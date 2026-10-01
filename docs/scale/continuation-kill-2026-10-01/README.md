# Continuation publication SIGKILL proof — October 1, 2026

A real worker subprocess runs against a fresh R3 three-node file-store cluster
for each cut. A snapshot transport wrapper blocks at the confirmed boundary;
the parent verifies retained state and sends actual SIGKILL. Exit status must
report SIGKILL. Server processes stay running; this is a worker-crash contract.

## Four confirmed cuts

| Cut | Live WF_JRN messages | Live WF_SIG messages | Recovery |
| --- | ---: | ---: | ---: |
| Before manifest creation, after verified archive write | 8 | 1 | 13.131 s |
| After acknowledged manifest creation | 8 | 1 | 12.934 s |
| After acknowledged journal prefix purge | 1 | 1 | 12.927 s |
| After acknowledged signal purge | 1 | 0 | 12.930 s |

Every full logical journal has eight entries and a committed checkpoint before
the kill. The manifest is absent only at the first cut. The repair scanner must
publish exactly one continuation candidate for that anchor; the original run
also remains eligible for normal consumer redelivery. A replacement pinned to
another node waits for the dead lease and completes under a higher epoch.

At the first cut it replays the initial handler to the committed checkpoint
without rerunning its effect, then publishes the manifest. At every later cut,
its snapshot port denies all archived-prefix reads: the initial handler runs
only in the killed process, the replacement loads one frame and no archive,
and the buffered signal remains available even after its source stream is empty.
The prefix and suffix effects each execute once, both state and locals match,
and every pinned peer returns immutable result 46. Full retained-state integrity
passes. A second repair scan after terminal completion must enqueue nothing.

Each final archive-plus-live history also replays offline through its boundary,
consumes all 12 SDK entries, compares terminal result 46 and invokes no recorded
effect callback. A separate instrumentation log distinguishes handler entry
from effect execution.

## Evidence

- `real-race.log`: all four strengthened cases passed under race in 67.402 s
  of package time (66.36 s test time). The 30-second kill-to-terminal gate passed
  individually for each case; this four-sample run is not a release p99 campaign.
- `repair-mutation.log`: a compiled production overlay disables completed-
  checkpoint repair; before-manifest SIGKILL fails with Reenqueued=0 in 3.380 s.
  The failure occurs after a verified real process kill, not during compilation.
- `vet.log`: vet exited zero with no diagnostics.
- Source hashes bind the test and production dependencies. Runtime source is
  unchanged from parent 8e62eac; only tests/documentation are added.

## Remaining

Request/frame/completion/suspension/handoff process-kill cuts, hidden replies and
fault combinations, repeated checkpoints under SIGKILL, retirement/reuse,
integrated panic/cancellation/timer/promise/limit/GC gates and the independent
final-source matrix and 24-hour soak remain open. Seeded transport reply cuts
already exist separately; this proof does not model the process stop or server
internals, and does not close the mixed seed 65 latency failure. Online GC is
unsupported.
