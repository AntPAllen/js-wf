# Independent SDK timer handle history — 2026-10-11

The raw journal auditor reconstructs completed timer_start operations using the
absolute SDK request position. It binds timer_cancel, timer_await, timer/signal
selection and every timer case of Select to a previously created live handle,
its original name, deadline and clock domain. Cancellation requires a confirmed
cancelled completion. Fired or cancelled handles cannot receive another recorded
action. A signal selection leaves its timer live until later cancellation/firing.

Worker-annotated checkpoint frames must contain exactly the cancelled handle IDs
and cannot cross a boundary with any live handle. Earlier cancellations persist
across checkpoint pairs. Unannotated journal-only frames keep structural scope.
The implementation uses its own decoded fields and never invokes SDK replay,
production timer selection or checkpoint restoration.

## Evidence

Eight decoded-history positive cases and fourteen corruption controls pass under
race. A combined race also passes existing state, promise, signal and metadata
history tests. The final timer cases include absolute position8, a deadline and
clock domain, later creation at12, and exact cancellation identity.

Native R1/R3Domain SDK flow regressions exercise the integrated auditor with no
handle timers. They prove compatibility only; they do not prove native timer
creation, scheduling, firing or cancellation. Exact completed timings, exits,
logs, source hashes and live executable receipt are recorded in review.json.
The new CI timer guard is executed against the focused log and retained here.

## Remaining scope

Timer clock readiness, scheduling/source queues, case-priority proof, full native
timer workflows, reference-valid physical corruption fixtures, every historical
checkpoint and original scale/fault/soak gates remain open. This does not qualify
literal current-main complete Tier1 or the frozen larger campaigns. Public
admission, import and online collection remain disabled.
