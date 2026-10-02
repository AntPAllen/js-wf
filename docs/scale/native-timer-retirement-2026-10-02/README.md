# Terminal native timer hint retirement

The timer and suspended-wait scanners now retire retained native hints after
reading a durable Completed or Failed journal. They enumerate only the invocation's
schedule subjects, validate identity/step/generation headers and delete the
observed sequence only when its generation matches the inspected invocation.
Active invocations and other generations remain. A concurrent replacement cannot
be removed by deleting the older sequence. Dry runs report eligibility without
mutation. Recurrent scanning handles late publications and uncertain delete replies.
This requires a live timer or suspended repair loop; direct schedule-only collectors
without invocation journals do not perform this cleanup. No deadline is relaxed.

`seeded-race.log` records1,000 seeded schedules covering eight combinations:
legacy timer or suspended scanner, normal deletion, drop before commit, lost reply
after commit, and Failed terminal outcomes. Cases preserve active/new generations,
retry deletion and catch late publication. First ten seeds replay exactly; the
seed42 pin and `cross-process-replay.log` cover serialized replay. Disabling the
production retirement function fails the named regression with Removed0.
`native-race.log` verifies actual R3 native retained deletion through the suspended
scanner, including active/dry-run/new-generation preservation and idempotent retry.
`race-controls.log` checks sequence-safe concurrent replacement and malformed hints.

The first integration smoke wired only the older timer scanner; this profile runs
suspended repair instead. That smoke failed physical drain in95.74s with three
retained hints, and its original events/non-store artifacts are preserved. The
first seed model's malformed Suspended payload was corrected to a valid active
Started tail before the final eight-mode checks. An earlier full simulator suite
passed in167.58s before suspended-scanner wiring, without coverage summary enabled;
it does not establish the final graph's per-workload gate. Final-source full
coverage and admitted native smoke evidence remain required.
