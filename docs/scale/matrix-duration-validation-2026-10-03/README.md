# Shared sustained-duration validation

The duration checker at baseline `c50ece7` accepted synthetic controls with
a NaN elapsed duration, contradictory package failure/pass events, or a
completion from an unrelated package. The retained baseline and corrected
control reports record these outcomes. The whole Tier2 artifact checker already
had a separate failure-event check; the contradictory-failure control concerns
the shared duration checker in isolation.

`scripts/check-matrix-result.py` now requires a finite numeric elapsed duration,
rejects failure/build-failure events, and requires exactly one passing package
completion matching the named test's package.

All 24 matrix verifier/planner regression tests pass (1.954 seconds), including
the existing synthetic 2,600-execution release controls. The unchanged original
events from the accepted local five-node ten-minute journal row also pass the
corrected duration checker; its raw SHA and 714.110-second named-test duration
are recorded in `corrected-controls.json`.

These are verifier controls and a retained-event recheck. They establish no new
workload result, source compilation, full-matrix qualification, or 24-hour gate.
Existing qualification runs remain undisturbed.
