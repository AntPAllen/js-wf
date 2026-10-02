# Accepted admitted ahead smoke after native hint retirement

Actual seed5 race execution at3184b6596e026c7bdf6f669f53d3329f8e64f406 passes
in86.29s (87.32s package time). The current offline row verifier accepts all
required common-clock, role, pending-duration cut, retained-prefix, history,
latency and physical-drain checks. There are56 terminal invocations,617 journal
entries,48 timer waits and one admitted shifted-owner kill. Worst terminal/progress
p99 is1.172907114s/14.522306877s. The suspended scanner records three retired hints;
physical drain then passes inside the unchanged30s budget. Original non-store
artifacts, raw drain monitoring and server logs are archived and each member is
byte-verified; original test execution and independent report are separate.

This35s workload is a smoke, not a ten-minute row,20 or200 seeds, all timer-cut
combinations or a24h full matrix. Ten-minute ahead run37002781141 at the same source
is a separate live validation.
