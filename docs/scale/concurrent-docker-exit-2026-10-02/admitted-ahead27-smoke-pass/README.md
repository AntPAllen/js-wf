# Admitted clock seed27 smoke at the new observer

Exact0f979d0e03d38b05136f699f53fa727dbd035e11 passes the actual R5 ahead-clock
race smoke in64.12s. Two mixed batches yield56 invocations,617 journal entries,
48 timer waits and one confirmed shifted-owner SIGKILL. Five physical probes,
15 clock observations, histories, raw retained-state audit, controller-clock
latency and final physical drain pass. Independent artifact verification accepts
the pending positive Sleep cut and strict exit before earliest due.

Kill starts15:56:20.136468313Z, reply returns15:56:20.655316358Z, confirmed
absence is15:56:20.781188088Z and earliest due is15:56:21.198033464Z. The
receipt marks concurrent observation. This particular cut observes absence
after the reply; the separate held-reply contract proves that the observer can
retain an earlier stopped-state bound when the reply is delayed. No timestamps
are backdated. Worst terminal/progress p99=1.228833856s/7.280230659s.

The72 published artifact files, source identity, actual events, independent
report, explanations and fencing review are preserved losslessly with SHA256
readback checks. Cluster configuration is included; build binaries and physical
server stores remain only in the original local root, preserved under
`/tmp/js-wf-concurrent-exit-ahead27-0f979d0`. The archive follows the hosted
workflow's artifact scope rather than claiming a copy of those stores.

This35s smoke cannot clear the original275s failed execution. Focused ten-minute
seed27 run37030391587 is launched at0f979d0 with both common-clock and timer-cut
requirements. Its acceptance, all in-flight cut combinations,200 clean seeds
and full matrix/soak remain open. The original failed cut stays rejected.
