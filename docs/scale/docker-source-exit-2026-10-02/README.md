# Docker source exit and container cleanup

The clock fault harness now records successful exact-name Docker state observations
separately from container-name cleanup. After SIGKILL, only `exited`, `dead` or
successful absence confirms the source is gone. Running, removing, unknown states
and failed listings do not establish exit. Controller observation time is a
conservative upper bound; daemon `FinishedAt` does not enter latency calculations.
`KillNode` still waits for cleanup before a restart may reuse the name and store.

The pending timer cut gate uses this confirmed-exit upper bound. It still requires
exit before the earliest duration boundary, the refreshed pending journal tail,
the acknowledged shifted native hint, replacement roles and physical drain.
The receipt binds node and container identity to restart operations and the
independent clock probe. Offline verification checks all timestamps. Historical
receiptless artifacts retain their original conservative cleanup timestamp.

`native-race.log` records real Docker kills for retained and automatically removed
containers, plus controls for running/removing states and failed observations.
The automatic-removal case observed `dead` about 118ms before cleanup. This proves
that the two observations can differ; it does not establish what happened in the
historical failed ahead20 seed5, whose process exit was not observed separately.
The initial native invocation used an absent `latest` image tag and failed before
starting a container; the recorded run uses the installed image ID 4d76d619c108.

`admission.log` covers stale, advanced and late pending-tail rejection.
`offline-controls.log` covers legacy compatibility, delayed cleanup, late removal,
invalid states, chronology, container identity and clock-source binding.

The admitted seed5 ahead smoke actually ran under the race detector and failed
in102.8s. Its selected source stopped at11:08:35.673152145Z, before the earliest
duration boundary11:08:36.942875028Z; cleanup completed121ms later. The later
five-current-replica heal check timed out on KV_WF_LEASE. This is a failed row,
not a latency/drain acceptance or proof of a NATS cause. Original non-store
artifacts and server logs are archived with verified member hashes; JSON test
execution is retained separately. The initial smoke command selected a nonexistent
test name and ran no tests; it is excluded from all evidence. The recorded corrected
command selected TestFiveContainerMixedServerClockAheadWithJournalKills.
