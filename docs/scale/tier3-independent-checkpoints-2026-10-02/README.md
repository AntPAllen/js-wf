# Tier3 completed-cohort checkpoints during continuing workload

Tier3 now uses the existing completed-cohort API and independently runs one
checkpoint reader, matching the Tier2 harness. After each completed tenth batch,
WF_INV high-water is captured before more starts; that cohort is quiescent and
never purged/reused during the check. Later executions can supply positive
pending timers while the checkpoint reads. All expected audits must finish,
counts must match the captured cohort, any error cancels/fails the row, and a
final whole-retained-state audit remains mandatory. Evidence records batch,
cutoff, start/end, counts and errors incheckpoint-audits.json.

Future CI requires every expected checkpoint artifact. The guard rejects missing,
duplicated, reversed, failed, unfinished, short-count and invalid-cutoff records.
All43 Tier3 Python artifact methods pass. Existing pending-clock/cut refresh
race controls pass1.143s. Real completed-cohort isolation and later-corruption
race control passes4.822s. These prove the component checks and compile the
changed fixture; a new full admitted native run remains required.

The previous run reaches13 clock cuts but cannot supply a pending timer while
its synchronous batch50 full audit runs. [Retained failed evidence](../canonical-r5-native-2026-10-02/behind-thirteen-cut-checkpoint-failure/)
is not converted to acceptance. Admission/source/lead/latency checks are unchanged.
