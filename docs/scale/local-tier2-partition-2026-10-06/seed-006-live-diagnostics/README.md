# Original seed6 replay with live peer diagnostics

New opt-in observations sample each peer's public local `/jsz` with Raft details and route count every2s while the existing35s fault context remains active. Each round records per-node timestamps, actual public server identity/state, local Raft group statistics and explicit transient diagnostic errors. Independent concurrent reads do not substitute for the original majority acknowledgment, minority probe, all-ten-store current/online gate or raw workload gates. Artifact write/close errors reject a passing diagnostic run; collection cancellation joins all readers before teardown.

Original requested replay: partition, seed6,10m,count1,nonrace,18m SDK, no timeout or p99 changes. This is a single diagnostic seed, not a restarted or resumed200-seed campaign. Source/model runtime graph is unchanged; no fullTier1 rerun. Actual run remains pending.

    python3 scripts/run-tier2-retained-row.py --root /tmp/js-wf-partition-seed6-live-diagnostics-20261006 --row partition --seed 6 --duration 10m --partition-diagnostics

## Native failure reproduced with live evidence

Executed6d277d7 requests the original10m seed6 case and fails at its first replica-heal deadline after70.78s. Eighteen observation rounds are retained (final cancellation may contain explicit read errors). All three routes are8 by round7. In the final complete round17, nine workflow store leaders report their replicas current, while the lease-store leader reports minority node2non-current/lag3803. That minority’s local lease stream remains last_seq4144 while the majority reaches5628. Logs repeatedly reject the expected snapshot/peerstate catchup frame.

The final timeout message namesWF_INV with a request deadline because the scan restarts there as the whole-fault context expires; the direct local observations identifyKV_WF_LEASE as the still-stalled store. This reproduces a live catchup symptom and distinguishes it from loss of routes or general store unavailability. It does not yet establish the protocol cause or a causalTier1 reproduction. [Full failed run and observations](native-failure/).

The wrapper also failed after writing native exit1 and source before/after ledgers due to a duplicated late flag read after its variable became an argv list. This finalization error is fixed for future runs. Independent finalization converts the preserved native log and runs the exact executed-source checker, which rejects it; no native rerun or verdict change. Complete6171member/75,712,148byte archive read back against all files. Original full200campaign remains failed; original24h journal run remains separate.
