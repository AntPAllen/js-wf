# Original seed6 replay with live peer diagnostics

New opt-in observations sample each peer's public local `/jsz` with Raft details and route count every2s while the existing35s fault context remains active. Each round records per-node timestamps, actual public server identity/state, local Raft group statistics and explicit transient diagnostic errors. Independent concurrent reads do not substitute for the original majority acknowledgment, minority probe, all-ten-store current/online gate or raw workload gates. Artifact write/close errors reject a passing diagnostic run; collection cancellation joins all readers before teardown.

Original requested replay: partition, seed6,10m,count1,nonrace,18m SDK, no timeout or p99 changes. This is a single diagnostic seed, not a restarted or resumed200-seed campaign. Source/model runtime graph is unchanged; no fullTier1 rerun. Actual run remains pending.

    python3 scripts/run-tier2-retained-row.py --root /tmp/js-wf-partition-seed6-live-diagnostics-20261006 --row partition --seed 6 --duration 10m --partition-diagnostics
