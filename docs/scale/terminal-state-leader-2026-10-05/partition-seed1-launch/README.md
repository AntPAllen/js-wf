# Captured partition seed1 replay launch

Unit `js-wf-partition-terminal-leader-seed1-20261005.service` starts the original
`TestMixedMatrixServerPartitionEveryThirtySeconds` at03:24:04UTC on2026-10-05.
Clean source revision93c513379de4fba4357475056115d6340ca413a3 includes the terminal
point-state leader-read correction and opt-in original-store retention. Historical
c4fed06 seed1 failure remains unchanged and its cause unconfirmed.

The case retains the original10m workload, three file replicas, seed1 route cuts,
20s audit attempts/60s total,30s per-type p99 and5m completion deadline. Normal
build matches the historical hosted mode. Explicit GOMAXPROCS2/GOMEMLIMIT2GiB,
5G service ceiling/25m outer limit and18m test timeout are captured. It shares this
VM with the journal24h soak and native-million candidate; timing differences and
other source changes prevent a causal claim from a future pass alone.

## Verified launch identities

- Actual SDK PID291425; SHA256
  `c31cbd68c5615889751cadb15fe59f121afde987a512bdcd3508079b93324c4b`.
- SDK full build information records clean93c5133, Go1.27.1, CGO1 and normal mode.
- Actual NATS2.15.0 PIDs291471/291472/291477 share the retained binary; SHA256
  `24759ea94c030a9c5f1cd7fc61c74e14615a3c35650b5bb28b29891b5899ba9d`.
- All actual executable bytes/full build fields/arguments and selected environment
  values match the captured preparation/environment. All1172 selected repository
  inputs match Git blob identities and SHA256 before build and at live capture.
  This excludes archived evidence in docs/scale and is not a compiler/toolchain/
  external-module input closure.

Default test compilation first omitted a VCS stamp and failed a preparation
assertion before any case/store started. That executable/full build information
and note are retained; explicit `-buildvcs=true` preparation then passed.

## Evidence and pending work

The archive retains captured source bytes, actual SDK and shared server binary,
full metadata/preparation/producer/unit commands and a labelled early output
prefix. All1193 canonical members/three parts read back; archive64,777,869 bytes,
SHA256 `557d2f65fbc8753611011a505ecf9bc4cd74d09cb3f408c5dcef9c49a70e8e80`.
Concatenate sorted parts and verify `archive-verification.json` before extraction.

The primary root is `/tmp/js-wf-partition-terminal-leader-seed1-20261005`, with
fresh original stores under `stores/TestMixedMatrixServerPartitionEveryThirtySeconds`.
They are live and excluded from this launch archive. The opt-in harness retains
them after shutdown. Source-after equality, terminal receipts/faults, final raw
integrity/drain assertions and independent history models remain pending.
This is launch proof, not seed acceptance or full/final-source/24h qualification.
