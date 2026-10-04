# Original ordinary CI failures retained for diagnosis

At exact source `f70a990d6f2e92afa2952f3eef065979bc544a92`, ordinary CI has three
terminal failures in addition to the separately diagnosed workflow validation
failure. These results remain failed; no latency bound or assertion is relaxed.

| Run / job | Test | Confirmed failure |
|---|---|---|
| 37165885799 / 111328558292 | `TestMatrixIsolationAcquisitionHandoff` | 3-second acquired-delivery selection deadline |
| 37165885859 / 111328558421 | `TestMixedWorkflowsRecoverFromFourServerFaults` | terminal p99 46.012754875 s, target strictly below 30 s |
| 37165885859 / 111328558543 | `TestLeaseAppendPressureFixedPlacement` | delayed single-writer append returns outcome unknown after tail lookup API 503/10008; only 23/48 calls complete |

The first test is a local filesystem handshake and does not use NATS. Its source
publishes the arm token with `os.WriteFile`, allowing a reader to observe the
created/truncated file before its token write. That interleaving needs a
deterministic regression before attributing this timeout to any broker. The
retained log alone does not establish that interleaving as this run's cause.

The mixed latency and steady-placement pressure errors likewise do not establish
a server defect. Their complete uploaded raw JSON and terminal job logs are
retained without edits. Artifact inventory metadata records available originals;
the failed mixed and append-pressure originals are downloaded. Passed unrelated
consumer-pressure/lease-disk and continuation artifacts are not copied here.

`evidence.tar.gz` contains API run/job/artifact identities, all three complete
job logs, original uploaded mixed/pressure inputs, the exact historical handoff
source and scoped analysis. Every member is read back and SHA-verified before
publication; `manifest.json` records hashes. These uploads contain no actual
workload executables or physical broker stores. No reexecution, causal fix,
failed-parent promotion or full gate qualification is claimed by this archive.
