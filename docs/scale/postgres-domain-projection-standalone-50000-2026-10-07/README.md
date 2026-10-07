# Full packaged PostgreSQL domain projection recovery

Clean `dc88aabfbcccedcc8b65912888b8183fa8b3a5af` passes the full original **50,000** workload in **284.64s** body / **284.6771s** SDK, count1/20m fixture/22m SDK/nonrace/2CPU/2GiB. The actual `js-wf/cmd/wf` executable is built with the same clean VCS revision as the SDK. All three actual children have captured executable hashes, birth/argv/cwd/environment and SQL application-name writer admissions:

| Child | Native outcome |
| --- | --- |
| Initial `wf project -domain WFVIEW` | SIGKILL reaped before all Starts/results; no SQL rows |
| Catch-up | Its admitted SQL writer session is terminated at 11 partial rows; native command logs writer lock session loss and joins exit1 |
| Replacement | Completes lag0 and joins SIGTERM cleanly with exit0 |

All 50,000 acknowledged Starts and exact results are checked while projection is down; stopped lag is100,000. The physical run queue drains and six workers join before the catch-up fault. The actual embedded journal leader shuts down/restarts with a new identity; pinned contexts refresh and all four R3 source streams and three actual WFVIEW peers confirm healing. Every exposed SQL row and indexed column remains identical across a complete final rebuild.

Independent review binds selected Git and external command/SDK inputs, the actual parent and command executables, actual PostgreSQL image/container/executable, closed media copies and every member of the complete original archive. [Review](native/independent-review.json). [Coverage controls](coverage-controls.json) accept the actual proof and reject32 altered records/raw logs, including missing phases, wrong command/domain, source/hash/birth/env/SQL identity, failed shutdown, missing full workload and native skips. The manual CI workflow now preserves and reviews both the original SDK-helper and packaged-command profiles.

This qualifies the full packaged PostgreSQL/domain projection SIGKILL/session-loss/library-journal-restart/rebuild component at this recorded source. The parent SDK domain trace is **not a child wire-prefix trace**. SQL is stopped gracefully after the SDK exits. It does not qualify NATS process SIGKILL, copied NATS-store audit, leaf routing, natural follower-lag/lost reply, online GC, whole current matrices, million physical timer drain, or the actual24h soak. Default official NATS2.15.0 remains unchanged. Original journal24h and hosted partition200 handles remain unchanged.

The complete 122,085,327-byte archive has9,070 members, SHA256 `2862ee44f669a4f23373fb852214ea4fd42c7df2cbe72f7a1520bd6fb0f2fbba`. SQL rows and original media remain in the complete archive and local fixture, rather than duplicated into Git. Complete archive metadata/inventory are committed here for S3 retention.
