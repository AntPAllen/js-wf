# Normal2GiB five-container journal diagnostic: complete originals reviewed

Exact source `4a875f030a3c8bb9dd0994864d16c34cd833ba90`, journal/seed1/10m,
normal build, explicit2GiB/GOMAXPROCS2, original2m SyncInterval and batched
512-record retained reader with point KV reads/full method tracing. Ten-minute
workload plus completion/audit/cleanup takes678.38s. Native source has not
adopted the later KV snapshot candidate.

2436 invocations /26869 journal entries /19 journal-leader kill/restart faults
pass independent raw row review at the executed source. Worst terminal/progress
per-type p99:11.213767743/7.263699444s under original30s. Eight mandatory captured
cohort audits pass, longest attempt1.850979s under original20s/60s/three-attempt
limits. Mandatory final full audit/drain retains its named-test assertion scope.
All three independently rebuilt history models return exactOk for3132 operations;
45 selected local model dependencies match executed Git; actual model retained.

All1237 captured local source files match executed Git and pre/post producer
inventories. Actual SDK digest and every build-info field match its live/proc
capture and retained binary:
`5017771cf16b1f9041880053aa63504bf538a4cfd91a37e3128a91a26d1d8c79`.
SDK/supervisor/launcher terminal and five fixture containers removed. This
inventory binds local execution sources; exhaustive external/toolchain inputs
and a hermetic build are not claimed.

Complete producer archive5094 members /208064623 bytes independently hashes
and original file bytes verify after terminal cleanup. Archive57073796 bytes in
three ordered parts; SHA256
`89d48a02dd43febb7eafba340e9133d18876e890a0e1d1f5623b7f55624dd9fb`.
Includes actual SDK, native stores, source, events/raw faults and traces. Separate
review-proof.tar.gz preserves actual model,45 source copies, helper, build info,
commands/results and reproducible review/publish scripts:54 members,8437639 bytes,
SHA256 `d5918332e931b54f35b14d39513f6fab9f7043ccae3a070c004f70112560a78d`.
Every review member and original part/concatenation hash verifies by readback.
Manifests record each digest; concatenate original parts, verify, then extract
into a fresh directory. Stores were not independently reopened.

This is a reviewed normal2GiB ten-minute journal diagnostic. It does not qualify
race/512MiB campaigns, candidate KV-watch fault capacity, full200-seed/current
source matrices, original million physical drain or actual24h full matrix. All
three earlier actual24h failures stay failed. No longer run is launched here.
