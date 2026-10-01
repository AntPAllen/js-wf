# Verified stronger behind-clock smoke

Run36938299478 at source06826547a27dda79fa4da2d534d805c020a0a76f
passes112.59s (race instrumented). Current row guard, event explainer and
fencing reviewer independently pass against downloaded artifacts.

196 invocations,2158 journal entries,one actual skewed-peer4 SIGKILL/restart,
15 clock observations,eight role observations,156 shifted timer-clock lookups
and168 audited timer waits are verified. All histories,invariants,strict
controller p99 and physical WF_RUN/64-consumer drain pass. Worst terminal
p99 is7.397225997s; worst progress p99 is12.870150587s. All55 acknowledged
repair records agree with final worker counters; no fencing records occur.

This is a35-second smoke, not sustained/full-matrix acceptance. It does not
admit every in-flight timer/effect/continuation combination, and does not
contradict the clock-transition counterexamples in Tier1. Raw artifacts are
retained; files over16KiB are gzip compressed with deterministic timestamps.
Inflate them into their original paths before running the artifact reviewers.
`artifact-sha256.json` hashes original uncompressed downloaded bytes.
