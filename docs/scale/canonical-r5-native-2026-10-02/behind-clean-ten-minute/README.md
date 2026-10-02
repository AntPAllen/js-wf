# Accepted admitted behind-clock ten-minute row

Run36974795077 at27b78ca3ba95071fc817b35dd9fc07c4e52ec13d is terminal/success.
Go test729.20s includes the ten-minute workload plus setup/final checks.25 batches,
700 invocations/7703 entries/700 terminals,600 timer waits and19 confirmed shifted
owner kills. Five independent common-clock probes,195 server-clock observations,
116 role receipts and40 shifted native-hint observations corroborate the workload.
All19 cuts remove the pending Sleep source before its duration. The corrected
all-waits-2s profile is explicit and every timer duration is checked.

Final audits/history/physical drain and independent controller bounds pass.
Worst terminal/progress p99 is8.543235666/8.408965994s. Two completed-cohort audits
are present alongside the mandatory final whole-state check. The current offline
verifier reruns all required clock-cut/common-clock/checkpoint checks and produces
a byte-identical report to the original upload. Original artifacts, logs and
terminal metadata are retained without rewriting. This clears one ten-minute
behind seed at the recorded source, not ahead, every timer cut combination,
24h/full-matrix release, or protobuf chaos: these workers still write default JSON.
