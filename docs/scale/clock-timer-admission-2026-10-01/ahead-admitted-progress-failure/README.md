# Ahead-clock admitted timer cut: progress gate fails

[Run 36945292216](https://github.com/AntPAllen/js-wf/actions/runs/36945292216)
at c03994c4f9a58ef388069cbb7f498caf3f27c7a6 fails in 162.51s. It reaches
168 invocations and 1,849 retained entries, but timer progress p99 is
60.270566034s against the unchanged under-30s target. Other workload latency
cells stay under target. The original Go events retain the actual failure.

This cut passes admission: node four actually owns both relevant R5 roles with
an observed +60s clock, the exact retained timer-0 suspended tail is refreshed,
and source removal completes 1.194302801s before its conservative earliest due.
The final journal corroborates that selected prefix. Independent controller
provenance, actual-clock/role and cut checks pass as diagnostics only. They do
not override the failed row or imply release acceptance.

For the selected two-second timer, the first completion append's conservative
lower bound is 60.268780416s after conservative earliest due. Every actual
creation clock and timer duration is checked against the retained request;
1,849 append windows and 144 waits are reconstructed from controller calls and
independent receipts. The diagnostic report is explicitly `native_gate_passed=false`.
Twenty-nine acknowledged repair records match final counters, with zero fences.
Histories, final retained audits, immutable terminals and physical drain pass.
All original downloads, failed job log and diagnostic reviews are compressed
losslessly with original SHA256/byte manifests.

The roughly sixty-second excess after replacing a +60s origin with unshifted
roles agrees with the existing absolute-deadline clock-transition model's
counterexample. That is an inference from timing and the explicit model; these
artifacts do not prove the precise broker scheduling/redelivery path. The next
runtime work is clock-transition timer liveness. No p99 relaxation, passing
sustained ahead campaign or all-in-flight matrix acceptance is claimed.
