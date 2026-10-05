# Retained worker-clock smoke

Normal parent SDK at `828db48`, actual PID527702 / SHA256
`3af0d2e22db8f0644dc1cec59c3c3403517e7b3979e140f18aaa686ba5594f51`.
Native PASS266.00s includes building two shifted race SDKs. The workload duration
is 35s, seven batches /196 invocations, journals and terminals /2171 entries.
Terminal aggregate p99 5.088019049s; all six terminal and progress cells below30s.
Initial and30s worker clock samples confirm approximately+5s/-5s/zero offsets.

Independent review verifies668 Git inputs against executed source,3286 durable
external inputs unchanged and captured, and one separately retained generated
Go test-main input. Producer and duration checker both exit0. Both generated time
source/overlay files and shifted SDKs are retained; actual observed worker bytes
match their respective generated executables. Three workers and three NATS
processes observed; local PIDs gone. Point observations are not lifetime coverage.

Complete closed original media/source/executables/reviewer archived, allmembers
and parts read back. This verifies the new clock retention and generated-input
classification paths. It is a smoke, not ten-minute or full-matrix qualification.
Independent copied-store integrity/history/drain remains separate.
