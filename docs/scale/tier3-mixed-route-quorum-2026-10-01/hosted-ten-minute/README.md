# Hosted ten-minute R5 quorum route proof

CI36913601073 is terminal SUCCESS at cleanad3a251. Named race PASS668.49s;
package PASS669.514s. Forty-three batches,1,204 completed invocations,13,285
entries and19 confirmed three-node quorum-removing cuts. All workload histories,
retained invariants, physical stream/all64-consumer drain and recovery p99 pass.
All19 original route observations/probes and5,676 raw/recovery samples reverify
unchanged against recorded nanosecond timestamps. Largest recovery terminal
p99 is5.925733370s; largest raw terminal p99 is25.428334946s. Raw samples remain
visible. The fixture uses1s route ping, route-only aliases and production2m sync.

All11 fencing records match counters and confirmed cuts:8heartbeat losses,
2execution losses and1initialization loss. Per-event exact delivery steps,
terminal samples and later invocation acknowledgements are retained. One
already-terminal duplicate is distinguished from the ten records whose terminal
follows fencing. There are103 repair attempts:102 acknowledged (22start,
69signal,11suspended) and one uncertain signal publication. The uncertain event
retains its error and may have committed; it is not credited as an acknowledged
repair. All decisions have checked source/decision explanations. No exact
server-side cause is asserted.

This establishes one clean ten-minute quorum-removing slice, not the majority
row, default-ping coverage or full24-hour matrix. Original hosted stores are
not part of the downloaded artifact. Large raw data is losslessly compressed.
