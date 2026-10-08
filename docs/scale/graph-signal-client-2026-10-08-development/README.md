# Canonical graph signal client development — 2026-10-08

Graph-mode signal admission now observes canonical lifecycle, validates terminal bytes for RequireRunning, confirms missing duplicate signals through generation-bound consumed history and exact owned bytes, and recognizes retirement without WF_STATE markers. Invocation and graph lifecycle are rechecked before publication and before wakeup enqueue. Legacy clients retain their existing storage path.

Native R1/R3 passes all20 prepared cases in `native-initial.jsonl` (2.612s): running versus forged mirrors/legacy terminal decoys, canonical success/failure/cancellation, inline/external duplicate confirmation after deleting the source signal, bad consumption hash, missing ownership edge, purge fence, and missing retired invocation.

`model-final-pins.jsonl` passes1,000 exact-replay schedules across31 modes plus every611 previous pin (2.590s). A first dispatcher build failed due to a variable typo (`model-initial.jsonl`). The next run failed because the test expected a repeated signal-wakeup message ID to enqueue twice (`model-after-dispatch-fix.jsonl`); corrected to assert the existing deduplication behavior. `model-after-wakeup-fix.jsonl` passes (1.405s). Neither failure is attributed to the runtime or NATS. Final mode fixtures distinguish absent mirrors from forged state/legacy journal cases.

Current inventory becomes140 workloads/642 pins. Frozen normal/race and extended verification follow; mutable development checks are not acceptance.

## Limits

These are lifecycle/history reader migrations. Start/incoming signal publication and unconsumed external payload staging remain legacy. Read rechecks do not make signal publication atomic with graph purge. A post-publication replacement/fence returns the acknowledged sequence and error without a wakeup; it does not retract the retained signal. Parent notifications created by legacy clients are not migrated by this option. Reader scans are linear and fail closed on expiry/uncertainty. Native histories and modeled consumption are prepared fixtures; existing actual graph workers remain a separate required regression. Graph drain follows explicit fixture retirement/expiry and does not drain legacy queues, staging objects or invocation messages.

Complete canonical publisher/consumer/state/snapshot/continuation/import/history/projection/CLI/deployment migration, full current140 simulation and all original native crash/partition/capacity/scale/matrix/24h/million physical-drain/dependency/default-adoption/release gates remain open. Production collection remains quiescent.
