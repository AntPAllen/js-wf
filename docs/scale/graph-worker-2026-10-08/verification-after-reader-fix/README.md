# Opt-in graph worker/client integration — focused verification accepted

Code is frozen at **0d63f3e**. All **1,398** selected tracked Go/module/workflow/simulation inputs matched Git before execution, remained unchanged afterward, and were rechecked against the frozen revision during review. Eight sequential commands ran once under their original five-minute limits, GOMAXPROCS=2 and GOMEMLIMIT=512MiB. Raw JSON, commands/environment, before/after inventories and executed runner/reviewer are retained here.

| Component | Normal | Race |
| --- | ---: | ---: |
| Native worker, R1/R3 | 6.634s | 20.457s |
| Eight focused graph journal groups | 9.465s | 59.727s |
| Client package | 0.004s | 1.014s |
| Seeded worker + independent transport helper + all pinned traces | 160.157s | 215.965s |

Normal worker simulation completes **10,000** schedules; race completes **1,000**. Both replay exactly and pass **503** unique pinned traces, including all **70** graph traces. The review rejects failed/skipped tests, race reports, missing replica subtests, altered source, missing command results or incorrect schedule/pin counts. Current-source full **133-workload** qualification is still required; this verifies the worker family and existing pinned regression cases only.

The worker owns Started inputs, consumed signals, step results and terminal payloads through graph entries. Native controls replay large payloads after deleting original legacy input/signal staging, replace three gracefully stopped worker objects, verify a single effect and absence of legacy journal writes, return the graph terminal result, then retire and drain the closed fixture. These are not process/peer SIGKILL or power-loss tests. WF_STATE and WF_INV are removed before graph retirement in the fixture; this does not establish a safe production retention ordering.

The original verification at19a8242 failed reader Close contention. The close-only fix atf81e74c failed reader Open contention. Both attempts remain preserved separately as failed. The accepted fix bounds retries to sixteen **definite CAS rejections**, reads fresh authority before each attempt, revalidates the invocation on Open and local expiry on Renew, and propagates ambiguous mutations without retry. Deterministic controls cover competing releases/acquisitions/renewals, invocation replacement, renewal expiry and unknown replies/readback. This addresses confirmed runtime reader metadata contention; it does not attribute either failure to the NATS server.

Canonical invocation/input publication, signal publication, state/terminal ownership, snapshots, continuations, importer, reconciler, retention/history tooling, public native SDK configuration and deployment migration remain required. Native concurrency/capacity/partitions/scale and actual process/storage crash qualification remain open, as do all original full simulation/native matrices, 24-hour run, million physical drain, dependency/default adoption and release gates. Production collection remains quiescent. This is accepted focused component evidence, not full runtime/release completion.
