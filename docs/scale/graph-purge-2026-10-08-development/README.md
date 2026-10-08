# Graph retention development — 2026-10-08

Canonical graph purge now validates terminal bytes, coordinates child retention through exact parent-owned consumption, fences graph admission before dependent deletion, retires current native timer hints and graph ownership, writes a compatibility tombstone, publishes the purge event, removes invocation last, and clears the compatibility marker. Existing pinned readers survive retirement. Explicit invocation APIs and the graph retention workflow bind retries to a recorded generation.

## Development evidence

- `native-child-initial.jsonl` remains failed: the new validator incorrectly rejected the checksum on an inline child envelope. Fixed by verifying the checksum; all eight R1/R3 sync/async payload cases pass in `native-child-after-inline-hash.jsonl` (16.090s).
- `native-stages-initial.jsonl` remains failed: the final drain assertion counted permanent object metadata as payload chunks. Corrected to count `$O.<bucket>.C.>`; `native-stages-after-chunk-filter.jsonl` passes (3.738s). No NATS defect is inferred.
- `native-workflow-initial.jsonl` records a draft unused-variable build failure. The finalized target assertion reads the durable Run result and input hash. Both actual retention workflows and all 22 committed-stage cuts pass in `native-stages-and-workflow-final.jsonl` (6.148s).
- `parent-ownership-controls.jsonl` passes all 32 inline/external envelope authorization cases (0.065s). Ordinary signals, forged metadata, missing graph edges and wrong ordering cannot authorize child retirement.
- `fence-controls.jsonl` passes terminal admission fencing, existing-reader survival, retirement and generation reuse (0.005s).
- `model-final-pins.jsonl` passes 1,000 exact-replay schedules across all 28 purge modes plus every previous 583 regression pin (2.737s). The initial model log covers an earlier 27-mode draft.

These are mutable development checks, not committed-source acceptance. The new pins extend inventory to 139 workloads/611 pins. Frozen normal/race and extended purge verification follow.

## Scope

Native committed-stage cuts inject an error after a completed operation; they are not process/storage crash tests. Native workers are joined replacements. The seeded model uses prepared canonical terminals and explicitly retries direct purge failures; it does not execute the durable retention workflow. Negative fixtures deliberately retain invocation/dependent data. Graph drain does not prove whole-queue drain. Parent history scans remain linear and fail closed on reader expiry. Production collection remains disabled.

Canonical invocation/input/signal/state/fallback-timer/tombstone discovery, snapshot/continuation/import/history/projection/CLI/deployment migration, complete current-source simulation, native capacity/partition/crash/power-loss/scale and every original matrix/24h/million physical-drain/dependency/default-adoption/release requirement remain open.
