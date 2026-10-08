# Canonical graph retention verification — 2026-10-08

Frozen source `5d6e17471e6b864c98e64ca91149498e74a54ca1` passes twelve primary and two supplemental count1/five-minute/two-Go-CPU/512MiB commands. Every one of the 1,534 selected tracked Go/YAML/module/pin inputs matches Git before, after and at review. Source remains unchanged throughout the run.

| Command | Package seconds | Top-level groups |
| --- | ---: | ---: |
| retention-normal | 11.879 | 9 |
| wf-normal | 1.153 | 60 |
| reconcile-normal | 35.967 | 29 |
| journal-normal | 6.991 | 14 |
| worker-normal | 25.143 | 4 |
| sim-normal | 179.036 | 3 |
| retention-race | 31.154 | 9 |
| wf-race | 15.322 | 60 |
| reconcile-race | 45.345 | 29 |
| journal-race | 47.375 | 14 |
| worker-race | 83.39 | 4 |
| sim-race | 40.11 | 3 |
| boundary-normal | 0.119 | 1 |
| boundary-race | 1.341 | 1 |

## Verified behavior

The standard retention runs pass all nine ordinary groups and explicitly skip the opt-in native blob-boundary group. The acceptance review detected that skip; two supplemental runs set fresh artifact directories and pass both quiescent and deliberately unsafe refresh controls in normal/race. Full retention coverage is established across those commands, and the unsafe legacy collector counterexample remains reproduced. Workflow SDK and reconciler packages pass normal/race; all 14 graph journal groups and four selected worker/provenance groups pass. Native R1/R3 validate all 22 interruptions after committed purge stages, resume idempotently, retain an already pinned external result, reject new readers at the fence, preserve reused invocation/signal data against an old explicit target and drain graph object chunks after reader release/expiry. The interruptions are controlled errors, not actual process/storage crashes.

Two actual graph retention workflows record the target invocation and graph-mode Run input hash before completing production purge. Eight native sync/async child transfer cases now run production purge: pre-transfer source retirement is rejected; durable parent-owned bytes allow source purge, collection and parent replay without another child effect. Parent terminal purge then drains its graph. Worker replacements are joined.

All 32 inline/external parent ownership cases pass. Runtime call ordering, explicit consumed-child provenance, exact generation/ref/hash/terminal bytes and owned graph edges govern child retirement. Ordinary signals remain opaque. Fencing/reuse controls verify existing readers retain their exact graph while new admission is blocked.

The new seeded family completes **100,000 normal** and **1,000 race** schedules, each with exact replay, actual completed-body accounting and all 28 modes covered. Both simulation commands pass every **611 unique regression pin**. All 583 previous pins remain byte-for-byte unchanged; 28 new pins cover canonical outcomes, absent/forged/older mirrors, pending/uninitialized/corrupt/wrong generations, dropped/lost fence/retirement/deletion/publication operations, retained readers and ID reuse. Current inventory is **139 workloads/611 pins**.

CI YAML and the 13 family names plus corpus row match the compiled inventory. Each family gets a separate five-minute/count1/1,000-seed normal/race job with two Go CPUs and 512MiB. Hosted runner completion is not established by this local review.

## Evidence

- [Independent event/source/inventory review](review.json)
- [Exact commands, environments and package results](results.json)
- [Source before](source-before.json) and [source after](source-after.json)
- [Runner](executed-regression.py) and [reviewer](executed-review.py)
- [Preserved development failures](../graph-purge-2026-10-08-development/)

## Remaining requirements

The seeded model prepares canonical terminals and retries direct purge calls; it does not execute the durable retention workflow. Negative fixtures retain dependent work and modeled unrelated dispatch/native hints remain pending. Graph drain is not whole-queue drain. Parent history scans are linear and fail closed on reader expiry. Permanent metadata growth/census cost, mixed-version deployment and production online GC are not qualified.

Canonical invocation/input/signal publication, fallback-timer/tombstone discovery, snapshot/continuation/import/history/projection/CLI/deployment migration, complete current139 simulation, native reader capacity/concurrency/partitions/process/storage/power-loss/scale and every original full matrix/24h/million physical-drain/dependency/default-adoption/release gate remain open. Production collection remains quiescent and the full goal stays active.
