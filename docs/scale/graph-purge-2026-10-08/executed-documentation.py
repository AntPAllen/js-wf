import json,shutil
from pathlib import Path
repo=Path('/home/exedev/js-wf');base=repo/'docs/scale/graph-purge-2026-10-08'
review=json.loads((base/'review.json').read_text());runs=json.loads((base/'results.json').read_text())['runs'];assert review['all_fourteen_commands_pass'] and len(runs)==14
rows='\n'.join('| '+run['name']+' | '+str(run['package_passes'][0]['elapsed'])+' | '+str(len(run['top_level_passes']))+' |' for run in runs)
text=f'''# Canonical graph retention verification — 2026-10-08

Frozen source `{review['source']}` passes twelve primary and two supplemental count1/five-minute/two-Go-CPU/512MiB commands. Every one of the {review['tracked_inputs']:,} selected tracked Go/YAML/module/pin inputs matches Git before, after and at review. Source remains unchanged throughout the run.

| Command | Package seconds | Top-level groups |
| --- | ---: | ---: |
{rows}

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
'''
(base/'README.md').write_text(text);shutil.copyfile(__file__,base/'executed-documentation.py')
p=repo/'docs/implementation-status.md';p.write_text(p.read_text()+f'''

### 2026-10-08 — canonical graph retention component verified

Frozen `5d6e174` passes twelve primary plus two supplemental count1/five-minute/two-Go-CPU/512MiB commands: retention, workflow SDK and reconciler normal/race, all14 graph journal groups, four selected native worker/provenance groups and purge simulation100,000 normal/1,000 race schedules with exact replay and actual completed-body accounting. All611 pins pass in both simulation commands; all583 previous pins remain unchanged. All{review['tracked_inputs']:,} selected tracked inputs match Git before, after and at review. Current inventory139 workloads/611 pins. [Complete reviewed evidence and timings](scale/graph-purge-2026-10-08/).

Native R1/R3 cover22 committed-stage cuts, retained reader survival, explicit old-target rejection after ID reuse, physical graph chunk drain, and two actual durable retention workflows with recorded target generation. Eight actual child transfer/replay cases now use production purge and preserve effect count one. All32 parent ownership controls and graph fence/reuse checks pass. Mutable development failures remain preserved separately. The first acceptance review rejected an opt-in blob-boundary skip; explicit normal/race supplements cover that group, preserving the unsafe legacy collector counterexample.

This verifies graph-aware purge and child retention coordination in that component scope. Prepared native cuts and joined worker replacements do not qualify process/storage crashes. Model negative fixtures and unrelated dispatch are retained; explicit graph drain is not whole-queue drain. Canonical publication/state/snapshot/continuation/import/history/projection/CLI/deployment, complete current139 simulation, native reader capacity/partition/crash/power-loss/scale and all original matrix/24h/million physical-drain/dependency/default-adoption/release requirements remain open. Production collection stays quiescent; the full goal remains active.
''')
p=repo/'docs/implementation-plan.md';p.write_text(p.read_text()+'''

## Canonical graph retention verified — 2026-10-08

Graph purge now validates canonical terminal data and exact parent-owned child consumption, fences new graph admission before dependent deletion, preserves old readers through graph retirement, retires generation-bound native hints, writes compatibility tombstones and removes invocation last. Durable graph retention records an explicit target generation before retries. Frozen `5d6e174` passes retention/workflow/reconciler normal/race with explicit supplemental coverage of the opt-in native blob-boundary group,14 graph journal groups/four selected worker groups,100,000 purge normal/1,000 race exact-replay schedules and all611 pins; all583 prior pins remain unchanged. Native R1/R3 cover22 committed-stage cuts, two actual retention workflows and eight production child transfer/purge/replay cases. [Reviewed evidence and limitations](scale/graph-purge-2026-10-08/).

Continue canonical invocation/input/signal publication and fallback-timer/tombstone discovery, snapshots/continuations/import/history/projections/CLI/deployment. Full139 current-source simulation, actual native process/storage/power-loss/partition/capacity/scale, every original native matrix/24h/million physical-drain/dependency/default-adoption/release requirement and production online GC remain uncompleted. Stage cuts and joined replacements do not reduce these gates or complete the full goal.
''')
