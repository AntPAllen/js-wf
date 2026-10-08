import json,shutil
from pathlib import Path
repo=Path('/home/exedev/js-wf');base=repo/'docs/scale/graph-signal-client-2026-10-08'
review=json.loads((base/'review.json').read_text());runs=json.loads((base/'results.json').read_text())['runs'];assert review['all_eight_commands_pass'] and len(runs)==8
rows='\n'.join('| '+run['name']+' | '+str(run['package_passes'][0]['elapsed'])+' | '+str(len(run['top_level_passes']))+' |' for run in runs)
(base/'README.md').write_text(f'''# Canonical graph signal client verification — 2026-10-08

Frozen source `{review['source']}` passes all eight count1/five-minute/two-Go-CPU/512MiB commands. All {review['tracked_inputs']:,} selected tracked Go/YAML/module/pin inputs match frozen Git before, after and at review. No source changed during verification.

| Command | Package seconds | Top-level groups |
| --- | ---: | ---: |
{rows}

## Verified behavior

The complete client package passes normal/race, including all20 prepared native R1/R3 signal cases and earlier canonical terminal controls. Graph lifecycle governs admission despite invalid compatibility state and a forged WF_JRN terminal. RequireRunning validates canonical terminal success/failure/cancellation. Deleted source signals can be confirmed through an exact canonical consumed record and its inline/external owned bytes. Bad consumption hashes, missing owned edges, purge fences and missing retired invocations reject. External native duplicate payloads exceed the ordinary inline signal threshold.

All14 graph journal groups and four selected worker/provenance groups also pass normal/race. Actual R1/R3 graph workers still recover large input/signal/result bytes and child transfer/replay through production purge while preserving effect count one. These are joined replacements, not process kills.

The new signal-client family completes **100,000 normal** and **1,000 race** schedules with exact replay and actual completed-body accounting. All31 modes are observed, including uninitialized histories, canonical terminal outcomes, forged compatibility state/journal, retirement and ID reuse, pre/post-publication invocation replacement or fencing, unknown graph reads/pin outcomes, inline/external duplicate provenance, malformed/unowned/doubled records, source publish reply loss, malformed acknowledgements and foreign retained subjects.

Both simulation commands pass every **642 unique regression fixture**. All611 previous pins remain byte-for-byte unchanged;31 new pins cover the new family. Source inventory is now **140 workloads/642 pins**. CI YAML and its14 family names plus corpus row match the compiled inventory. Hosted CI completion is not inferred.

## Evidence

- [Source/event/inventory review](review.json)
- [Exact commands, environments and timings](results.json)
- [Source before](source-before.json) and [source after](source-after.json)
- [Runner](executed-regression.py) and [reviewer](executed-review.py)
- [Preserved development failures](../graph-signal-client-2026-10-08-development/)

## Limits and remaining work

Read rechecks are not an atomic publisher/purge fence. An acknowledged signal racing an observed replacement/fence returns its sequence and an error without a wakeup; the signal remains retained. Start/incoming signal publication, external staging and graph-aware worker-created parent notifications remain legacy paths requiring migration. Native and modeled consumed histories are prepared; actual worker regression is separately covered above. Linear scans fail closed on reader expiry/uncertainty. Modeled graph drain uses explicit fixture retirement/expiry; legacy invocation/signal/run data and staging objects remain retained. This is not whole-queue or production online GC qualification.

Complete canonical publisher/consumer/state/fallback-timer/tombstone/snapshot/continuation/import/history/projection/CLI/deployment migration, full current140 simulation and all original native reader capacity/concurrency/partition/process/storage/power-loss/scale/matrix/24h/million physical-drain/dependency/default-adoption/release requirements remain open. Production collection stays quiescent and the full goal remains active.
''')
shutil.copyfile(__file__,base/'executed-documentation.py')
p=repo/'docs/implementation-status.md';p.write_text(p.read_text()+f'''

### 2026-10-08 — canonical graph signal client component verified

Frozen `373e6a7` passes all eight count1/five-minute/two-Go-CPU/512MiB commands: complete client normal/race (including20 prepared native R1/R3 signal cases), all14 graph journal groups, four actual worker/provenance groups, and signal-client simulation100,000 normal/1,000 race schedules with exact replay and actual completed-body accounting. All642 pins pass in both simulation commands; all611 previous pins remain unchanged. All{review['tracked_inputs']:,} selected tracked inputs match frozen Git before, after and at review. Current inventory140 workloads/642 pins. [Complete reviewed evidence and timings](scale/graph-signal-client-2026-10-08/).

Graph-mode admission, RequireRunning, duplicate consumption and retirement checks now use canonical lifecycle/history/owned bytes, with no legacy mirror fallback. Native controls validate source-deleted inline/external duplicates, terminal outcomes, invalid compatibility mirrors, missing graph edges, purge fences and generation retirement.31 modeled cuts include lifecycle/identity races around publication, unknown graph replies, forged/doubled consumption and lost signal replies; rejection never authorizes a wakeup. Previously accepted actual worker/child transfer behavior remains covered by this frozen regression.

This closes those signal-client reader/decision paths. Read rechecks do not make publication atomic with purge; start/incoming signal staging/publication and worker-created legacy parent notifications still require migration. Prepared consumed histories, joined native workers and explicit graph retirement/expiry do not prove process/storage crash or whole-queue drain. Remaining canonical state/fallback-timer/tombstone/snapshot/continuation/import/history/projection/CLI/deployment, complete current140 simulation and all original native capacity/partition/crash/power-loss/scale/matrix/24h/million physical-drain/dependency/default-adoption/release requirements remain open. Production collection stays quiescent; the full goal remains active.
''')
p=repo/'docs/implementation-plan.md';p.write_text(p.read_text()+'''

## Canonical graph signal client decisions verified — 2026-10-08

Graph-configured signal admission, RequireRunning, source-deleted duplicate confirmation and missing-invocation retirement checks now use canonical graph lifecycle/history and exact owned payload bytes. Invocation/lifecycle rechecks bracket publication and precede wakeup enqueue. Frozen `373e6a7` passes full client normal/race (20 native R1/R3 cases),14 journal groups/four actual worker groups,100,000 new-family normal/1,000 race schedules and all642 pins; all611 previous pins remain unchanged. [Reviewed evidence and exact limits](scale/graph-signal-client-2026-10-08/).

Continue canonical start/incoming signal publication and staging, graph-aware worker parent notification, remaining state/fallback-timer/tombstone/snapshot/continuation/import/history/projection/CLI/deployment migration. Read rechecks are not an atomic publisher/purge fence. Full140 current-source simulation and all original native process/storage/power-loss/partition/capacity/scale/matrix/24h/million physical-drain/dependency/default-adoption/online GC/release requirements remain open; prepared histories and joined replacements do not reduce those gates.
''')
