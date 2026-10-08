import json,shutil
from pathlib import Path
repo=Path('/home/exedev/js-wf');base=repo/'docs/scale/graph-parent-worker-2026-10-08'
review=json.loads((base/'review.json').read_text());runs=json.loads((base/'results.json').read_text())['runs'];assert review['all_eight_commands_pass'] and len(runs)==8
rows='\n'.join('| '+run['name']+' | '+str(run['package_passes'][0]['elapsed'])+' | '+str(len(run['top_level_passes']))+' |' for run in runs)
(base/'README.md').write_text(f'''# Graph worker parent notification verification — 2026-10-08

Frozen source `{review['source']}` passes all eight count1/five-minute/two-Go-CPU/512MiB commands. Every one of the {review['tracked_inputs']:,} selected tracked Go/YAML/module/pin inputs matches frozen Git before, after and at review. Source remains unchanged throughout verification.

| Command | Package seconds | Top-level groups |
| --- | ---: | ---: |
{rows}

## Verified behavior

Native and modeled worker constructors bind their notification client to the selected graph store. Existing transports and observer are copied, and the caller's original client stays unchanged. Terminal execution, replay and held/owned duplicates therefore use canonical parent lifecycle and generation checks. Missing-unconfirmed parents, unknown graph metadata and corrupt duplicate consumption prevent ACK. Canonical retirement/replacement permits late-notification suppression. A forged legacy parent tombstone cannot suppress a valid current graph parent.

All22 prepared native R1/R3 worker parent cases pass normal/race: active and uninitialized parents, forged state/journal, purge fence, retirement, missing retired invocation, replacement, unconfirmed missing invocation, and source-deleted inline/external/corrupt consumption. Healthy foreign child lease bytes/revisions remain unchanged and no handler/effect executes. The default native constructor selects graph mode without injecting a graph client. The model adds an uncertain parent root read and checks ACK/NAK, exact notification bytes, retained source-deleted duplicates and explicit graph drain.

The full client package passes normal/race, including original-mode preservation, supplied transport/result binding, incomplete configuration rejection and earlier canonical signal/terminal controls. All14 graph journal groups and five selected worker groups pass. Existing actual R1/R3 child transfer/replay and large input/signal/result worker controls still preserve effect count one and production purge behavior with joined workers.

Both the new parent family and existing terminal-worker family complete **10,000 normal** and **1,000 race** schedules each, with exact replay and actual completed-body accounting. All12 new parent modes and18 old terminal modes are covered. Both simulation commands pass every **654 unique pin**. Inventory is now **141 workloads/654 pins**. CI YAML and its15 family names plus corpus row match the compiled inventory; hosted CI completion is not inferred.

## Existing pin migration

Exactly three prior parent-notification traces have refreshed expected transport events: `parent_notify` (seed4), `parent_notify_drop` (seed20), `parent_notify_lost` (seed15). Constructor binding replaces legacy state reads with canonical parent metadata and invocation rechecks. Workload, seeds, all decision lists, original fault injection and ACK/NAK assertions remain unchanged. Every other639 previous pin remains byte-for-byte unchanged. Twelve new pins cover the new family. The original corpus failure and original pin bytes remain preserved in development logs and Git350a447; no old failure is reclassified as success.

- [Pin hashes and lineage](../graph-parent-worker-2026-10-08-development/pin-migration.json)
- [Complete source/event/inventory review](review.json)
- [Exact commands, environments and timings](results.json)
- [Source before](source-before.json) and [source after](source-after.json)
- [Runner](executed-regression.py) and [reviewer](executed-review.py)
- [Development failures and limits](../graph-parent-worker-2026-10-08-development/)

## Remaining requirements

Read rechecks are not atomic publisher/purge fencing. Incoming start/signal publication and staging remain legacy; the selected graph runtime must cover parent and child lifecycles. Mixed legacy-parent rollout is not qualified. Native and modeled histories are prepared, actual worker replacements are joined and graph drain follows explicit fixture retirement/expiry. Legacy queue/invocation/staging work remains retained. These controls do not establish process/storage crash or whole-queue drain qualification.

Canonical publication/state/fallback-timer/tombstone/snapshot/continuation/import/history/projection/CLI/deployment migration, complete current141 simulation and all original native reader capacity/concurrency/partition/process/storage/power-loss/scale/matrix/24h/million physical-drain/dependency/default-adoption/release requirements remain open. Production collection stays quiescent; the full goal remains active.
''')
shutil.copyfile(__file__,base/'executed-documentation.py')
p=repo/'docs/implementation-status.md';p.write_text(p.read_text()+f'''

### 2026-10-08 — graph worker parent notification component verified

Frozen `7e61971` passes all eight count1/five-minute/two-Go-CPU/512MiB commands: full client normal/race, all14 graph journal groups, five selected worker groups (including22 prepared native R1/R3 parent cases and actual child/input/signal/result regression), and parent plus existing terminal-worker simulation10,000 normal/1,000 race schedules each with exact replay and actual completed-body accounting. All654 pins pass in both simulation commands. All{review['tracked_inputs']:,} selected tracked inputs match frozen Git before, after and at review. Current inventory141 workloads/654 pins. [Complete reviewed evidence and timings](scale/graph-parent-worker-2026-10-08/).

Both worker constructors now select canonical parent admission, retirement and duplicate confirmation, covering terminal execution/replay and held/owned duplicates. Native fixtures preserve healthy foreign leases and execute no handler/effect. Missing-unconfirmed/unknown/corrupt parents prevent ACK; canonical retirement/replacement suppresses late notification. Actual child transfer/replay still preserves effect count one with production purge and joined workers.

Exactly three existing parent-notification pins refresh transport events while preserving original seeds4/20/15, workload, every decision and dropped/lost reply/ACK/NAK expectations. Originals remain at350a447 and their prior corpus failure is retained. Every other639 old pin is unchanged;12 new pins cover the parent family. This is a documented constructor behavior migration, not a gate or failure-verdict change.

Incoming canonical start/signal publication/staging and atomic publisher/purge fencing, mixed legacy-parent rollout, remaining state/fallback-timer/tombstone/snapshot/continuation/import/history/projection/CLI/deployment, complete current141 simulation and all original native capacity/partition/crash/power-loss/scale/matrix/24h/million physical-drain/dependency/default-adoption/release requirements remain open. Production collection stays quiescent; the full goal remains active.
''')
p=repo/'docs/implementation-plan.md';p.write_text(p.read_text()+'''

## Graph worker parent notification clients verified — 2026-10-08

Native and modeled graph worker constructors now bind notification clients to the selected graph store, covering completion, replay and held/owned duplicates. Canonical parent retirement/replacement suppresses late notifications; missing-unconfirmed/unknown/corrupt parent histories prevent ACK. Frozen `7e61971` passes full client normal/race,14 graph journal/five worker groups,22 native R1/R3 parent controls,10,000 parent and terminal-family normal/1,000 race schedules each, and every654 pin. Exactly three existing transport expectations change with unchanged seeds/fault choices and preserved original traces;639 other old pins remain unchanged. [Reviewed evidence and scope](scale/graph-parent-worker-2026-10-08/).

Continue canonical incoming start/signal publication and staging with atomic publisher/purge fencing, remaining state/fallback-timer/tombstone/snapshot/continuation/import/history/projection/CLI/deployment. Mixed legacy-parent rollout, full141 current-source simulation and all original actual native process/storage/power-loss/partition/capacity/scale/matrix/24h/million physical-drain/dependency/default-adoption/online GC/release requirements remain open. Prepared histories and joined replacements do not reduce the original gates or complete the full goal.
''')
