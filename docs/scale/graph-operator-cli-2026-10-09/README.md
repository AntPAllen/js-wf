# Canonical graph operator CLI — 2026-10-09

## Configuration and commands

Use the same explicitly provisioned graph namespace and replica count as the worker:

```sh
wf -url nats://localhost:4222 -replicas 3 \
  -graph-authority-stream WORKFLOW_GRAPH_AUTH \
  -graph-authority-prefix wf.graph.workflow \
  -graph-object-bucket WORKFLOW_GRAPH_OBJECTS \
  -input-file input.json start workflow-type workflow-id
```

All flags precede the command. The three graph flags are required together. Admission opens existing stores, validates configuration and uses the selected `-domain`; it does not provision or import storage. `-journal-encoding` defaults to JSON. Clients and workers must share the same graph configuration with canonical Starts and Signals enabled.

Supported commands:

| Command | Behavior |
| --- | --- |
| `start type id json` | Publish canonical Start; return its handle. |
| `signal type id name key json` | Publish canonical Signal with caller deduplication key. |
| `result type id` | Await canonical terminal result within `-timeout`. |
| `cancel type id` | Request canonical cancellation. |
| `purge type id` | Graph-aware retention purge with `-grace`. |
| `describe type id` | Return invocation sequence and a pinned canonical journal snapshot. |
| `export-journal type id` | Export canonical journal records from that snapshot. |
| `export-replay type id`, `replay type id` | Export owned replay inputs or verify a handler offline; see [canonical replay export](../graph-replay-export-2026-10-09/README.md). |
| `scan-start`, `scan-signal`, `scan-terminal`, `scan-timer`, `scan-suspended` | One bounded manual repair scan; dry-run unless `-apply` is supplied. |

For Start and Signal, `-input-file FILE` replaces the final JSON operand. Files must contain valid JSON and are limited to 64 MiB. SDK/storage limits still apply. Canonical results and journal reads do not trust `WF_STATE` or fall back to `WF_JRN`. Exported journal payload references remain references; this export is not a self-contained replay bundle.

Graph project/list/lag now accept a separately provisioned `-graph-view-bucket`; see [canonical KV visibility](../graph-visibility-2026-10-09/README.md). Legacy journal capacity and tombstone sweep/scan/loop commands report that their migration is incomplete. Assignment commands remain available. Legacy mode remains the default when graph flags are absent. Continuation, fallback timers, imports, production GC and complete deployment/adoption remain open.

## Component evidence

`normal.jsonl` covers configuration rejection and native R1/R3 domain operation. Each native fixture sends a roughly 5 MiB owned Start and Signal payload, requires exact result bytes, forges a compatibility state result and requires the canonical result unchanged, exports terminal history, exercises dry-run scans, cancels a second workflow, purges the completed workflow and requires `ErrPurged`. It checks one effect, no legacy journal writes and domain requests with zero wrong-prefix requests. The worker is joined before purge.

`legacy-normal.jsonl` covers existing operator commands in the default API and a JetStream domain. `race.jsonl` covers both these existing commands and the new native graph controls. These are development component checks. The separate frozen full suites at `9a1ccdc` exclude this later operator CLI work; complete current and extended qualification and every original native/scale/soak/drain/release gate remain required.

## Manual repair effect verification

`manual-repair-corrected-normal.jsonl` and `manual-repair-race.jsonl` additionally delete the terminal projection, require a dry-run to leave it absent, apply `scan-terminal` and require exact canonical terminal metadata restoration. Canonical history remains byte-identical and the effect count remains one. The original `manual-repair-normal.jsonl` failure is retained: its assertion incorrectly expected an inline large result instead of the canonical graph result reference. The corrected assertion compares the projection to the actual terminal journal record.
