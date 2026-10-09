# Canonical graph KV visibility — 2026-10-09

Graph visibility uses `visibility.WithGraphJournal(store, queryBucket)` and a separately provisioned KV bucket. The standard runtime state/lease/assignment/membership/reconciler/clock and legacy view buckets are rejected. Use one dedicated query bucket per selected graph namespace. Admission opens existing storage; it does not provision it. PostgreSQL graph namespace integration remains incomplete and is explicitly rejected. Legacy PostgreSQL/KV behavior remains separately supported.

Provision the query bucket explicitly with `js.CreateKeyValue(ctx, jetstream.KeyValueConfig{Bucket: "WF_GRAPH_VIEW", Replicas: 3})`, alongside the existing runtime and canonical graph stores. For the CLI, select the same three graph store flags and expected replicas as the worker, then add the query bucket:

```sh
wf -graph-authority-stream WORKFLOW_GRAPH_AUTH \
  -graph-authority-prefix wf.graph.workflow \
  -graph-object-bucket WORKFLOW_GRAPH_OBJECTS \
  -graph-view-bucket WF_GRAPH_VIEW -interval 100ms project
```

Use those same store/bucket flags for `list [status]`, `-rebuild list`, `-limit 1..1000 [-after cursor] list`, `-attribute key=value list` and `lag`. Existing graph `describe`/`export-journal` commands continue to inspect pinned history directly and do not need the query bucket.

## Source and projection semantics

Canonical catalog discovery captures a finite watermark before reading roots, so quorum witnesses cannot extend a cycle indefinitely. Each bound native invocation pointer must match the confirmed canonical Start. Describe pins the matching generation, validates the Started input hash and journal sequence/epoch boundaries, derives status and search attributes from canonical records and validates terminal generation/kind. It reads no legacy journal or compatibility state and does not fetch the Start input body. Retirement/purge removes rows and indexes on rebuild; unknown catalog/source/lease observations cannot certify absence or prune the view.

Background refresh uses a process-local cache of rows this projector itself verified. Unchanged generation/schema/journal count/status reuses those verified bytes and repairs corrupted query rows without acquiring another pin. Mutable query bytes cannot seed that cache. Changed history is read again. Explicit `Rebuild` rereads history. A live delivery lease only defers history pins; it cannot authorize status or payloads. While deferred, retain the verified prior row or a provisional queued row from the validated source pointer. Release/expiry permits catch-up. These views remain approximate and rebuildable; workflow decisions and result reads use canonical runtime APIs.

Graph records currently lack a commit timestamp. `Updated` is the time this history was observed; unchanged cached refresh retains it. Graph `Lag` counts discovered bound workflows with missing/stale generation, schema, journal count or status, plus discovered retired/purging workflows whose rows remain. It does not count reader/quorum-witness authority traffic as pending workflow events. This is distinct from the legacy consumer backlog metric. Background library refresh defaults to one second; the CLI uses `-interval` (default 100 ms). Full catalog refresh and small-deployment KV query costs still need scale qualification; this is not an event-consumer or large-deployment PostgreSQL adoption claim.

## Evidence and retained failures

`final-cli-normal.jsonl` / `final-cli-race.jsonl` verify native R1 and R3 domain operation with the actual CLI project subprocess at 100 ms cadence while the worker processes roughly 5 MiB Start/Signal payloads. They require canonical completion despite forged compatibility state, graph list/lag, bounded rebuilt pagination excluding a forged legacy row, online/offline replay after source/blob deletion, manual terminal restoration, cancellation, purge, one effect, no legacy journal writes, correct domain APIs and clean project SIGTERM exit. Existing default/domain operator commands also pass.

`reader-recovery-controls-normal.jsonl` / `reader-recovery-controls-race.jsonl` verify six deterministic canonical visibility cases, configuration admission, pagination and existing retained-reader controls. Cases cover catalog uncertainty, forged source, held/unknown lease hints, attrs/status, no input body reads, retired row/index cleanup and witness-independent lag. The ordinary case corrupts query bytes, then requires the process's verified cache to restore them without pins. Later `namespace-controls-race.jsonl` adds state/lease/assignment sink rejection. Small test fixture compilation and retirement-precondition failures remain retained.

The original `cli-race.jsonl` fails R1 when repeated pins cause large Signal publication to exceed its unchanged deadline. Worker lease hints alone cannot protect unleased client publications. `cached-cli-race.jsonl` passes after verified-row reuse; the final subprocess controls pass too. Removing reuse through the retained overlay makes `counterfactual.jsonl` fail the deterministic zero-pin/cache-integrity control. Every prior run keeps its source scope.

These are development component checks, with selected source observations captured after compilation; no frozen-source/full-suite claim is made. The full normal acceptance and currently live race campaign at `9a1ccdc` exclude this migration. Complete current/extended qualification, PostgreSQL namespaces, durable event/large-scale projection qualification, continuation/snapshot/import/deployment/production-GC adoption and every original remaining native/scale/actual24h/million physical-drain/default-adoption/release requirement remain required.

The earlier `final-controls-race.jsonl` fails the existing native consumer-deletion reader control: after deletion succeeds, a quiet read's `Info` sees the consumer absent during SDK recovery. The reader now retries that observation without certifying source exhaustion. A deterministic retained-delivery control passes; the old reader overlay fails it. The corrected native R3 retained-source control keeps all 600 records and its original deadline. Other source/iterator/info errors still propagate. This separate legacy reader fix is included in the final controls; no fixture deletion check or gate was relaxed.
