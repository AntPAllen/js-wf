# Recovery before the first canonical checkpoint pointer

## Changes

Canonical lifecycle metadata now exposes its absolute journal tail. With explicit v5 configuration, the continuation scanner treats a bound, nonterminal `StepCompleted` tail without a checkpoint pointer as an interrupted completion. It defers held delivery leases and validates the native source token/generation and exact captured recovery sequence before publishing a fresh wakeup. The tail does not classify the completed step as a checkpoint or authorize frame access: the worker reads owned history and validates any frame. Ordinary completed SDK steps may receive the same safe recovery wakeup. Other no-pointer tail kinds are skipped.

If a newer completion changes the captured tail, the client rejects the observation and the scanner confirms the changed sequence before advancing. Publishing the matching checkpoint pointer during validation preserves the same completion sequence and remains eligible. Later waits, terminal/retired/purging states and uncertain decisions retain their previous handling. Discovery still opens no payloads and acquires no reader pins.

## Executed development evidence

- `model-race.log`: all 27 modeled recovery cases pass, including 12 new no-pointer cases: ordinary and checkpoint completion, lease deferral/uncertainty, forged source, publication before/after commit, later wait, wait changes before/during source validation, changed completion tail and matching pointer publication. Every case explicitly requires zero forbidden payload reads/root CAS attempts. Dry-run, fresh retries within dedup, confirmed-prefix recovery and v4 rejection remain. All four bounded catalog scanners pass at 16 seeds. Both shared bound Start client controls pass.
- `native-race.log`: original published and new unpublished checkpoint handoff variants pass in native R1/R3-domain clusters. The unpublished variant never installs a pointer before its production repair loop dispatches; it then resumes the owned frame and executes two stages with large state/result bytes, one effect and a terminal duplicate. Held leases defer and repeated queue purges are repaired. Loop shutdown is joined and events retain invocation/completion identity. Public continuation constructor admission is still rejected in both option orders.
- `final-sdk-flow-race.log`: production SDK initial handler executes `RunOnce`, stores state and calls `Continue`; subsequent named stages restore input/state/locals, perform their own effect and publish the next checkpoint. Each boundary's queue is purged and recovered through the scanner. Both native configurations finish with result `43`, one invocation of each handler, exactly two effects, consecutive absolute journal indices, one terminal record and no legacy journal writes. The existing legacy global journal limit/terminal slot control also passes.
- `sdk-partition-race.log`: the same SDK workflow finishes through actual native queue consumption by the production `RunPartition` worker, with joined cancellation/close before verifying handler/effect counts. Both native configurations pass; a separate fenced duplicate cannot reenter any handler. This exercises autonomous stage scheduling with normal handoff publication.

`initial-race.log` retains the first metadata/native check, and `sdk-flow-race.log` the initial direct SDK flow. `initial-native-compile.log` retains a test variable naming collision, corrected before native execution.

`executed-review.py` checks required terminal results and hashes inputs observed at review. These are development component checks, not frozen full/extended qualification. The normal/race suite qualified at `9a1ccdc` excludes these changes. Shared simulation inventory remains 148 families/748 pins; the new controlled cases are external package component tests, not additional full-suite seeded families.

## Remaining requirements

The unpublished native fixture prepares the initial checkpoint explicitly and releases the old lease; it does not kill an OS process. The separate SDK flow generates actual checkpoints and the partition variant consumes queue deliveries, but neither injects process/server failure during SDK checkpoint publication. Full worker-kill/failure-cut/limit/audit integration remains required.

Archival prefix compaction, bounded worker reference loading, collection-safe materialization, full offline/import integration, v5 CLI/deployment and public graph continuation admission remain incomplete. Every original current-full/extended/runtime/native/scale/actual24h/physical-drain/adoption/release gate remains open.
