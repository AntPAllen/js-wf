# Graph child-result ownership transfer — frozen verification failed

Frozen code **c1cf238** transfers a runtime child's verified terminal result into its parent's `SignalConsumed` graph publication. The worker matches the recorded `call`/`call_async` declaration and observed invocation parent headers, acquires a terminal reader, validates exact canonical outcome bytes, and stages a fresh parent-owned payload grant. The source view remains pinned through parent publication/readback. No foreign-owned source edge is reused across owners.

The durable declaration records child type, ID, invocation, result reference and hash. Parent replay checks that declaration against the recorded request and parent-owned outcome bytes, then resolves results through the parent's graph. Replay does not open the retired child. A rejected journal-limit signal retains the same declaration and transitive result edge inside its Failed outcome. Unknown source reads/copies/publications do not authorize success; uncertain source pin release can leave a bounded expiring reader.

## Verification scope

Six sequential commands were planned with count1, original five-minute timeouts, GOMAXPROCS=2 and GOMEMLIMIT=512MiB. Normal worker25.646s and client2.769s passed. The third command timed out at300.010s during the10000-schedule child model, so race commands and the frozen pin loop were not reached. All1,459 selected tracked inputs matched frozen Git before and after execution and during failure review. This attempt remains failed; the deadline will not be extended.

- Native parent/child workflows: R1/R3, synchronous/asynchronous calls, external child result and inline child result whose encoded outcome requires external signal staging. Workers stop/join between stages. After transfer, child invocation/state/signal and any staging bytes are removed before child retirement and collection. The exact original child physical payload/entry disappears; parent replay succeeds, the child effect count remains1, legacy journal stays empty, and final graph objects/physical chunks drain.
- Eighteen replay provenance controls reject missing/foreign/duplicate declarations, including rejected journal-limit signals; ordinary user JSON strings remain opaque.
- Existing native input/signal/result/terminal-duplicate controls and the client package guard shared delivery/history and terminal behavior.
- New modeled family: nineteen cuts, 10,000 normal/1,000 race schedules with exact replay. Source terminals are prepared small fixtures, while parent workflow, dispatch, signal drain, ownership transfer, retries, replay and collection execute production code. The source reader preserves bytes during retirement; publication still requires the unchanged parent upload intent TTL. All553 pins must pass; all534 previous pins remain byte-for-byte unchanged.

## Limits and remaining work

This is focused component verification. The model retains the prepared child's dispatch message; graph-object drain does not claim whole legacy queue drain. Native stops are joined replacements, not process kills or power loss. Fixture retirement explicitly establishes legacy identity/state removal ordering.

Production retention must preserve a child generation until the parent publishes its owned copy. The existing purger still reads legacy state/journal, so graph-aware retention and scanner migration remain required. Current invocation and purge identity reads still use legacy stores. Start/input/signal publication, state writers, reconcilers, snapshot/continuation/import/history/deployment and canonical purge ordering remain incomplete. Reader capacity/concurrency, catalog paging/scale, native partitions/crashes/power loss, complete current136-workload simulation, original matrices/24h/million physical drain/default dependency/adoption/release gates remain open. Production collection stays quiescent.

Development errors and their original failed traces are preserved [separately](../graph-child-transfer-2026-10-08-development/). This frozen attempt is failed. Subsequent fixes and qualification must use a fresh evidence directory. Development controls do not convert this timeout into acceptance.

A separate later frozen verification after the provenance fix and bounded pipeline passed; see [its independent evidence](verification-after-provenance/). This original attempt remains failed.
