# Canonical pending Start discovery and repair development

Added bounded retained-sequence authority discovery, quorum-confirmed lifecycle inspection, a token-bound canonical Start scanner and the `graph-start` kind in the existing lease/cursor repair loop. Discovery does not load the complete catalog or an input body. The scanner recovers pending Starts before WF_INV exists and bound Starts with no runtime history, leaving uncertain attempts unresolved while certifying only completed prefixes. Retirement/replacement skips require a fresh witness and are reported as superseded without counting an enqueue.

Review found a definite recovery race in the preceding component: after reading a valid Start, RecoverStart ran ReserveStart again and could create a replacement after concurrent retirement. Recovery now resumes the captured token without reserving; `RecoverStartAttempt` rejects a different token. A controlled reader-release retirement test demonstrates the cut and verifies no new pending generation appears. Await now waits on confirmed pending canonical input when WF_INV is absent; a ready root with missing source reports uncertainty.

Development native R1/R3 reopen the graph and execute the actual fenced repair loop and production worker for reserved/source-committed/bound-without-enqueue cuts, with three effects total per replica setting. These are prepared durable cuts, not process kills. Models execute production scanner/client/worker paths across22 modes,1,000 schedules with exact replay and explicit fixture graph drain. Source remains to be frozen and qualified.

Development failures remain preserved: first unit assumed a root would stay at the same retained sequence after quorum reads; native drafts omitted the partition-count argument, then exposed pending Await behavior and failed to wait for R3 metadata-leader readiness; seeded drafts used the wrong replay-dispatch variable, missed capture output directories, omitted next_root from the fault-injector allowlist, and supplied an Await test port missing State. The seed-inventory tool directly inspects test bodies, so both wrappers now construct the tracked seed iterator in the test before passing it to their shared harness. No NATS cause or passing prefix is inferred for failed commands.

Fifteen Start traces change and three remain byte-identical. Updated Start traces reflect removal of the reservation read during recovery; all prior seeds and decisions remain unchanged. Original bytes remain at13b80f2; [migration ledger](trace-migration.json) binds old/new hashes. All670 other old pins are byte-identical.22 new scanner pins extend the corpus to710 and the compiled inventory to144 workloads. Frozen normal/race qualification is pending; complete current-source simulation, canonical Signal publication, remaining migration and original release gates remain open. Production collection remains disabled.


## First frozen qualification failed

Frozen `a646f02` passed ownership normal (58 groups), full client normal (7), full reconciler normal (32), and graph journal normal (16), then worker normal failed in `TestNativeGraphWorkerParentNotifications/R3/consumed_corrupt`: the setup KV.Get returned `nats: key not found` immediately after lease acquisition, before worker construction/dispatch. The other five worker groups passed, including the new canonical Start recovery group, but this worker command and campaign remain failed. The remaining nine commands were not run. All selected source inputs stayed unchanged.

The parent fixture now observes its exact initialized lease value/revision within three seconds before starting dispatch, using only reads and no renewal. Unexpected owners/epochs/values or other read errors still fail; the original one-minute test context, five-minute command gate, foreign-owner preservation and zero handler/effect requirements remain unchanged. No cause is inferred for the initial missing KV read. A fresh committed-source qualification is required and will be stored separately.


## Bound Start repair contention fix; qualification pending

The second frozen campaign at `030b328` failed in reconciler race after nine passing commands; four later race commands were not run. Its [original logs and source snapshots](verification-after-lease-setup/) remain preserved. Subsequent instrumented native runs showed repeated Started-append CAS losses against the repair path's input reader acquire/release. Repeated bound repair advanced the same logical head while workers staged append objects. No provider defect is inferred.

Bound enqueue repair now checks lifecycle metadata, the captured token/invocation and exact WF_INV source pointer, then rechecks lifecycle before enqueue. It opens no input reader and does not change the logical head. Pending recovery still validates owned input before source publication/binding; workers still validate exact binding and owned input before execution. A retirement after the final observation can at most create a stale queue delivery that the worker rejects. Missing or foreign source remains uncertain.

Two deterministic Await regressions cover source-visible-before-binding and missing-source-after-terminal observation: neither alone proves a purge. A 32-seed interleaving test runs repair after staging Started but before head CAS; old pinned recovery is an explicit negative control and fails the append, while production bound scanner repair permits it. The development native race run passes R1 in3.43s and R3 in8.71s, each with3 effects, without changing the10ms loop cadence,1-minute context or5-minute command deadline. The22-mode scanner model completes1,000 exact-replay schedules; all710 pins pass. Nine scanner pins change with identical seeds/decisions;13 scanner pins and all688 older pins stay unchanged relative to030b328. [Migration and development evidence](development-contention-fix/). A new frozen qualification is required; full-goal and original release gates remain open.


## Completion result retained across definite graph CAS contention

The third frozen campaign (`c194e8c`) remains failed: R1 completed3 workflows but executed4 effects after a StepCompleted CAS loss and redelivery. The exact concurrent authority operation is unconfirmed. [Original source and events](verification-after-contention-fix/) are retained.

The production worker now retries a definite graph CAS conflict up to16 times within the existing15-second append context, renewing its lease before every retry. The same entry and computed result are kept; GraphStore validates the invocation, lifecycle, exact tail/index and epoch afresh on every preparation. Unknown outcomes and revoked grants are not retried. Primitive GraphStore.Append semantics are unchanged.

A32-seed production-worker interleaving obtains/releases an external reader between completion staging and CAS, then proves one effect and exact generated-trace replay. Its unknown-publication control requires redelivery and2 effects. Go overlay of the original c194e8c worker fails every selected reader case with2 effects while the unknown controls pass. Native R1/R3 race recovery,1,000 original worker-family schedules and all710 pins pass in development. The collector_wins trace alone changes: a definite conflict now retains the result and completes with1 effect; the old2-effect expectation was caught and preserved as failed before correction. Seeds/decisions and all709 other pins stay unchanged. The first trace-migration script selected unrelated terminal-worker names and stopped; original bytes/hashes were then recovered from Git and the bounded six-mode migration verified. [Development evidence and migration ledger](development-completion-retry/).

CI includes the staged-append reconciler control and the production-worker reader interleaving. Fresh frozen normal/race qualification remains required; full migration, original fault/scale/matrix/24h/million physical-drain and release requirements remain open.


## Start repair component accepted at736604d

All16 frozen normal/race commands pass, with independent source, test-group, schedule, pin, migration and failed-campaign checks. Three model families each complete10,000 normal/1,000 race schedules, and all710 pins pass twice. Native R1/R3 prepared recovery requires exactly3 effects. [Accepted qualification and exact scope](verification-after-completion-retry/). Complete current144-family simulation and every remaining original plan requirement stay open.
