# Pressure failure separated from already modeled worker recovery

[Run 37177259306](https://github.com/AntPAllen/js-wf/actions/runs/37177259306), failed job **111362365148**, source **`f149b873b349f6def63b50a29d79abb1c142739f`**, fails `TestLeaseAppendPressureConsumerTrafficFixedPlacement` in **26.98 s** (package **26.992 s**). Raw artifact **11293764137**, ZIP **38,045 bytes**, is retained completely with terminal metadata/logs and exact executed review/preservation scripts.

Three healthy rows complete. In the fourth, delayed baseline row, owner `dtrue-atrue-w8-2` completes six mandatory renewals and then receives `journal.ErrUnknown` from a pre-publication tail lookup returning API **503/10008**. The other seven owners complete all 48 calls. This row has **no consumer traffic and no contender probes**, so neither load is established as the cause. The fixture deliberately stops on any operation error; it does not execute worker redelivery. No retry, timeout or acceptance gate is changed to turn this failed comparison green.

Later monitoring confirms expected lease/state leaders on node 0, journal leader on node 1, and current surviving peers. Node 2 refuses its monitoring connection, matching the intentionally stopped node. These later snapshots do not establish internal server state at the earlier failed request. The underlying server cause remains unconfirmed. Actual failed workload executable, physical stores and producer pre/post source captures were not uploaded; selected source files are obtained from exact executed Git. No independent store reopening or corruption conclusion is claimed.

## Existing source-identical deterministic coverage

The [accepted full Tier1 race proof](../snapshot-timeout-cause-2026-10-04/hosted-race/) already passes `TestSeededWorkerTailLookupRecovery` over **all contiguous seeds 1–1,000**. Its retained raw events verify all **12 combinations** of four worker stages (Started, StepRequested, StepCompleted, terminal) and one to three injected tail API **503/10008** errors. Failed deliveries preserve the existing journal prefix, retain pending dispatch, release the lease and publish no outcome; replacement workers drain delivery and produce a checked terminal journal/result. A step whose completion append fails may run again, as required by the documented effect contract.

Every one of **685 selected runtime/simulator/Tier1 producer inputs** is Git-byte-identical between that accepted source `283ba32` and failed CI source `f149b87`. Actual retained race binary, raw suite/event files, primary independent review and runtime ledger match the committed accepted proof manifest. The selected workload's exact events, coverage counts and new cross-revision ledger are retained here. This reuses verified qualification evidence; no duplicate model or cluster trial is launched.

This separates the runtime's modeled response to the observed API error from the unresolved server condition. It neither reproduces that server condition nor qualifies the failed six-row steady-pressure comparison. Full matrix, current normal100k and actual original 24-hour qualification remain open.

## Preservation

All **21** proof members are SHA256-checked by readback before atomic publication of the **118,155-byte** archive. It contains all original uploaded files, exact source selections, terminal API/logs, independent review, accepted workload events/suite and runtime equivalence proof. Verify `manifest.json` before extraction. The larger accepted race executable/full raw suite remains in its separately committed complete proof; it is hash-bound here rather than duplicated.
