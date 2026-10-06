# Full chunked cold audit completed; cleanup gate failed

Cleanacc9123 normal4CPU/GOGC500/4GiB fresh verified full400k/4.8M copied fixture.
The exact report is400k invocations/journals/terminal and4.8M entries, with4.8M
once-only journal visits. Audit/reduction completes17.114919600s. Both created
cursor deletions return success; INVcount0 is observed17.115921s. JRNcount2
persists through19.985561s; original20s overall deadline expires20.000712281s.
NativeFAIL37.33s; no owner SIGKILL is injected. This is a completed full retained
read/reduction within20s, but not a passed combined cleanup or fault gate.

The residual consumer names/identities were not captured. Restored historical
consumers are a hypothesis, not a confirmed cause. Donor meta logs contain prior
wf-audit names, which alone does not prove an active restored assignment. Do not
attribute the count to this reader, NATS, or stale donor state without named APIs.

CPU profile spans20s with18.28CPU seconds; overlapping cumulative parse4.65s,
JSON decode4.51s, select0.61s. Full allocation3,331,208,608bytes/9GC and10.957ms
GC pause recorded. Earlier unbuffered run was incomplete; no isolated speed ratio
or server/GC attribution follows. Same cardinality/deadline/config, shared VM,
500ms observation; memory stats include profile flush while verdict time does not.

Independent review verifies686 selected source inputs, actual SDK/five server
executables/modules/mounts/closedPIDs,1058 unchanged donor files and full1713-file/
827alias base-plus-delta reconstruction. Delta50,162,643bytes SHA256
`764ab6b0f887da24999eaacca49192ab33ccdd5d68aacabe65a45b6ac47f2872`.
Pinned canonical donor Git parts plus delta parts are required; not standalone.
All original and failed fixtures stay closed. No public/default/live/24h adoption.

Next changed instrumentation captures named consumer configurations before the
baseline and at first residual count, plus named Info for current audit cursors.
The zero-consumer assertion and original20s overall budget remain unchanged.
