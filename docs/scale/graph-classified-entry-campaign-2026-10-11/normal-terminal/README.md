# Classified actual 100000-entry normal result — 2026-10-11

Frozen source 9189259faca292659001bbbcbf936c73a1e00318 completed the actual 100000-entry
native file-storage fixture: 49,992 SDK state writes split across two checkpoint
prefixes, final entries 100000 and terminal slot 99999. No rejected user effect
runs; both stages execute exactly once. Owner-indexed checkpoint/archive recovery
uses the explicit 6h compaction-intent diagnostic lifetime. Production defaults
and per-request bounds remain unchanged; this is not a latency/performance claim.

Original unit js-wf-classified-entry100000-normal-20261011.service, InvocationID
4574548accd94da986ff05b3652910d2, supervisor 208830/child 208893 have actual exits 0.
MainPID=0, RemainAfterExit=yes, exact loaded supervisor command/source/kind/root/
unit/lifetime, ExecMainPID and 330m/10GiB service limits were independently checked.
The child wall time was 6133.959047916011s (test body 6133.72s).

The reviewer verifies 1946 immutable frozen inputs, the prepared retained binary,
complete log/limit assertions and all 3170 closed native files/3,365,343,276 bytes
against the preserved file hashes. Both padding profiles are fully accounted:
24+33=57 exact definite CAS conflicts; no uncertain, wrapped or unclassified
errors. The zero-profile-error gate is false; the approved definite-CAS-only
classification gate passes. Earlier unclassified failures remain preserved and
this result does not determine their cause.

The copied terminal state/source manifests/storage census/raw log and accepted
review are immutable evidence snapshots here. Native media remain at
/home/exedev/js-wf-classified-entry100000-normal-20261011/native.

The same-source race counterpart now runs in
js-wf-classified-entry100000-race-20261011.service, InvocationID
c0bd10f5eece4d7f9bbaa7e40139d2bc, supervisor 225845/child 225917. Its launch is bound
in ../race-launch*.json and requires this independently accepted normal result.
No race result is claimed. This frozen source predates the newer raw history/
envelope/limit/selection auditor; current-source independent full-scale auditing
and original broader fault/soak/rollout/admission/collection gates remain open.
