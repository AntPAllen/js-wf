# Original Phase3 200/50/6 repeated-worker-fault cohort qualified

At exact **`24cf85739260ee0f734e73329d57511030ab5697`**, all **200 invocations** complete **50 journaled increments** with result50 in **141.438603 s**, under the original five-minute target. Six logical worker slots execute as actual subprocesses; replacements create64 total OS process instances. The complete named test passes **211.93 s** (wall212.165 s), including finishing45-second holds, graceful exits and final audits.

## Fault timing and recovery

The independently reviewed timeline contains **70 faults** at consecutive two-second scheduled intervals:

| Kind | Total | Active worker targeted |
| --- | ---: | ---: |
| Confirmed SIGKILL and replacement | 58 | 47 |
| Confirmed SIGSTOP/SIGCONT | 6 | 6 |
| Worker connection partition | 6 | 6 |

Actual applied interval median/maximum is **1.999956/2.001208 s**. All twelve long holds last **45.017984–45.028757 s**. Every stopped worker has a confirmed current lease; every network cut verifies an active proxy connection before cutting and zero after closure. Holds overlap while the controller continues its two-second choices, preserving two executable members. Eleven kills target idle workers; these are retained and not counted as active cuts.

Kill status is verified by the compiled helper's actual `WaitStatus.SIGKILL` check before replacement readiness. The report's `confirmed` timestamp for kills reuses its kill-request observation; it is **not** independently observed exact status-confirmation time. Pauses confirm actual process stop/running state. Proxy-heal times denote restored forwarding, with terminal recovery proven separately by the result/journal gates.

## Independent retained-state and SDK review

All **200 retained final journals**, **20,400 entries**, independently verify exactly50 requested/completed pairs, absolute indices, input hashes, nondecreasing epochs, one worker per epoch, one Started/Completed and result50. There are **59 epoch changes** and no interleaving. The actual named cluster API invariant check independently run by the fixture reports200 invocations/journals/terminals; no physical store is reopened here.

A separately built helper copies the exact tested handler and executes production `wf.Replay` against every retained journal: **all200 replays pass**. All29 actual local Go/module dependencies match executed source before compilation and after review. The actual model executable and its helper/source ledgers are retained. This independently checks SDK replay and final outputs; it does not fabricate client histories or a raw invocation-stream dump.

## Complete preservation and scope

The proof retains the actual workload executable, actual replay executable, all selected captured Go/Cgo/test/module sources, exact pre/post ledgers, build/commands/context, both independent reviewers, complete worker logs/dispatch/fencing/metrics, fault report, every final journal and all **387 original store files**, **19,088,440 bytes**. All3,849 selected source inputs remain identical before/after; all562 selected local inputs match executed Git. This inventory is a superset including dependency tests not linked into the executable. Assembly, embedded and compiler-generated inputs are not claimed as exhaustive hermetic capture.

All **4,847 proof members** are reopened and SHA256-verified before publication. Both archive parts concatenate to the complete **47,924,840-byte** digest in `manifest.json`. Reassemble sorted `proof.tar.gz.part-*` outside the repository, verify the digest, then extract. Originals are hashed and preserved, not rewritten or independently reopened.

This qualifies **the supplied plan's original Phase3 200-invocation/50-step/six-worker cohort with repeated two-second faults**, seed1, at its executed source. It does not qualify all of Phase3, the full20/200-seed mixed matrices, client linearizability/liveness coverage, the million-timer drain or actual original24-hour full-matrix soak. The historical TTL30s worker-kill mismatch is not rerun; this distinct original cohort uses production TTL12s/AckWait13s/heartbeat3s.
