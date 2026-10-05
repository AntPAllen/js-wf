# Real JetStream-domain continuation retirement/reuse

Clean9581ebc adds a real-domain R3 library fixture and executes the existing
strict continuation retirement/reuse scenario through domain-routed public
clients, workers, journals, snapshot manifests, frames, blob chunks and results.
Every server is configured with WFRETIRE; all three pinned account-API clients
report that domain and three distinct library-server IDs are retained. This is
not only an overridden Options() value or a mocked API subject. Default fixtures
keep their prior configuration; the domain is retained by fixture restart options.
No production runtime semantics or Tier1 state-machine graph changes.

Actual retained race SDK PASS23.44s. Original30s startup readiness and60s scenario
contexts stay. Generation1 retires and reuses as3; exactly two old objects are
reclaimed. Collected old frame/blob are absent, and reinjected old manifest is
rejected by both journal and worker before user code. Fresh manifest response
is deliberately lost once; fresh initial stage enters twice, with exactly three
user effects overall. Two terminals pass raw integrity; all three peers return
fresh result2 and survivor remains1. Exact shared object bytes and shared/survivor/
fresh references survive quiescent collection. Four initial calls include retry;
no repeated recorded effect. No active-writer GC claim.

Independent review checks actual live SDK hash against retained executable and
clean VCS/fullbuild/race fields,650 selected repository Go/module inputs against
Git and identical before/after ledgers, native domain IDs, original scenario
admissions and counts. Library servers are embedded in the actual race SDK,
not separately captured native server processes. These inputs are not an exhaustive
external-module/toolchain/assembly inventory. Original stopped stores remain at
/tmp/js-wf-continuation-domain-race-20261005; no independent reopen is claimed.
Complete1,076-member archive/two split parts read back/hash-verify.

This accepts positive real-domain retirement/reuse and lost-manifest repair at
the executed source. It does not exercise domain server faults or force lazy
object-absence confirmation, qualify legacy-server versions, worker SIGKILL,
online GC, full final-source matrices or24h. Existingb2d7011 journal24h remains
on its live original handle; native race/build overlap its20–22-minute region
on the shared VM. No stable latency/performance claim from this focused test.
