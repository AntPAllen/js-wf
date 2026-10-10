# Compiled canonical graph CLI fixture — 2026-10-10

## Fixture implementation; acceptance pending

The canonical graph operator fixture now supports an actual compiled CLI for
all24 command processes per R1/R3 case, including large Start/Signal/result,
owned journal/replay export, online/offline replay, isolated list/lag, canonical
history/scans, dry-run/apply repair, cancellation and purge. The continuous
projector also runs as the actual CLI binary, giving50 physical CLI processes
for the two cases. Workflow effects and source/history assertions are retained.

The shared standalone invoker preserves the existing legacy fixtures. Captures
up to16 MiB keep the existing memory recorder; larger canonical captures use
the proxy's existing file-backed trace API with an explicit256-MiB per-process
encoded budget. Truncation/file failures invalidate completeness. No request,
fixture or production bounds are extended. Compiled error assertions require
exit1 and the exact expected stderr; projector receipts persist actual exit and
reaping after shutdown. Source control/helper bytes are frozen alongside this
README. These are test-only changes; public admission/collection remain off.

Original in-process canonical R1/R3 regression passes race96.492 seconds
([log](in-process-race.log)) before the external-only error/capture adaptations.
This is preliminary regression evidence, not final-source compiled acceptance.
An initial invocation omitted creating its retained root and failed before any
fixture work; that [development log](development-missing-root.log) is preserved.
The final fixture compiles and the basic command-selection unit control passes.

A framing-aware streaming wire validator checks exact API domain, INFO server
identity, complete framing/capture statistics, no legacy WF_JRN requests and
zero connections for offline replay. Six parser controls pass, including wrong
domain, malformed/incomplete payloads, oversized packets, initial INFO and
payload text that cannot masquerade as a protocol command across seeded splits.
[Parser control log](parser-controls.log).

Next: freeze a clean checkout and run the complete CLI race package with
standalone opt-ins. Review all actual child/projector receipts and complete
wire before accepting compiled canonical coverage. The previous legacy-only
standalone acceptance remains separate. Actual100,000 entries, current full
simulation and original storage/fault/soak/retention/rollout gates remain open.

## Closed compiled qualification

Frozen runtime/test source **5e2d5ac** passes the **complete CLI race package
with standalone opt-ins in412.333 seconds**, actual child/service exits0.
Compiled canonical cases pass R1 **70.79s** and R3 **78.67s**, retaining the
original two-minute fixture deadline. Independent review accepts131 physical
CLI processes, including48 compiled canonical commands and2 actual compiled
projectors, plus the existing68 legacy commands/8 handshake controls/5 leaf
daemons. It checks1,924 exact Git/source inputs, clean race CLI binary, real
reaping/exit receipts, complete domain/INFO/framing/byte-count captures and
4,002 artifacts totaling1,260,490,076 bytes. [Closed receipt](state.json),
[full log](race.log), [result](review.json),
[canonical wire](canonical-wire-review.json).

Validation source **790457c** is separate from the frozen runtime. The first
review rejected intentional compatibility cleanup during canonical purge.
`retention/graph_purge.go` fences canonical retirement before deleting the
matching legacy journal subject. Review now permits exactly one INFO and one
PURGE for WF_JRN **only in the explicit purge command**, with payload exactly
`{"filter":"wf.jrn.graph-operator.success"}`. Every other legacy journal API
and every mismatched filter is rejected; graph projectors make no legacy
journal requests. The first [rejection](development-wire-rejection.json) and
[frozen reviewer](development-review.py.txt.gz) remain preserved. Empty offline
captures omit frame_records because zero is serialized with omitempty; review
allows that only where the actual frame stream and connections are empty.
Eleven final parser controls pass, including wrong cleanup target, duplicate
fields, history reads disguised as cleanup and omitted offline counter.
[Controls](parser-final-controls.log). No cluster rerun was needed for these
verifier corrections; production/runtime code is unchanged.

For compiled commands, logged api_requests=0 refers to the unused parent SDK
trace; actual child API counts come from the independent wire report. Offline
replay has exactly zero connections. No historical signal-timeout cause is
claimed. PostgreSQL/retained-envelope opt-ins, natural faults and the broader
simulation/storage/soak/retention/release gates remain open. Artifacts are kept
at `/home/exedev/js-wf-compiled-canonical-cli-artifacts-20261010` (~1.2 GiB),
with a separate small legacy wire-review copy. The retained service is
`js-wf-compiled-canonical-cli-20261010.service`, invocation
3100ee7e04684877946201cd11d366cb, MainPID0/ExecMainStatus0.
