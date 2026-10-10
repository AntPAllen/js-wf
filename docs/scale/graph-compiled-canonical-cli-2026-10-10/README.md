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
