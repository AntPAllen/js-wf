# Append request deadline cuts — 2026-10-11

Six deterministic in-memory controls distinguish a grant read deadline, a CAS
that did not commit, a committed CAS resolved by exact readback, a committed CAS
with unavailable readback, cancellation after commit, and replacement before
readback. Request deadlines are injected as transport responses without sleeping
or expiring the enclosing context. Cancellation is modeled separately.

Each control verifies the authoritative population, publication attempt count,
unchanged immutable objects, and recovery using the same exact private prepared
publication. A replacement is rejected rather than resurrected. This is protocol
component coverage, not worker redelivery, an added seeded family, or NATS conformance.
No production protocol change was needed.

Normal and race commands pass all six new controls and the existing unknown-reply,
collector/commit, uncertain-census and context/copy controls (22 named PASS records
per command, including parent tests). Two focused Go overlays actually exit 1:
skipping grant validation fails grant_read, and skipping exact publication witness
checks fails cas_committed_replaced. The five other deadline controls pass in each
mutation. Logs, exact overlays, command exits and 1154 unchanged Go source hashes
are verified by review.py. Executable capture was not performed for these commands.
The existing graph-publication CI job runs the full component package in both modes;
no hosted CI completion is claimed.

The native SDK fixture now logs journal append/error operations for every child
promise flow, including successful children. Its only source change from 7fffe3a
is the observer condition. A worker compile command passes; no native rerun has
been performed. The earlier native compatibility failures remain failures. Their
original source manifest/reviewer belongs to the 7fffe3a source snapshot and should
not be interpreted as a verification of this modified observer fixture.

The native archive and 100,000-entry failures still have unconfirmed causes.
Worker-level fresh-delivery recovery, retained native-store investigation, full
current qualification and original release gates remain open. No timeout, lease,
retry or acceptance target has been relaxed. Public continuation remains disabled.
