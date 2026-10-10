# Canonical CLI timing and terminal projection repair — 2026-10-10

## Result and scope

Full `go test -race ./cmd/wf -timeout=10m -count=1 -v` passes **169.892 seconds**
(package), actual child exit **0**, supervisor wall174.573 seconds. R1 canonical
operator commands pass41.32 seconds; R3 domain53.61 seconds. Independent
[review](review.py) checks1,932 before/after source inputs against the current
files, frozen changed fixtures, the exact command/exit/log hash, both canonical
cases and skipped tests. Source identity is base516bc9f plus the input manifest
and three frozen modified Go files; it is not an assertion of pristine516bc9f.
[Receipt](restored-full-state.json), [log](restored-full-race.log),
[review result](review.json).

The full visibility race package passes **7.703 seconds** including the terminal
lease model cases and unavailable owned-history control.
[Log](visibility-restored-race.log). Unfinished lease-held workflows retain the
existing zero-reader behavior. Held-lease uncertainty still stops projection.

## Failure, reproduction and change

The earlier full CLI signal failure remains preserved under
`../graph-step-request-envelope-2026-10-10/development/full-cli-race-failure.log`.
Its R3 signal unknown outcome occurred at18.76 seconds, before the two-minute
fixture watchdog; the server-side cause remains unconfirmed. A preliminary
R3-only timing diagnostic passed62.277 seconds with unchanged bounds
([log](race.log)); this is preliminary evidence with an earlier log formatter,
not the acceptance of the final source.

The first instrumented complete package fails158.050 seconds, actual exit1,
with R3 `graph view mixed sources {[] } <nil>` after successful Start, Signal,
Result and Replay. [Failure log](full-race.log), [receipt](full-state.json).
Before/after input inventories match; review verifies historical Git source plus
its frozen CLI fixture. Native stores and projector traces remain under
`/home/exedev/js-wf-cli-full-diagnostic-20261010` (~247 MiB).

Two seeded in-memory cases reproduce an empty terminal page without a server:
a fresh projector sees an existing delivery lease after a completed or failed
terminal record and writes a queued row. Both fail before the repair
([development log](terminal-held-before.log)). This confirms a deterministic
projection defect consistent with the native symptom; it does not prove every
native empty-page cause or explain the earlier signal timeout.

Projection now permits owned-history readers for a quorum-confirmed terminal
lifecycle even while a delivery lease remains held. The lifecycle observation
only selects that read path: full owned records, source token, terminal outcome
and attributes must still validate. An injected object-read loss preserves the
prior row and unrelated rows; it cannot publish a terminal row from summary
metadata. Existing nonterminal lease-held and lease-unknown controls remain.
One development assertion initially expected the journal unknown sentinel;
the simulator exposes its own `ErrTransportLost`. That test-only expectation
was corrected; its [failed log](visibility-terminal-unknown-race.log) is retained.

CLI fixtures log operation names, elapsed time, API request counts and remaining
fixture lifetime. Argument values are not logged. Two-minute fixture,45-second
CLI context, canonical three-second attempt bounds, payload and production
configuration remain unchanged. [Restored runner](run_restored.py) uses a new
output/root and records actual exit atomically; no earlier output is overwritten.
Restored stores remain under `/home/exedev/js-wf-cli-full-restored-20261010`
(~257 MiB), preliminary R3 stores under
`/home/exedev/js-wf-cli-r3-diagnostic-20261010` (~159 MiB).

## Remaining gates

This qualifies the current default CLI race package only. PostgreSQL,
standalone executable/leaf opt-ins and retained-envelope opt-ins are skipped and
unqualified; exact names are in review.json. A subsequent passing signal call
does not resolve the earlier intermittent timeout. Full current simulation,
original actual100,000-entry boundary, storage/fault/security/soak/retention and
rollout gates remain open. Admission and collection remain off. The separate
100,000-native-grant diagnostic retains its original live invocation; this
visibility/CLI change does not alter that frozen native package.
