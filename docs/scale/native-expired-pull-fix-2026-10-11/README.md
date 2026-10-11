# Fresh native race diagnostic after expired pull fix — 2026-10-11

## Terminal closure

Actual SDK exit1 after86.877748085seconds. Ten old invocations completed in
31.435seconds; the distinct active-population gate passed at41.697seconds and
purge10/reuse1 overlap passed at44.558seconds. The first reported error was
release Signal for live-00000/invocation16 with a healthy parent context:
`signal publish outcome unknown: bind ordered signal through 6: prepare signal binding: journal append outcome unknown: context deadline exceeded`.

The failing BindNextSignal call returned from PrepareStreamAppendWithApplication,
before calling Commit. This does not identify an acquire/upload subphase,
exclude another concurrent actor, or establish a server-side cause. Signal release,
completion and final raw audit were not accepted. The retained native store is
closed:2159files/16,881,838bytes. `review.py` independently compares1169 frozen
inputs with Git, confirms the actual race executable and terminal receipt, and
matches every closed native file. The original failed-store cause remains
unconfirmed and the full10k/10k/1k gate remains open.


Source 5285012db6da56f7b60f4220eb4b7e2a00f3b4af is frozen in a separate
Go/module snapshot (1169 files), each compared byte-for-byte with Git before
launch. The driver checks that snapshot again through terminal closure. Neither
previous failed native stores nor their frozen source snapshots are reused.

One fresh R3 canonical active10/10/1 race diagnostic uses the original 20-minute
parent deadline and 22-minute test deadline. It includes the distinct-cohort
fixture gate, commit phase labels and expired-pull context fix. It is not the
full 10k/10k/1k gate. No PostgreSQL proof is requested.

The driver builds a retained integration executable and records its actual
/proc executable hash at launch, actual test exit and closed native file census.
`live.json`, logs and receipts are still mutable until the process is terminal;
preparation alone is not a passing result. Poll unified exec session 32889 or
the concrete PID recorded in live.json; an observation timeout is not failure.
Do not reopen the original retained native store while its process is live.
