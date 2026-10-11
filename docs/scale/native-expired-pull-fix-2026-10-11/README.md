# Fresh native race diagnostic after expired pull fix — 2026-10-11

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
