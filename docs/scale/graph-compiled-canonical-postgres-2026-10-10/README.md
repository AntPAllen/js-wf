# Compiled canonical PostgreSQL CLI qualification

The previous complete CLI acceptance skipped PostgreSQL. Frozen `98eacf4`
adds an opt-in compiled canonical PostgreSQL fixture using the same production
commands and actual projector processes as the KV fixture. The focused race
campaign is live; no acceptance is inferred from its launch.

The command runs both R1 and R3/domain cases, all nine PostgreSQL visibility
uncertainty/terminal cases, and namespace isolation. Each canonical case keeps
the existing two-minute fixture deadline and command/request bounds. Large
Start/Signal/result, owned replay exports and offline execution, canonical
list/lag/history/scans, terminal repair, cancellation, purge and one effect
remain asserted. PostgreSQL backs list, lag and the concurrent real projector;
the canonical journal remains the execution authority.

[Source](source.json) binds a clean isolated checkout and 1,924 inputs.
[Launch](launch.json) records the live service and actual Go child.
[Database](database-launch.json) records a new loopback-only PostgreSQL 18
container and image identity, separate from other databases. The test service
uses a one-core quota, GOMAXPROCS=2 and GOMEMLIMIT=1GiB; the database has a
half-core quota and 512MiB memory bound. Existing entry/Tier 1 campaigns
continue their original invocations.

After the actual child and matching retained service stop, run:

```sh
python3 docs/scale/graph-compiled-canonical-postgres-2026-10-10/review.py
```

[Independent review](review.py) requires both package passes, no skips or race
reports, all three selected groups, immutable Git inputs, a clean-source race
CLI binary, 48 actual commands and two reaped compiled projectors. It validates
every complete raw NATS wire capture, domains, one offline replay per case,
exact PostgreSQL view options and fenced compatibility cleanup. All retained
artifact files are hashed. The raw root is
`/home/exedev/js-wf-compiled-canonical-postgres-artifacts-20261010`.

This focused campaign does not establish complete CLI-package acceptance at
the new test source, 50,000-row crash/rebuild acceptance, sustained database
fault recovery or the broader implementation gates. Admission and collection
remain off. Preserve failed evidence and inspect actual terminal status;
observation timeouts do not authorize a restart.
