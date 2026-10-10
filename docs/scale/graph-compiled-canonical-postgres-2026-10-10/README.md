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

## Persistent CI coverage

The `canonical-postgres-cli` job in
[operator-runtime-domain-controls](../../../.github/workflows/operator-runtime-domain-controls.yml)
runs these exact focused race selectors against PostgreSQL 18 on relevant
pushes and pull requests. It requires both native cases and all three selected
groups without skips, and retains complete captures and the real Go log even
on failure. Hosted execution is independent of local acceptance; adding the
job does not establish a hosted pass or the independent wire review.

## Closed failed campaign

The original invocation stopped with actual Go and retained service exits 1.
[Failure review](failure-review.json) binds 1,924 unchanged Git inputs and all
778 retained files (628,742,152 bytes). R1 passes 111.85s. R3/domain fails its
Signal command after 11.274s with an unknown publication outcome; the parent
still has 99.6s left. All nine visibility cases and namespace isolation pass
in the separate 1.825s visibility package. This campaign remains rejected.

[Request correlation](failed-signal-requests.json) and
[diagnosis](failure-diagnosis.json) show a positive Signal source ack and replies
to all 410 captured requests, with a maximum observed packet gap near 135ms.
These are proxy observations. The three-second aggregate BindNextSignal
context enclosing a large owned payload read is the next model candidate;
exact internal cancellation and server-side cause are not established.
No request, fixture or workflow bounds were increased and no campaign rerun
was launched. A misleading deferred purged-row assertion on early failure
has been fixed separately: absence is asserted only after successful purge.
The existing successful-path rebuild/absence requirement stays intact.
