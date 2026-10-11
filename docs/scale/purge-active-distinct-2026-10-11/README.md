# Distinct active invocation cohort — 2026-10-11

The purge/reuse fixture's aggregate callback counter could accept two active
callbacks for one invocation while another invocation was missing. The active
population and post-purge gates now require exactly one active callback for
every expected live index at one locked observation. Entry/exit share the same
lock. Population waits report total and unique counts, with up to20 missing and
duplicate indices, every30seconds. Invalid live indices fail explicitly. Existing
count targets,20-minute bound,20ms polling interval and release behavior remain.

This is a fixture correctness change, not a fix for the unconfirmed native
active-population stall. Older active-mode fixtures prove an aggregate callback
count at their observations; their executable results and raw audits remain
valid at that scope, but they do not establish this new distinct-cohort check.

## Verification and integration

Two component controls exercise a collision that the old total predicate would
accept, an extra duplicate, the complete distinct cohort and departure/drain;
a128-goroutine held cohort checks synchronized entry/exit under race detection.
Normal and race captured executions pass both controls. These compile the
modified fixture but do not execute a new native cluster or qualify the full
active10k requirement.

Changes were prepared in the isolated checkout
`/home/exedev/js-wf-purge-active-unique-20261011` while the old native SDK retained
its original sources. Captured SDKs, actual exits and1169 input hashes are
reviewed independently. `fixture.patch` and `active-helper.go.txt` preserve the
exact proposed changes. All1169 qualified input bytes also match integrated main
`e882213`; `integration.json` records that comparison. The isolated commit is
`98d296b`. No model family/pin or production runtime code changes.

CI runs the controls in both modes before the native fixture, requires each
named PASS and uploads the control log. The new helper also triggers the
workflow. Both workflow Python blocks compile locally; no hosted result is
claimed. Executables remain under `/home/exedev/js-wf-purge-active-distinct-20261011`.
`review.py` verifies the source/patch/helper, binary hashes, actual exits and PASS
names. Initial uncaptured unit runs are diagnostic. A preparation failure from
six omitted tracked Go evidence inputs in the sparse checkout is retained;
those inputs were populated and checked before captured SDK runs.
