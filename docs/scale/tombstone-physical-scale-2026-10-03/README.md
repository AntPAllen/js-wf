# Full-size physical tombstone drain — 2026-10-03

The existing 100,000-tombstone integration test previously stopped when KV keys
became absent. That only proved logical deletion: DEL revisions could remain in
the underlying stream. It now keeps the production elected cursor loop running
until a paginated stream-info census of `$KV.WF_STATE.tombscale.>` contains zero
physical records, then verifies its persisted cursor still exists.

Actual default full-size run: 100,000 expired tombstones across a three-node
file-backed cluster. All were written in 2.455 s. The named test passes in
153.420 s / package 153.433 s, reports zero retained marker messages, and confirms
logical absence through the other pinned client. The production scanner uses
budget 256 and cadence 10 ms. This proves bounded full-population cleanup for
this fixture, not a population-independent recovery latency guarantee.

The actual live executable was copied from `/proc/<test-pid>/exe` before Go
removed its build directory, and its SHA256/build info retained. The source
inventory was captured while the already-compiled test was live and verified
unchanged through completion. It is not a pre-build inventory. The baseline Git
revision, exact fixture change, fixture bytes, raw log and verdict are archived.
Physical test stores were automatically cleaned by the test and are not retained;
there is no independent physical-store reopen claim.

Every archive member was reopened and SHA256 checked before publication.
Reproduce the full-size run with:

```sh
WF_TOMBSTONE_SWEEP_SCALE=1 go test -p=1 ./integration -run '^TestHundredThousandTombstonesSweptByLeaderLoop$' -count=1 -v
```

The retained corpus already has deterministic deletion-marker regressions;
this test exercises the corrected marker cleanup through the actual production
loop at its stated 100,000-key scale. Full fault matrices and 24h remain open.
