# Original-head compaction intent renewal

`BeginCompactionIntentRenewal` captures a copied prepared publication and a
complete isolated-namespace scope enumeration. `Advance(ctx, maxScopes)` checks
at most that many scopes, extending only grants owned by its frozen token with
the exact destination, original head and old or exactly matching new expiry.
It preserves generation, phase, object and locations. Foreign scopes are unchanged.
Original source authority and the caller's current clock are checked before
mutations and between batches. Renewal must finish before the old expiry.

The operation publishes no root. It returns no partial plan or trusted content
certificate. Only full completion returns a copied plan with the new expiry;
normal compaction commit independently checks every record and target grant.
In-batch errors latch failure. A fresh operation can reconcile exactly matching
prior renewal updates while the old deadline and original source remain valid.
Closed/collected grants cannot be revived. No root, fence or descriptor schema
changes are introduced.

Renewal covers **every publication-owned scope**, including discarded staging
branches, not just final forest reachability. An expired discarded grant can
advance the source head even when final target grants are renewed. The model
fixture explicitly creates such an unpublished branch. A first intermediate-
scope bypass passed with an ordinary completed-stage fixture; that weak fixture
was strengthened and the original bypass output is retained.

## Scope and evidence

Seventeen controls cover healthy/inherited graphs, lost/dropped renewal replies,
cancellation, expiry before/within renewal, source changes before/after CAS,
reader acquisition, collector races, revoked grants, wrong expiry and copied
input/result ownership. A separate expired-publication baseline rejects commit.
Healthy renewal examines55 scopes, renews21 owned scopes including the discarded
branch, preserves the root after old expiry and commits independently. Inherited
archive renewal examines74 and renews18. The per-call scope budget is3.

Deliberate expiry, source-head and intermediate-scope bypasses have exact retained
sources, commands and results. The final full graph-compaction selection includes
the new controls and existing native R1/R3 store-restart controls under race.
Those native fixtures exercise the existing compaction path; they do not qualify
native renewal. All853 saved simulation traces remain unchanged.
Run `python3 docs/scale/graph-compaction-intent-renewal-2026-10-10/review.py`
to inspect the scoped development evidence.

## Remaining work

This primitive requires a frozen token: no concurrent staging or other renewal
under it. Namespace enumeration remains context-bound but stores the entire key
list; this is not a bounded-memory census. Per-scope checks include additional
source/metadata RPCs. Production integration must handle staging pause/resume,
durable operation binding and renewal outcomes, worker lease ownership and a
bounded namespace scan before adoption at scale. Runtime maintenance still has
its15-second outer deadline and fixed intent expiry. Actual100000 and every
broader original qualification gate remain open. Public continuation admission
stays closed and production collection stays off.
