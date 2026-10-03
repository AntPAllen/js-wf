# Superseded Tier2 campaigns cancelled

Three older full-matrix campaigns occupied ten runners and had 476 queued
jobs while the current-source campaign's planner was queued. Their revisions
predate the scanner/marker corrections and cannot establish current-source
release qualification:

| Campaign | Revision | Active / queued jobs before cancellation | Artifacts before / after |
| --- | --- | --- | --- |
| 37128612905 | `9ac3ad44267b68d1a1ec63ce47e1fb11d9c00204` | 4 / 209 | 8 / 12 |
| 37057872230 | `076ebad5cb65daba7eae1b81950fc95e29fa259c` | 3 / 172 | 46 / 49 |
| 36891850893 | `92586ea3ff579362b5ab61e7026b6d1f244a2d90` | 3 / 95 | 123 / 126 |

Each cancellation was explicitly requested to retire superseded work, and all
three campaigns were subsequently observed terminal with conclusion `cancelled`.
Every previously listed artifact ID remains listed after cancellation. Ten
additional partial-job artifacts were uploaded; their contents have not been
independently reviewed and are not accepted full-duration evidence.

The current-source campaign `37149506857` was then observed with its planner
executing. A subsequent check observed the planner completed and all 221 row
jobs queued. This establishes scheduling progress, not any passing row. Current
mutations `37149508529` and simulation `37146526818` remain live and untouched.
Cancellation does not establish that older campaigns failed a runtime invariant,
and does not erase their previously accepted component evidence.

`metadata.tar.gz` retains 20 JSON observations: before/after campaign metadata,
artifact listings, cancellation command results, artifact-ID subset checks and
current-campaign observations. Every member was reopened and SHA-verified before
the temporary archive was atomically renamed; `manifest.json` records its hashes.
Artifact listings establish availability only, not independent content validation.
Previously preserved originals remain unchanged.
