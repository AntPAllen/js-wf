# Complete reader catalog lifecycle — 2026-10-08

Frozen source: `f077727`. Standard isolated component regression, not canonical runtime/release qualification.

| Verification | Result |
| --- | --- |
| Complete graph package, normal |47 groups,33.594s |
| Complete graph package, race |47 groups,114.950s |
| Shared stream guard, normal/race |1 group each,0.024s/1.039s |
| Catalog seeded transport, normal |100,000 schedules,9 modes, exact replay,88.944s |
| Catalog seeded transport, race |1,000 schedules,9 modes, exact replay,14.666s |
| Graph pins |All17 publication/12 reader/9 recovery/9 catalog pins pass unchanged in both focused runs |

`RootCatalogPort.RootKeys` supplies permanent isolated destination discovery, including empty roots and witnessed absences. Native metadata census must be complete, count/namespace/hash valid, and every root envelope strictly bounded/canonical and bound to its hashed subject. Tentative GET images discover identity only. `SweepWithReaders` then witnesses each root and fences expired pins before object-grant collection. A missing/uncertain/malformed catalog fails closed. Concurrent roots created after a census remain discoverable for the next sweep. Original object-grant Sweep is preserved for historical replay; canonical graph adoption must use the complete reader lifecycle.

Model controls expire empty snapshots without grants, reclaim pins after all old grants close, preserve a winning renewal and reject unsupported/unknown/duplicate/invalid/root/expiry/cancellation outcomes before deletion. Nine shared modes verify exact replay, retained metadata/payload identity, empty pin high-water/schema fences, next-census late root cleanup and complete terminal object/pin drain. Observed absent roots are recorded separately in the model catalog, leaving previous trace bytes unchanged.

Native R1/R3 controls expire12 roots with8 empty pins each, preserve a retired live reader/payload and later reclaim every object/chunk while retaining14 destination identities (including an absence witness). RootKeys does not rewrite metadata. Eleven injected native census/GET controls reject partial counts, wrong namespaces/subjects, uncertain GET, sequence0, invalid JSON, foreign identity, foreign schema and oversize bodies before any logical expiry. Existing trusted publisher/collector principal controls now explicitly admit the catalog API under their unchanged permissions;37 denial checks per fixture remain intact.

`results.json`, complete JSONL/stderr, source inventories and executed scripts record all four original runs.1,363 selected tracked inputs matched Git before execution and remained unchanged afterward (`review.json`). The unrelated `.claude-artifacts` directory was retained/excluded. These standard regressions do not independently bind SDK executable/dependency/peer-media bytes, validate catalog paging/large-population/concurrent native admission, deployed old-version rejection or broad native release acceptance. No official body was retried.

Current inventory is130 seeded workloads/480 pins. Complete current-source Tier1 qualification remains pending. This isolated catalog does not discover canonical invocation/history storage or migrate runtime readers/writers/import/retention. Compaction, abrupt process/power loss, complete native fault/concurrency/scale matrices,24h, million physical drain and production online GC remain required.

The original full126 normal100k campaign is independently accepted at074bcfcc with126 complete workloads/433 pins/12.6M bodies under its original count1/300m/profile. [Frozen terminal evidence](../tier1-full126-2026-10-08/normal100k-terminal/). It cannot qualify these newer component changes. Its complete64MiB original fixture remains local pending S3 preservation/retirement.
