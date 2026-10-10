# Stream compaction comparison and authenticate node grant coordinates

Commit now streams source records alongside the relocated archive/live ranges.
It compares every record body, payload count and payload hash, then freshly
checks each original and relocated payload grant. A range iterator loads each
intersecting tree once, retains O(log population) pending trees and no record
bytes, and makes failures sticky. ReadRange shares that iterator; its existing
traversal order and cancellation behavior remain covered.

WalkNodes supplies freshly authenticated node coordinates. Compaction matches
each relocated node grant to that exact first index, height and stream, with a
fresh fence/intent/receipt/destination/expected-head/expiry read per node. This
removes repeated root-to-node membership lookups. Existing payload origin
membership checks and inherited archive verification remain. No cross-call
node or grant cache was introduced.

## Measured work

The same production journal/model cost fixture compares against the corrected
node-grant implementation at `5b1e061` (the earlier unsafe baseline is excluded).

| Entries | Previous compaction Gets | Streaming compaction Gets |
| --- | ---: | ---: |
| 36 | 1,093 | 569 |
| 260 | 12,104 | 4,924 |
| 1,028 | 58,442 | 21,566 |

These count model Get operations, not native RPCs or latency. First checkpoint
confirmation work remains111/783/3087 Gets and37/261/1029 entry reads; indexed
confirmation remains4 reads/17 Gets. The timed reader still completes57 virtual
seconds with28 renewals. Read-budget rejection still preserves pointer/cursor,
releases its pin and permits a fresh retry. Cost controls pass under race39.098s.

## Directed controls

Iterator/node traversal controls pass under race1.612s: ordered bounds, empty
ranges,259 node reads for131 records, external payloads not loaded, bounded
pending trees, cancellation, callback failure, corrupt roots/later leaves and
sticky failure after the corrupt leaf is repaired.

Existing compaction controls and28 node-grant cells pass under race27.579s.
The cells cover live/archive leaves/branches and missing intent, destination,
first index, height, stream, kind or closed fence. A node-validation bypass
fails all28. Four new controls rebuild valid target forests with freshly granted
replacement data/payloads; equality rejects them under race1.196s. Bypassing only
record data/hash comparison makes all four fail by accepting changed relocation.
Exact mutants and outputs are retained.

The old pins fail exactly14 compaction cases as expected. Only those14 Get traces
were refreshed. Executed helper, compressed prior traces and independent review
prove every non-Get event (including grant reads), seed, decision and other trace
field unchanged. All853 saved simulations then pass15.620s.

Native R1/archive64 and the seven directed handoff recovery cells pass together
under race58.580s: two checkpoints, terminal slot63, zero forbidden effects;
controlled collection reclaims19 original receipts before stage entry. Native
archive phases remain7.491s/11.673s and whole transitions8.913s/14.087s. The Get
reduction alone does not establish improved native latency or headroom.

Run `python3 docs/scale/graph-compaction-streaming-2026-10-10/review.py` to verify
the retained source hashes and evidence; see [development review](development-review.json).

## Remaining original requirements

These are directed development checks. The actual100000 gate remains failed
at its first transition with an unconfirmed exact subphase. First checkpoint
verification and fresh grant/archive work still scale with the prefix. Bulk
publication needs bounded/resumable work, reader and intent lifetime handling,
crash/unknown outcome controls, exact generation/tail binding, independent
receipts/reclamation and the reserved terminal slot. The unchanged15-second
deadline is still a constraint; no multi-hour cap rerun was started.

Both frozen campaigns continue on their original older sources and exclude
this optimization and the recent grant/recovery fixes. Their successful
execution cannot qualify this source. Full current-source simulation/extended,
native fault/scale/soak, retention/import, collector/admission and rollout gates
remain open. Public continuation admission stays closed; production collection
stays off.
