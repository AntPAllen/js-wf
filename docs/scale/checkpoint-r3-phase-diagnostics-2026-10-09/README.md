# Checkpoint request amplification and ordered traversal

The initial R3-domain phase diagnostic (`race.log`) failed with actual exit 1
at the unchanged two-minute fixture deadline, before bounded recovery began.
Prefix construction consumed 72.100 seconds; checkpoint materialization,
discovery and pointer publication consumed a further 36.058 seconds and 10,570
JetStream SDK requests. The final prefix/read controls exhausted the budget.

## Implementation

`retainedgraph.ReadRange` traverses intersecting subtrees in record order with
O(log population) traversal space. It fetches each intersecting node once, uses
the existing digest/canonical/position/receipt validation, and keeps no cache
between calls. It excludes subtrees outside the range, stops on callback errors
or cancellation, and does not interpret partial callbacks as a complete read.

`Protocol.ReadRetainedRange` witnesses the exact live pin before traversal and
again before each leaf is delivered, using the collection clock. Released,
expired, altered or uncertain authority cannot authorize the next callback.
`GraphView.ReadRange` uses the existing entry validator and maps logical indexes
across the archive/live boundary. Checkpoint discovery uses this traversal;
the native fixture also uses it when reading the complete prefix to install
its negative read controls. All 131 prefix records, negative controls, recovery
assertions and the original two-minute deadline remain intact.

## Executed checks

- Focused race controls pass: retainedgraph 1.414 s, graphpublication 1.200 s,
  journal 86.717 s, actual exit 0. Coverage includes range boundaries, corruption,
  cancellation, callback failure, mid-range pin release/expiry/unknown authority/
  altered snapshot, checkpoint ownership/index controls and all 16 seeded
  archive/collection scenarios.
- Final canceled-context guards and their empty-range archive check pass race
  in 2.397 s, actual exit 0 (one representative archive scenario).
- Exact 131-record census passes race in 1.575 s: ordered traversal performs
  **259 node GETs**, equivalent individual point reads perform **1,029**.
- An earlier control attempt failed because an empty range inside a tree still
  fetched branches. Its terminal package results were retained in the tool
  transcript (retainedgraph FAIL, graphpublication PASS, journal PASS 86.695 s,
  overall exit 1); the empty-range early return fixes that concrete defect.

The subsequent native R3 run also fails, actual exit 1, in 120.915 s:

| Phase | Original diagnostic | Ordered traversal run |
| --- | ---: | ---: |
| Prefix construction completed | 72.100 s | 100.787 s |
| Checkpoint materialization/discovery/publication | 36.058 s | 18.622 s |
| SDK requests in that phase | 10,570 | 4,384 |
| Remaining fixture budget after that phase | 11.841 s | 0.590 s |

This demonstrates reduced request amplification, but **does not qualify native
R3 recovery**. The run again fails in prefix/read controls before bounded
recovery. Timings were observed with other qualification/control work active;
no exclusive CPU, causal CPU diagnosis or NATS defect claim is made. No deadline
was relaxed and no further identical native retry was launched.

`range-native-command.json` records launch-time source hashes and actual exit.
The checkout was mutable: canceled-context admission/final guards were added
to `journal/graph_range.go` during native compilation. The follow-up source
observation records that limitation; this native attempt is not complete
immutable build-source qualification. The final guards are separately tested
in the source-hashed final context control.

Current full-source/all-pin qualification, public continuation admission,
production collection and all original broader plan gates remain open.
The remaining dominant cost is native append/readback and reader refresh during
prefix construction; another identical run would not establish that cause.
