# Relocated compaction node grants

`retainedgraph.Walk` reports `true` for tree nodes and `false` for external
payloads. Compaction commit interpreted that flag backwards: it skipped every
relocated tree node and redundantly validated payloads already checked in the
record comparison loop. This omission existed before the traversal optimization.
Commit now checks relocated nodes in both live and archive forests before its
canonical root CAS. Original payload grants and relocated payload grants remain
checked; inherited archive frontier verification retains its existing contract.

The 16 directed controls cover live/archive × leaf/branch × missing intent,
foreign destination, wrong location and closed fence. They mutate only grant
authority while node bytes remain readable. All 16 accept invalid publication
on the retained original implementation and reject on the corrected code,
leaving the canonical root unchanged. The original source, exact failures and
an initial fixture build error are retained. Existing compaction controls and
the new controls pass together under race in 28.076 seconds.

The checkpoint cost/reader controls pass under race in 52.643 seconds. Mandatory
node validation increases compaction Gets to 1093/12104/58442 for 36/260/1028
entries. Counts from the implementation that omitted node checks cannot serve
as a safe performance target. Native R1/archive64 passes under race in 57.058
seconds: two checkpoints, terminal slot 63 and zero forbidden effects. This is
a bounded development run; it does not clear the failed actual100000 gate.

All 853 saved simulations pass in 15.177 seconds. Only 14 compaction pins changed.
The executed refresh helper and compressed previous traces are retained. The
independent Python review compares *all* trace fields after removing only Get
and ReadBlob events: seeds, decisions, mutations and other events are unchanged.
The old pins' expected mismatch is retained as a failed run, not acceptance.
Run `python3 docs/scale/graph-compaction-node-grants-2026-10-10/review.py` to
recheck retained logs and current pins; see [development review](development-review.json).

These checks are unfrozen development evidence. Both live frozen campaigns
(`0404fc0` full158 race and `2de6dfa` compaction) contain the original omission.
Their source and running commands stay unchanged. The external compaction
reviewer will retain verified execution evidence while setting `accepted=false`
for the known defective source. Neither campaign qualifies corrected grant
safety. Original extended simulation, native fault, actual-cap, retention,
import, collection, admission and rollout gates remain open; public continuation
admission stays closed and production collection stays off.
