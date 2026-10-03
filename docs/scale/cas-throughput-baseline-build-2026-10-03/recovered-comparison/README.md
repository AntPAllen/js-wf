# Recovered paired CAS throughput gate

Hosted37122546973 at1b03266b23da7729e48800910279e3cc97221a06 passes both
placements against fixed baseline4fa311954f42d1325a46dad12bb5d128dad2b747.
Independent review validates all12 raw reports:three alternating baseline/
candidate rounds for leader and follower, same runtime versions within each
placement, actual stable client/leader topology,10000 hot appends and100000
appends across1000 parallel invocations, and110000 retained messages/1001 subjects.

Recomputed medians match uploaded results exactly. Candidate/baseline ratios:

| Placement | Hot | Parallel |
| --- | ---: | ---: |
| Leader | 0.954251 | 1.006982 |
| Follower | 0.999921 | 1.006874 |

Every ratio exceeds the unchanged0.8 minimum. Uploaded measurement harness
bytes match exact candidate Git; both recorded harness hashes validate.
Original reports, logs, summaries, shared source and terminal metadata are
archived with every member SHA256 checked before atomic rename.

The uploaded summaries record binary hashes, but the binaries were not retained
in these artifacts; those hashes cannot be independently checked against bytes.
This qualifies the existing paired20% throughput regression gate at the recorded
source, with that provenance boundary. Full release remains open.
