# Complete Tier2 consumer-leader row qualified at c4fed06

All 17 successful shards cover every seed 1–200 exactly once at executed
`c4fed061bc614488d4f89b53b216b756490f7da0` in parent run 37149506857. Each seed runs the requested
600-second mixed workload. Independent aggregation reads all archive parts
(including sparse working-copy omissions restored from committed Git), verifies
part and complete archive SHA256, and checks every archive member size/hash.
Every actual retained model executable matches its recorded digest; all 45
model dependency source files match executed Git and agree across shards.
Every seed returns exact `Ok` for all three Start/Signal/Await history models.
Raw fault and latency checks were independently performed in each shard review;
aggregation checks their exact seed coverage, totals and unchanged R3 gates.

| Measure | Result |
| --- | ---: |
| Invocations | 519,960 |
| Journal entries | 5,730,955 |
| Leader kills | 3,800 |
| Independently checked history operations | 669,084 |
| Worst terminal type p99 | 18.076655685 s (<30 s) |
| Worst progress type p99 | 9.110842432 s (<10 s) |

`row-qualification.json` records each exact shard proof identity and digest.
`review.py` and `review.log` retain the executed aggregation and outputs. Raw
histories, actual models, source bindings and downloaded metadata remain in the
referenced complete shard archives; this small aggregate does not duplicate them.

This qualifies the complete consumer-leader row at c4fed06. Final integrity/drain
assertions retain their executed named-test scope. Native workload executables
and physical stores were not uploaded; no independent store reopening is claimed.
The final-source full 13-row Tier2 matrix, 16-row Tier3 matrix, original million-
timer physical drain and actual 24-hour full-matrix soak remain open.
