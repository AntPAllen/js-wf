# Complete recorded-source Tier2 all-server-kill row

All17 successful shards cover seeds1–200 exactly once at
`c4fed061bc614488d4f89b53b216b756490f7da0` /run37149506857. Every original case
runs600s; aggregation checks the recorded duration and minimum named-test time.
Every published part, whole archive and each member size/hash verifies, including
sparse checkout fallback to committed Git. Each actual model executable matches
its digest, all45 dependency/module sources match executed Git and agree across
shards, and every seed returns exactOk for all three history models.

| Measure | Result |
| --- | ---: |
| Invocations | 476,840 |
| Journal entries | 5,256,067 |
| Admitted all-server faults | 3,800 |
| Independently checked history operations | 613,446 |
| Worst terminal type p99 | 18.295491603s (<30s) |
| Worst progress type p99 | 13.019867582s (<30s) |

Raw fault/latency identities and reports are independently checked per shard.
Aggregation verifies exact200-seed coverage, unchanged gates, totals, complete
model verdicts, model/source bindings and archive contents. The script and actual
output are retained; aggregate manifest hashes these files. Referenced complete
shard archives retain the raw evidence without duplicating it here.

This qualifies the complete all-server-kill row at its recorded source. Runtime
executables/full workload source inventories/physical stores were not uploaded;
final integrity/drain remain executed named-test assertions, not independent
store reopening. Parent run contains failures in other rows. Full13×200 Tier2,
16×200 Tier3/final-source gates, native-million physical drain and actual24h soak
remain open. This is not blanket current-main qualification.
