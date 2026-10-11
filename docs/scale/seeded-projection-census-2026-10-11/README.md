# Seeded raw projection census model — 2026-10-11

The new graph_projection_census workload runs actual canonical reserve/publish/
bind/begin/append/retire operations on existing in-memory graph/start/KV ports.
Its raw snapshot copies stored authority/fence metadata, loads bounded physical
objects independently, captures all projection keys/bytes and reads the exact
invocation source. integrity.CheckGraphJournals validates those persisted bytes;
production graph readers do not validate their own output.

Live/retired × seven modes gives14 directed cases: healthy, orphan, missing,
wrong_generation, stale_reader, create_drop, create_lost_ack. KV fault injection
uses existing before-commit drop and committed lost-ACK semantics. A stale reader
returns the previous wrong-generation value after the captured census is correct.
Invalid observations must fail; healthy/committed-lost-ACK state must pass.
Fixture-only healing uses the same canonical generation and fresh raw snapshot.
The healed live journal has two entries; retired state has one projection-only
receipt. Sources/projections are deleted, virtual time advances60s, and graph
sweep must leave zero owned objects with reference checks passing.

## Qualification

```sh
python3 docs/scale/seeded-projection-census-2026-10-11/run.py normal
python3 docs/scale/seeded-projection-census-2026-10-11/run.py race
```

Final actual exit0 for both: normal4.571s/race50.544s. Each runs1000 completed
generated bodies, exact Trace replay for every body, all14 new regression pins
through the generic replay registry, and existing default/explicit durable failure
trace persistence controls. Both actual test executable SHA/build/command captures
and driver exit receipts are retained.2025 Go/source/corpus/CI inputs hashed before
both commands remained unchanged. Extracted exact workflow receipt guards passed
against both final logs; YAML parses. Hosted execution is not claimed.

The original generation pass covered all14 modes in4.022s and wrote their initial
traces, copied into the regression corpus. An initial development compile failure
(unused encoding/json import) has its log and exact test source preserved. A
prepared normal run passed before correcting an artifact filename spelling in CI;
its source manifest, log, executable and exit receipts are retained separately.
Final normal/race both bind the corrected CI source.

AST discovery reports161 families in seed-inventory.txt and the corpus contains
894 pins. This evidence qualifies the new family and14 new pins; it does not
qualify the complete161-family suite or all894 pins. Existing trace decision
tables were not changed. The four live frozen campaigns remain source-bound and
predate this family.

The workload models two-entry terminal metadata, not SDK effect execution, NATS
conformance, online repair or physical durability. Fixture corruption/healing is
explicitly test-only. Full current normal/race/extended, native fault matrices,
retired causal history, physical drain/power/soak/scale and rollout remain open.
