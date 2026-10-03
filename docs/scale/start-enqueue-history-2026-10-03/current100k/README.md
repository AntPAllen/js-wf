# Complete current 121-workload 100k campaign

[Run37120761679](https://github.com/AntPAllen/js-wf/actions/runs/37120761679)
uses exact `9de1e726f6633e29a52f8b7b45e44b4ae1ac08c4`. All121 source-inventoried
workloads complete the exact contiguous seed range1..100000:12,100,000 seed
bodies. All269 regression pins and168 top-level tests pass; only the two
explicit trace-only tests skip. Package time7106.148s.

Independent review regenerates tier1-result.json byte-for-byte from raw Go JSON,
verifies all170 test names and269 pin paths against exact Git source, and
regenerates the121-workload AST inventory. Counts are12,300,039 schedules,
179,746,693 choices and2,694,012,558 transport events, including fixed/repeated
cases in the aggregate. The producer did not retain the compiled binary or
before/after source hashes; separate full1k/race evidence at the same revision
preserves those build proofs within its own execution scope.

All uploaded originals, terminal metadata and review utility are archived;
every member was SHA256-readback checked before atomic rename. Extract to a
temporary directory and run its review.py with that directory and the repository
checkout as arguments. The AST review requires unchanged sim source at the
recorded revision. This accepts the complete Tier1 seeded campaign at its
recorded source, not any real-cluster matrix, the24h soak, or later runtime changes.
