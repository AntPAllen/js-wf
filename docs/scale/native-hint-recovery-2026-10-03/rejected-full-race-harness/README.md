# Full race attempt rejected; harness working directory corrected

The complete default-1k attempt at `e824cc1` is rejected. The retained binary
has verified `-race=true` metadata and SHA256
`6bed4334671d1f644828848dc4b5df0476fc450ce69e7b24a1ec3871a93b3a63`.
All 971 inventoried source hashes match before/after and exact Git bytes.

The binary ran from the repository root instead of Go's normal package working
directory. `TestPinnedRegressionCorpus` failed because `testdata/regressions`
was absent there; `TestSeededWorkerObservedRenewalCost` failed opening its
relative `testdata/costs` fixture. The package subsequently hit its aggregate
30-minute timeout while executing `TestSeededWorkerSnapshotExecutionReplay`.
It completed 152 top-level passes and two documented skips; this is incomplete
and cannot qualify the full race suite.

The same retained binary, invoked from `sim/`, passes both previously failed
fixture cases in 27.953 seconds: 267 pinned subtests and all 1,000 observed
renewal-cost seed bodies. Complete raw JSON is retained with verified gzip
readback. This focused result proves the path correction, not full coverage.

The rejected suite's completed scan-capacity and result-budget tests took
471.42 s and 167.82 s respectively. The fixed snapshot helper at seed42 passes
in 0.12 s; its short CPU profile chiefly samples race instrumentation and JSON
work. The initial profile invocation requesting ten seeds is explicitly
rejected by the existing minimum-1,000 seed guard and provides no coverage.

The producer now runs the binary from `sim/`, inventories all `sim/testdata`
fixtures, records each command's actual working directory and allows 60 minutes
for the aggregate instrumented package (75-minute hosted job). All 1,000 seeds,
per-operation timeouts, modeled deadlines and p99 limits remain required.

`originals.tar.gz` preserves 22 original members, including the exact compiled
binary, inventories, raw suite events, timeout stacks, and both profile setups.
Every archive member was SHA256 checked by readback before atomic publication.
The original temporary roots remain untouched.
