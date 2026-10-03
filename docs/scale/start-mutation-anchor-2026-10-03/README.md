# Start mutation harness anchor correction

Current-source campaign `37149508529` Start-repair job `111281642971` failed
at `c4fed061bc614488d4f89b53b216b756490f7da0`. Artifact `11284204778`
contains runner controls and a report with `mutation anchor must occur exactly
once`; neither a baseline nor a mutant sustained workload executed. The old
anchor expects unnamed return values, while the production scanner now returns
named `result ScanResult, scanErr error` to certify partial progress.

The anchor now matches that signature while preserving the intended mutation:
the production scanner immediately returns without repair. Selected anchors are
validated before compiling runner controls. A new regression checks all six
anchors against real source files and rejects missing/duplicate anchors. All
11 sustained harness tests pass in 0.087 seconds.

The focused production model baseline executes and passes; the corrected Go
overlay compiles and executes the same test, failing at `repair missing start`
with zero reenqueues. Commands, raw Go JSON, exact overlay and verdicts are
retained. Focused executables/build directories were not retained. This proves
the corrected overlay's semantic detection, not sustained qualification.

A separate comparison verifies all 606 tracked Go/module paths against
`c4fed06`, including fixtures. They are byte-identical; only Python mutation
selection/validation and documentation change. Existing simulation, matrix and
other mutation jobs continue. The failed campaign remains rejected; retry only
the affected Start-repair category at the corrected runner revision. No
six-category gate is cleared by this correction or equivalence comparison.

`originals.tar.gz` retains the failed job/artifact and focused correction proof,
including the complete file equivalence ledger. Every member was reopened and
SHA-verified before atomic rename; hashes are recorded in `manifest.json`.
