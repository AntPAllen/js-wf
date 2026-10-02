# Current thirteen-row Tier2 matrix accepted at seed 1

Run 37050644803 succeeds at exact
`3f483bd61fba121a38c5b42e3aa84d07265ecab0`. All thirteen rows execute at least
ten minutes with successful named tests and package completion. The whole-matrix
checker validates every terminal job, checkout revision, seed, recorded retained
audit and latency verdict. Its campaign-revision result matches the current
checker result, apart from the new raw-event hashes. All thirteen raw Go JSON
row verdicts independently match the Actions logs.

Totals: 34,636 invocations and 313 faults. Worst aggregate terminal p99 is
15.010394 s, worst workload-cell terminal p99 is 18.069686 s, and worst
enabling-event progress p99 is 12.938768 s. This qualifies the current harness
across all thirteen variants, including the typed native-delete and concurrent
exit-observation fixes. It proves one seed per row, not 200 consecutive seeds
or the five-container 24-hour full-matrix soak.

The original eleven-row archive and this lossless supplement together preserve
all downloaded row artifacts, complete Actions logs/metadata, campaign/current
checkers, source inventory and review results. Every original eleven-row member
was rehashed before publishing the supplement; all supplemental members passed
SHA256 readback before atomic rename. See both manifests. Source attribution
uses verified Git checkout logs and 578 matching local Go/module/workflow files;
the workflow did not upload a complete source manifest or physical stores.

The proof concerns recorded runtime integrity checks and raw execution events;
it does not independently reconstruct the entire campaign from physical stores.
