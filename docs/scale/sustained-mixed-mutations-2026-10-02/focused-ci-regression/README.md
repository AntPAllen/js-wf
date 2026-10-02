# Focused mutation regression after shared harness extraction

[Run37018289819](https://github.com/AntPAllen/js-wf/actions/runs/37018289819)
is independently accepted at `026d574c45627438d18eee3110b466dca1a85062`:
all seven successful jobs, twelve actual named baseline passes and semantic
mutant failures, exact production and fixture hashes matching Git, exact
single-source overlays and both actual rejected runner controls per job.
Original CAS/enqueue/start receipts and purge/reuse generations independently
decode and match the required category counterexamples.

This validates the existing focused fixtures after extracting their common
cluster-borrowing helper. It does **not** clear the sustained ten-minute mutation
gate,200-seed full matrix or24-hour soak. No skipped test, build failure or
timeout counts as a semantic detection.

`originals.tar.gz` retains all original downloaded artifacts, terminal metadata,
full job log and independent report. Every member was read back and SHA256
matched to its original bytes; see `sha256.json`. The twelve independently
checked pairs and decoded proofs are in `independent-check.json`.
