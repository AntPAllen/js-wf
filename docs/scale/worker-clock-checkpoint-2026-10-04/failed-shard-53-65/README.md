# Failed full-campaign worker-clock shard 53–65

[Run 37164231641](https://github.com/AntPAllen/js-wf/actions/runs/37164231641), job 111324439563, exact source `79915ca41a5c5a23b9997eea5f3f66d82530ee30`, fails seed 61. Seeds 53–60 are producer passes, not independently qualified here; 62–65 were not executed.

The retained batch-90 checkpoint (2,520 invocation cutoff) reports **`retained audit: context deadline exceeded`** after **60.004493079 s**, with zero journals/entries/terminals in its partial report. The previous batch-80 audit passes in 16.396120238 s. The console's later timer `context canceled` message is secondary. This is an observation timeout, not proof of absent/corrupt state or a confirmed server cause. This old revision predates the per-attempt/primary-error diagnostic improvements already on main. No bound was relaxed and no new rerun was dispatched.

All 759 actual captured source inputs in both pre/post ledgers match executed Git for every executed seed 53–61. `raw-proof.tar.gz` preserves all raw upload files, terminal run/job metadata and console, artifact references, exact source inventory, executed review/preservation script and reviewer sources. All 905 members were independently read back and SHA256 verified; `manifest.json` records the hashes. `analysis.json` defines the exact failure/scope.

Raw artifact 11292301122 is downloaded. Original workload/store artifact **11292335973 (964,571,637 ZIP bytes)** is referenced but **not downloaded or verified**; actual binaries/physical stores are not claimed. No stores were reopened. The failed full parent remains ineligible; successful focused seed-6/seed-15 diagnostics do not repair it. Complete full matrix and 24-hour qualification remain open.
