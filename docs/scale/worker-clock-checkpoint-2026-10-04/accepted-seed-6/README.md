# Independently accepted ten-minute worker-clock seed 6

Run [37170797762](https://github.com/AntPAllen/js-wf/actions/runs/37170797762), job 111344671177, executes exact source `19f3cb36c148b95f94737400dbaa158247814fd6`. The requested individual seed passes independent raw/source/clock review and three independently rebuilt production history models. This does not explain the original seed-6 startup timeout or qualify failed full parent 37164231641, the full row, matrix, or 24-hour soak.

- 3,052 invocations, 33,907 journal entries, 19 faults; named test/package 845.52/846.556 s.
- Worst terminal/progress type p99: 5.151790801/0.411139508 s.
- 21 five-replica clock proofs, 105 broker messages, 48,956 raw dispatch records and eight fencing records; all 765 captured source inputs match executed Git.
- All ten intermediate checkpoints pass their first attempt. Last attempt: 11.195881565 s against the unchanged 20-second bound. Final whole-state assertions retain their actual named-test scope.
- Three independent production models pass 3,924 raw operations. All 45 actual Go/module dependencies match executed Git before compilation and after review. The source-isolated c4fed06 checkout has identical dependency bytes; current main's changed snapshot reader is deliberately excluded. The actual model-review executable and dependency sources are preserved.
- All 3,963 canonical archive and restored members verify by SHA256. Three actual clock workload executables, NATS executable and physical broker stores are retained in the complete nested original archive. Stores were hashed, **not reopened**.

`summary.json` records the exact metrics/checkpoint timings. `manifest.json` inventories all 231 proof members and five archive parts. Every outer member was read back and hashed; concatenated parts match the recorded complete archive SHA256. Reassemble outside the repository:

```sh
cat proof.tar.gz.part-* > /tmp/clock-seed6-proof.tar.gz
mkdir -p /tmp/clock-seed6-proof
# Verify the concatenated SHA256 against manifest.json before extracting.
tar -xzf /tmp/clock-seed6-proof.tar.gz -C /tmp/clock-seed6-proof
```

The bundle includes full raw upload, terminal API metadata/log, complete canonical `original-stores/rolling-originals.tar.gz` and manifest, independent reviews, all 75 actual reviewer sources, 45 model dependencies, model helper/executable, and exact executed preservation script. The original canonical tar is 108,405,901 bytes with SHA256 `13f5983e70dcb8a05d4fd4e34c50ec307f8564141af03af0f0b9c57a8cb755ff`.

`independent-review.log` retains an initial ambiguous CLI argument rejection; the corrected invocation is in `independent-review-final.log`. Generic shard review does not independently rerun history models; `history-model-proof.json`, the retained executable and exact `Ok` outputs provide that additional scope. Original failure artifacts and deadlines remain unchanged. No old 30-second-TTL recovery smoke was rerun.
