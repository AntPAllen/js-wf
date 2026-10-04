# Independently accepted worker-clock seeds 27–39

[Run 37164231641](https://github.com/AntPAllen/js-wf/actions/runs/37164231641), terminal successful job 111324439636, executes exact `79915ca41a5c5a23b9997eea5f3f66d82530ee30`. All thirteen requested ten-minute seeds pass independent raw/source/clock review and three independently rebuilt production history models.

- **39,452 invocations, 437,316 journal entries and 247 faults**.
- Worst terminal/progress type p99: **5.254787923/0.80936262 s**.
- All **759** captured source inputs match both pre/post ledgers and executed Git for every seed.
- **273** clock proofs, **1,365** broker-clock messages and **135** completed-cohort checkpoint audits verify against raw files. Old source lacks per-attempt checkpoint timing; no first-attempt claim is made.
- All three production Start/Signal/Await history models independently pass **50,724 operations**. All 45 actual Go/module dependencies match executed source before compilation and after review; the actual helper/model executable and sources are retained.

All **51,520** canonical archive members SHA256-verify. The unchanged 1,235-file raw upload is supplemented only by exact matching non-store canonical members needed for review. There are 39 clock binary paths but only three distinct executable hashes; 132 byte-identical added files are materialized through hard links. Every referenced binary's actual bytes verify, and the committed compact proof retains those executables. Physical cluster files are streamed and hashed from the complete original archive, not extracted or reopened. No independent store-reopening/drain result is claimed; final full-state/drain assertions retain their named-test scope.

`manifest.json` inventories the compact proof and its 25 MiB parts. Every regular member and hard-link content is read back and SHA256-verified; concatenated parts match the complete compact archive digest. Reassemble sorted `proof.tar.gz.part-*` outside the repository, verify the digest, then extract. The bundle retains the full raw upload and actual clock/model binaries, all reviewer/model sources, independent reports/outputs, exact preservation/restoration/model scripts, API metadata/logs and the full original-store manifest.

The complete canonical original store archive is **1,391,980,519 bytes**, SHA256 `873a28800bd786c746a8eb81ba4e94fe8b4cea3a20e712f55f7e2f714e4f1349`, retained at the RAM path in `summary.json` and in GitHub original artifact **11292227392** (ZIP 1,390,721,397 bytes). Root capacity prevents including it in this compact Git proof. RAM is not reboot-durable; GitHub metadata expiry is **2027-01-02T00:12:33Z**, not a guarantee of indefinite availability. Full physical stores and the broker executable remain in that canonical archive; the compact proof contains their verified manifest, not their bytes.

`root-transfer-error.json` records the initial local temporary-ZIP capacity failure. Repeating the same artifact transfer with a RAM temporary directory completes; no workload/campaign was rerun or original data changed. This qualifies **only complete shard seeds 27–39**. Full parent 37164231641 already has failed clock jobs and remains ineligible. These results do not explain startup/audit failures, qualify the full 200-seed row or matrix, or satisfy the original 24-hour full-matrix gate.

## Duplicate RAM expansion removed after verification

At pushed proof commit `821b1bb`, all 1,378 expanded inputs, every canonical compact archive member and all committed Git archive parts were SHA256-verified. No open descriptors remained. Removing only the duplicate raw expansion recovered **722,939,904 exclusive allocated bytes**. The actual clock executables and SDK source bytes remain in the canonical compact archive and committed Git proof; their former provider raw paths are removed. The actual model executable and full canonical original-store archive remain retained. Restore the compact proof before using provider paths or repeating raw review. Failed evidence is unchanged. `duplicate-removal.json` and `executed-raw-recovery.py` record the checks and exact paths.
