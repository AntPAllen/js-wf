# Independently accepted clock diagnostics: seeds 112 and 121

Both focused ten-minute diagnostics pass at exact `3b2999ec9ec3a9dd131ffe742fdfd48c3cfbaf34`:

| Seed | Successful run / job | Invocations / entries / faults | Slowest checkpoint attempt |
| ---: | --- | --- | ---: |
| 112 | [37183948633](https://github.com/AntPAllen/js-wf/actions/runs/37183948633) / 111381924505 | 2,940 / 32,488 / 19 | 18.675611114 s |
| 121 | [37183950090](https://github.com/AntPAllen/js-wf/actions/runs/37183950090) / 111381922136 | 2,940 / 32,576 / 19 | 19.351887878 s |

Each of the ten checkpoints (batches 10 through 100) completes on its first attempt, without an error or retry, inside the unchanged 20-second attempt budget. At batch 90, where the historical failures occurred, elapsed times are 17.344681710 / 19.351887878 s. The latter has only about 648 ms of margin; it does not establish why the original 60-second audits failed. Keep the original 20-second attempt / 60-second total limits.

Independent review binds terminal job/source identities, all 786 captured source inputs per diagnostic, clock proof messages, fault histories and unchanged raw uploaded files. Each run records 21 clock proofs and 105 broker messages. Worst terminal/progress type p99 is 5.153438515 / 0.390463839 s across both runs. All three production Start/Signal/Await models pass 7,560 history operations with all 45 actual Go/module dependencies matching the executed source before and after. The actual model executable and three actual workload executables, their build information and exact clock overlays are retained.

Both complete original-store uploads were downloaded. Every one of their 3,963 canonical archive members was SHA-verified against its manifest; the eight required clock assets were restored from each run's own originals. Byte-identical assets share eight hardlink members in the compact proof. The original uploaded raw files remain unchanged. Stores were not independently reopened.

`summary.json` records counts, individual attempt timings, original archive hashes/bytes/locations and GitHub expiry dates. `manifest.json` binds the 353-member, 90,769,650-byte compact proof, including eight hardlinks. All member contents and concatenated parts were independently read back and SHA-verified. The proof contains complete raw evidence, API snapshots, original manifests, actual executables, source/model inputs and executed reviewer/assembly scripts. Concatenate `proof.tar.gz.part-*` in lexical order and verify the recorded canonical hash before extraction.

The two approximately 106 MB physical original archives are retained in RAM at the exact paths in `summary.json` and in GitHub artifacts 11296412730 / 11296327992 until their recorded expiry; they are not duplicated into Git. RAM copies are not reboot durable. This is successful focused diagnostic evidence, not qualification of the failed historical parent, a complete row/matrix or the actual 24-hour soak. The original server-side cause remains unconfirmed; no new production fix is attributed to these results.

## Availability after the VM restart

The historical RAM archive paths in `summary.json` disappeared at reboot. Both original archives are now independently restored to persistent root-disk paths, with canonical hashes, committed manifests and all 3,963 members each verified. [Current locations and exact restoration proof](../../../post-restart-evidence-2026-10-04/clock-originals-restored/) supersede the prior RAM availability statement. The summary and compact proof retain their original execution records unchanged; qualification and no-reopen boundaries remain unchanged.
