# Full50000 packaged SQL recovery with actual stock-leaf SIGKILL

Accepted at recorded clean `0030e225058ed34e291e77485370c87e3332fce4`. Actual nonrace SDK1439144 passes381.73s body/381.777s SDK. Original50000/20m case/22m SDK/count1/twoCPU2GiB remain. No production code changes.

The original SQL relation-lock startup SIGTERM/exit0 check and all three full recovery phases pass unchanged: initial SIGKILL, every50000 result while stopped/100000 lag, partial SQL backend termination plus library journal-leader restart/fatal exit1, replacement SIGTERM0, zero final lag, all50000 exposed rows/indexed fields identical before/after rebuild, purge and dependency checks.

The additional actual packaged leaf-fault child1451498 makes partial progress from1126 to1127 rows. Stock WFEDGE leaf1439600 is SIGKILLed/reaped. The child reports broken-pipe/deadline transport failure, exits1 and releases all SQL sessions while the leaf remains dead. PostgreSQL responds normally; its backend is not terminated by the fault. Only then does leaf1452679 restart with the original executable/config/store/ports and a distinct actual server ID. Whole fault-to-heal is **6.095665s**, below unchanged30s. Final replacement completes the original full rebuild. This is supervised fatal-child replacement, not uninterrupted recovery of the same daemon.

Fault wire retains all1,073,190 client/5,768,671 server bytes, exactly one relayed connection, three accepted clients and two refused upstream reconnect dials. Actual interrupted tail bytes are **zero in both directions**. Every complete API publication uses WFVIEW and actual old-leaf INFO identity binds. The three original recovery traces total48,265,997 client/256,263,475 server bytes/409,600 API publications; these exclude startup/fault traffic. New fault-only tail support preserves/hashes an interrupted packet separately; such tails are never counted as complete API publications. All healthy traces remain strict.

[Independent review](independent-review.json) verifies2181 exact Git inputs,3360 external Go inputs,1285 closed SQL-media files, all five actual CLI children/two stock leaf processes, exact fault/reap/SQL-health/restart ordering, every final row and all9233 archive members. [37 leaf-fault mutations](leaf-fault-controls.json), [35 startup mutations](startup-controls.json) and [32 inherited recovery mutations](inherited-controls.json) reject. Three parser groups verify every byte split, strict healthy-tail rejection and malformed/domain failures; prior three healthy streaming control groups remain green. These are read-only reviews of the same native; no rerun or original-store reopening.

Complete archive **208,179,839 bytes**, SHA256 `d26070544748fcf35d11d8c58c84bff87025021f8706fb6408bf9fcb827e0ba7`. All original source/executables/closed NATS stores/stopped PostgreSQL media, every complete five-phase wire file and all logs are preserved. Only the small startup wire is copied to Git; the full fault and three recovery files remain in the complete archive. Manifests are committed before S3 upload.

Manual CI adds `standalone-leaf-sigkill`, retained systemd producer, all full original assertions, independent review and all three proof controls. YAML parses five profiles; hosted acceptance is separate.

This closes one recorded-source packaged SQL/leaf SIGKILL/fatal-child replacement component. It does not qualify hub process SIGKILL, naturally lost replies or follower lag, full fault matrices, onlineGC, original million physical drain, full release or actual24h. The isolated original24h remains active.

## Restoration

Download the complete archive from `s3-readback.json`, then use `scripts/restore-full-fixture-proof.py` with this directory's metadata/inventory into a fresh destination. Empty directories are omitted; no process starts. Local retirement requires fresh full remote hash/every-member verification, unchanged current inventory, process/descriptor/Docker/mount/loop closure and exact owned stopped SQL container/volume checks. Original verdicts remain unchanged.

Completed local fixture/archive and exact owned stopped SQL container/volume are now retired after fresh verification: [removal proof](../reclaimed/). All original bytes remain in S3.
