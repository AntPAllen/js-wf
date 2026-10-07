# Native blob uploader and collector SIGKILL recovery

At **5fdfbafdf9257ddb0aecd01f5cbdb1db2e2637fd**, actual retained race SDK **2701741** passes the entire `internal/blobpublication` package in **40.06s**, count1/two Go CPUs/512MiB/original3m. New process cases retain the native-object **30s parent** and an explicit **30s child SDK bound**. Existing native-authority20s parents are unchanged. All **2,354** selected committed inputs match Git, captured files, and source-before/after. The actual parent SDK's stable live identity/hash/profile/clean revision and closed lifetime are verified.

Six new R1/R3 cases run a real subprocess of the actual retained SDK through the native protocol and a complete-packet TCP proxy:

| Child boundary | Observed state before SIGKILL | Recovery after kill/join |
| --- | --- | --- |
| Uploader before metadata publication | Three acknowledged physical chunks, no metadata, uploading generation1 | New collector fences destination, closes generation, tombstones and purges the partial upload |
| Uploader before destination-root publication | Three chunks plus committed object metadata, ready generation1, root CAS packet held | New collector advances destination head before removing the pin and physical bytes |
| Collector before physical purge | Root already retired, generation1 closed, attempt tombstone committed, three chunks retained, exact purge request held | A new generation2 root is committed; another collector reclaims only the old generation's chunks |

Every child is admitted by PID/birth/exact arguments/critical environment/actual executable hash, observed twice with identical identity, and verified to execute the **same captured binary** as the parent. Parent `Process.Kill` sends SIGKILL; `Wait` joins the child and verifies native signal9, exit code−1 and `/proc` absence. The captured held request is cancelled with **zero forwarded bytes**. All cases commit and read a new generation2 of the same logical content, preserve it across old-object reclamation, then retire it. Final native subject census contains exactly two permanent metadata tombstones and zero chunk subjects/consumers. These are **actual uploader/collector OS process kills**, not the earlier controlled connection-loss substitute.

Independent complete TCP framing verifies the original three chunk publications/content hash and acknowledgments, committed metadata or collector tombstone as applicable, exact held root-CAS or chunk-filter purge payload, and absence of the killed publication from forwarded bytes. The observed INFO callee ID/name/version/embedding revision matches the retained native fixture peer. No acknowledgement for the held request is invented.

The same retained run requalifies all14 preceding native object scenario proofs, including standard shared reads, late chunks, metadata reply loss, all-peer graceful same-store cold restart and unsafe configurations. **231 actual-positive native/wire substitutions** reject; **14 additional foreign-callee-ID substitutions** reject. [Main review](independent-review.json), [callee binding review](peer-binding-review.json). The complete **3,431-member** fixture archive preserves parent SDK, six child logs, native media, complete wire/identity proofs and source snapshots. The initial offline reviewer output-directory failure is separately retained; the native SDK was not repeated to fix it.

## Remaining runtime and release work

The experimental native publication destination and dedicated recoverable bucket are still separate from runtime WF_INV/WF_SIG/WF_JRN/WF_STATE publication and retained history. Migration and all reference writers/readers, lifecycle permissions, epoch/tombstone compaction, arbitrary partitions, lost root replies, wider native concurrent writer/collector/scale qualification, native NATS-server OS process kill and power loss remain open. Production still uses quiescent blob collection. This component does not qualify the current full124 graph, full native matrices, original24h, million physical drain or default dependency adoption.

Archive/S3 readback and local retirement are recorded by committed receipts when complete. Restore to a fresh directory before reuse; restoration starts no broker.

Complete S3 archive/metadata/inventory readbacks are verified. Fresh complete remote member/hash checks, unchanged original inventories and process/descriptor/all-Docker/mount/loop closure permit retiring original fixture, archive and staging: **133,521,408 allocated bytes (127.3 MiB)** reclaimed. [Exact removal ledger](../reclaimed/README.md).
