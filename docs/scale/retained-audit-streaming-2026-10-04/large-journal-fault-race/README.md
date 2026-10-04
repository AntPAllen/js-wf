# Large streaming journal-fault race control

Executed source: `71e6ad7332e3baa691273100dc4ed2d8873c3f9c`.
Actual race SDK: `f980acc69093f0efbf586c410661f8fe8168114548efe39c924beea6b3826458`.

The same file-backed three-node fixture contains12000 invocations,144000 journal
records and12000 terminal values. Every publish acknowledgment is checked. A
complete combined streaming/watch-state baseline must match before fault injection.
The actual R3 memory consumer leader shuts down at callback128 with143488 records
pending. Recovery visits all144000 records and returns the exact complete report
in10.745176241s under the unchanged20s deadline. Both stream consumers clean up
tozero under the named test assertions. GOMAXPROCS2/GOMEMLIMIT2GiB and race build
are verified against the captured live SDK process, now terminal.

All2894 selected input hashes,64 local Git source files, runner, actual SDK/all
Go build-info fields and raw result rows are independently checked. Complete
originals contain3277 members/156825358 bytes;43412854 compressed bytes split
into two parts. Original/member/part/concatenated digests all read back. See
manifest.json and independent-review.json. Concatenate numbered parts to recover
proof.tar.gz, containing actual binary, inputs, raw output and native stores.

This is library shutdown, not OS SIGKILL; no state-watch delivery interruption.
Stores remain retained, not reopened. 100k fault capacity, legacy/five-container
controls, full matrices and actual24h soak are separate. Default readers and
original audit budgets remain unchanged.
