# Full combined audit compatibility on NATS 2.11.17

Executed source: `b3c64f185b49b270391beb730a12e39263c64910`.
Actual race SDK: `863346696d8fbc417b6c12a9d3314b0219bb76971b3de9556e9fb7927973f822`.

The corrected retained native control passes in34.55 seconds. Original point,
batch, state-snapshot and both streaming modes match reports and exact errors
through compaction, cohort filtering, later malformed records, terminal
corruption/Delete/recreation, snapshot corruption/repair and orphan detection.
Both public combined APIs pass under20-second audit limits. Three actual NATS
2.11.17 processes verify running executable hashes against the retained legacy
binary dc3a94debfc18ee9762c783d941db36a8360955d9686c86bd660a9acc63701aa.

All2896 selected inputs /66 Git-local files, actual runner/SDK/all build-info
fields and3284 original members /136946618 bytes verify. Complete58021062-byte
archive has three parts; every original/member/part/concatenated digest reads
back. See manifest.json and independent-review.json. Concatenate numbered parts
to recover actual executable, source inputs, raw output, native stores and legacy
binary. The modern fixture server is built but not executed. External live SDK
capture was missed; legacy running executable digests were checked in the test.
All processes are terminal. Stores are retained, not reopened.

The first incorrect expected-error run stays failed and preserved separately.
This is compatibility evidence, not legacy scale/fault or five-container/full
matrix/actual24h qualification. Default readers and audit limits stay unchanged.
