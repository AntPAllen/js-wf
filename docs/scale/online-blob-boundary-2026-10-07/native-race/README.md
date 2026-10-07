# Active-writer blob boundary: native race contract

Source `24307025a6d9f225cb6967ec1619c7810a14e90f`; actual race SDK PID 142864. The two original cases passed in 0.48 s on stock embedded NATS 2.15.0 R1, with distinct server identities. This is a controlled counterexample to using the quiescent collector while writers are active. It does not establish online GC safety.

The quiescent case retained the acknowledged input reference. The refresh-after-census case ran production `Client.Start` inside the collector's pre-delete hook: a fresh object generation was uploaded and its reference acknowledged, then the cached collector candidate deleted that generation. The resulting reference was dangling. Native minimum age is zero; the one-hour grace counterexamples are seeded model evidence only.

Independent review verified actual SDK identity, source and binary, both boundary proofs, all 2213 archive files and eight rejected actual-log mutations. The complete archive is 27,217,111 bytes, SHA256 `b6ce55975c038faebca376342e0f49eded7ab28542b616d771970ff3c0096f1b`. No original store was reopened. The retained archive preserves producer results; storage receipts are recorded separately.

The existing collector still requires writer quiescence. A safe online publication/collection protocol remains unimplemented. See [seeded preparation](../preparation/README.md).
