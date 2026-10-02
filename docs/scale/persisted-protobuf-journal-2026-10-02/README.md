# Persisted protobuf with mixed-format readers

ProtobufV1 storage is WFJ-NUL plus the version1 envelope with sequence0; physical
sequence stays supplied by JetStream. Shared record projection is used by both
interchange and storage. New JSON writes are byte-identical to the previous JSON
marshal. JSON remains default; worker option/CLI flag enable protobuf writes only
after all readers have been upgraded. Mixed history is supported without rewriting.
Unknown format/version/fields or stored sequence fail closed. Old JSON-only
readers are incompatible; downgrade to old binaries is not supported while any
protobuf entries remain. Snapshot arrays retain their established JSON format.

Race codec/interchange/auditor/CLI-package checks pass. R3 worker fixture passes
4.886s package/3.86s test: a real legacy lease writes a JSON prefix and releases;
a new higher-epoch worker writes protobuf, one spilled effect completes, three
peers audit four entries/one terminal, quiescent sweep preserves live and archived
blob references, and snapshot compaction/read/audit pass. The first fixture used
an artificial epoch without acquiring the lease and was correctly rejected by
the auditor; the corrected fixture uses production acquisition/release.
CLI flag race smoke passes3.090s, including protobuf byte verification, metrics,
retention workflow and same-ID reuse. Client/retention/worker race packages pass.
All repository packages compile and affected packages pass vet.

The production append/read/fencing transport runs1000 independently counted
seeds with both encoding orders and drop-before-commit/lost-ack/unchanged-CAS
faults. All six combinations and exact replay pass race1.518s. The seed42 disk
pin passes race1.042s. Corpus now259 pins/114 seeded workloads. This does not
replace full1000/100k or protobuf rolling/chaos matrix acceptance. The already-live
100k campaign continues on its original source; it does not include this workload.

The original plan specifies a Go-only SDK. A second executable SDK is future
scope, not an additional requirement for completing this plan.
