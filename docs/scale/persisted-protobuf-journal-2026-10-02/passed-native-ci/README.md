# Actual persisted protobuf native CI pass

Run36974885921 at035403dc0093f2d83bf9bc5927e9e5b6b107a863 is terminal/success.
The compiled inventories explicitly contain both native and CLI tests. Original
logs show actual TestProtobufWorkerResumesJSONPrefixAndCompacts PASS3.32s and
TestWorkerRunnerProtobufJournal PASS26.52s (includes handler plugin build).
Fixture regeneration, journal/interchange race checks, marked-storage Python
decoding and eight independent reverse records pass. Original logs, metadata
and uploaded vectors/bindings/version data are retained. This proves focused
mixed-format storage/native/CLI interoperability at that source, not the full
fault matrix or reverse writer rollout CI (added at2b2130f).
