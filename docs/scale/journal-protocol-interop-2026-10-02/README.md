# Versioned protobuf interchange verification

Schema version1 covers all eight journal kinds with exact uint64 identity and
raw JSON payload bytes. Protoc3.21.12/protoc-gen-go1.36.6 regenerate the checked-in
binding byte-for-byte. Go race tests pass, including actual SDK production/replay
for four recovery states, nondeterminism, recorded errors, spilled results and
corrupted result hashes. Generated Python protobuf4.21.12 decodes all eight Go
vectors and creates eight independent reverse vectors; Go race verification
passes for those reverse values. Tool versions, vectors, hashes and logs retained.
Production WF_JRN remains JSON. This clears schema/adapters/codec interoperability
and documents recovery; persisted protobuf migration and second SDK remain open.
