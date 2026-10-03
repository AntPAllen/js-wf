# Rejected CAS comparison build

Hosted37121445966 atcd59b86 fails both placements before measurement because
the shared harness references `journal.UnmarshalEntry`, absent from fixed
baseline4fa311954f42d1325a46dad12bb5d128dad2b747. Raw original job logs are
retained with SHA256.

The benchmark writes JSON through journal.New on both revisions. Its diagnostic
now decodes that JSON through encoding/json, allowing the same measurement
harness to compile at both revisions. Both local build commands completed;
all110 original baseline source hashes match Git, with changes confined to the
shared measurement harness. Production journal, fixture and dependencies stay
at the fixed baseline. Build metadata and binary hashes are recorded.

The baseline validation copy is a source archive and uses `-buildvcs=false`.
This is build validation, with no measured throughput or performance gate claim.
