# Latest full checkpoint4160 copied recovery preparation

Adds an explicit profile for the original failed checkpoint4160/cutoff116480 from source `bc9f92bdfd1f01ad78d4acd24e6576d46c82a078`. The original checkpoint8520/cutoff238560 entry and physical totals remain exact and unchanged. Their actual retained peer-outage proof still passes all 68 substitution controls and is rejected under the latest profile.

The latest native diagnostic uses all five original file stores restored from the fully verified complete S3 archive, the original Raft identity, identical stock NATS2.15.0 executables, cold actual cursor-owner SIGKILL/same-store recovery, the original 20s parent, complete physical journal traversal, full116480 invocation/journal/terminal counts, native snapshot barriers, joined watches, zero audit cursors and all four R5 sources current before return. It supports the same explicit real watch-connection or peer-outage controls. It does not warm the journal before its fault.

The original latest checkpoint guard specified no fixed entry total. This profile preserves that guard plus nonzero entries and complete checker journal invariants, rather than importing an older donor count. Its reviewer binds executed checker source and full store admission; it does not independently decode the file-store payloads or establish an independently predicted logical entry total. It cannot qualify the concurrent 24h run or establish its original cause.

Producer and reviewer now admit a retired original only through committed original metadata, inventory, failed execution and S3 receipt, followed by complete archive restoration/member verification. No original stores are reopened. Future trials must use fresh roots.

Verification: both opt-in native test entry points compile and skip without fixture configuration (`go test ./integrity -run '^TestRetainedAuditBulkSoakCheckpoint(8520|4160)CursorOwnerRestartVerifiedCopy$' -count=1`, 0.008s); three Python source files parse, `git diff --check` passes; actual older fault proof passes68 controls. This is preparation, not latest native qualification.
