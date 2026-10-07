# Development configuration-fixture failure

The original race development command was `GOMAXPROCS=2 GOMEMLIMIT=512MiB go test -race ./internal/blobpublication -run '^TestNativeObject(SameStore|Unsafe)' -count=1 -v -timeout=3m`. It failed after 6.415s. Both R1/R3 all-peer same-store restart cases passed; the purge-denied configuration case failed during provisioning with actual NATS API error10052, “roll-ups require the purge permission.”

The fixture incorrectly expected that combination to be provisioned before adapter validation. The correction explicitly checks this server configuration rejection, while other unsafe configurations continue through native creation and adapter rejection. This initial verdict stays failed. Ordinary Go temporary stores were not retained; no source-bound actual SDK/media/archive qualification is claimed. The two uncommitted development source snapshots and complete command output are retained here.
