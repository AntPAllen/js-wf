# Completed full50000 SQL leaf fixture retired

The complete accepted original fixture/archive and its exact owned stopped PostgreSQL container/volume are retired. Fresh full compressed remote hash/every archive member, current complete local inventory, process/descriptor/Docker/mount/loop checks and terminal producer/SDK identity were verified. Before the owned container and volume were removed, all1,285 original closed-volume file hashes were verified again against the archived media copy and no other Docker container referenced that volume. Other fixtures, shared images, registered worktrees and the original24h remain local.

[Executed ledger](removal.json):1,335,463,936 allocated bytes (**1.24 GiB**) reclaimed across fixture, archive and closed owned SQL volume. Observed free bytes91,242,889,216, subject to live-run growth. Permission limits are recorded with closure evidence; original accepted verdict is unchanged.

## Fresh restoration

Download the complete archive using the standard authenticated S3 client and `../native/s3-readback.json` archive URL. Run `scripts/restore-full-fixture-proof.py` with `../native/archive-verification.json`, `../native/fixture-inventory.json` and a fresh destination. The complete compressed hash and every member are verified before restoration. The restored `postgres-stopped-data` contains the verified closed SQL media. Restoration does not start NATS/PostgreSQL, reuse historical process identities or alter the original verdict. Investigate fresh restored copies with newly recorded process identities.
