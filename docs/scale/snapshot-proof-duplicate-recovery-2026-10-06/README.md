# Snapshot proof duplicate recovery

At pushed `bfd918b`, both complete native JSON/protobuf snapshot archives were
re-read from canonical Git parts and all member inventories verified. Committed
S3 full-readback receipts were bound to the identical archive sizes and hashes.
Actual SDK execution records matched archived originals and their PIDs were absent.
Raw archive and working-tree part copies matched canonical bytes; visible file
descriptors were checked with explicit permission limits.

Only those redundant raw archives and working-tree parts were removed (parts via
sparse exclusions), recovering130,174,976 allocated bytes. Free space at completion
was969,535,488 bytes. Original stores, sources, executables, metadata, caches,
canonical Git archives and live campaigns remain. S3 is an additional verified
copy; this operation does not infer remote provider durability or promote any
historical failure. No NATS startup was performed.
