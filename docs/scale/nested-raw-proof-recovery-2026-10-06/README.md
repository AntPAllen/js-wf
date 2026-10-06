# Nested raw archive duplicates recovered

At pushed `76b95d8a005b081238162d378c16b67c21eb2009`, six complete canonical Git archives were read in full,
including all parts and member hashes. Five nested originals archives were also
read in full and compared against their complete original inventories; those
canonical-bound inner inventories are preserved here for subsequent fresh copies.
Current execution records where present matched canonical hashes and their SDK
PIDs were absent. Visible descriptor checks preserve their permission limits.

Removed only redundant raw archives: 2,177,323,008 allocated
bytes recovered, 3,145,932,800 bytes free at completion. Original/failed stores,
source, executables, metadata, caches, live stores and canonical Git proof remain.
Publication archives do not imply closure or a new runtime verdict. The297,722,514-
byte R5 profile publication also has a complete S3 readback copy with committed
receipt in `../s3-proof-offload-2026-10-06/`.

To reproduce a prior command requiring a reclaimed `originals.tar.gz`, reconstruct
that member from the recorded canonical outer archive; verify its hash against
this inner manifest before use. No original NATS fixture was reopened.
