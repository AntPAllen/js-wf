# Retire the redundant candidate input restore

The 5,634-file component matches the committed full fixture inventory at `docs/scale/lease-partition-component-2026-10-06/contiguous-component`. Its complete S3 archive is already retained. This cleanup removes only the idle local restore after fresh full compressed-byte/member verification, unchanged local inventory and process/descriptor/container/mount closure. The restoration recipe and receipt are preserved here; future investigation can restore a fresh copy. The active full126 normal qualification is separate.
