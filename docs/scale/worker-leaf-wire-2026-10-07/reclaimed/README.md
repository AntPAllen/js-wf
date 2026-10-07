# Local worker leaf fixture retired

Reclaimed **359.6 MiB** from the completed original worker leaf fixture and staging archive after fresh complete S3 compressed-hash/member readback, exact current inventory matching, and visible process/descriptor/Docker/mount/loop closure checks.

The original collector remains failed with exit1; native Go tests passed and corrected independent verification accepts the immutable captures. No verdict was rewritten and no stores reopened. Live24h and reusable scale fixtures remain local.

Restore the complete archive using [the committed receipt](../native-race/s3-readback.json), verify its compressed hash and every member against the inventory, then extract into a fresh directory. The static wire control fixture remains in Git for persistent verifier tests.
