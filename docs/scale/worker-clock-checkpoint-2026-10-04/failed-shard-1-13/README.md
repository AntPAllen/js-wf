# Additional failed worker-clock shard 1–13

Full run **37164231641**, job **111324439684**, exact `79915ca` fails at
worker-clock **seed 6** after producer passes for seeds 1–5. Its primary log
error at 01:37:41.2149551Z is `stale or future server clock sample` for
01:37:11.11267929Z, about 30 seconds old. Cancellation and graceful-process
counter mismatches follow. Named test/package fail at 43.37/43.422 seconds.
This differs from seed 15's retained-audit deadline. No server cause or recovery
fix is established. Earlier producer passes are not independently qualified.

The archive retains terminal job metadata, the complete original job log,
full current API artifact inventory, selected references and scoped analysis.
Every outer archive member hash-verifies. Raw artifact **11289888005**
(14,697,884 bytes) and store artifact **11290782676** (617,981,262 bytes) are
referenced only: their payloads have not been downloaded or independently
hash-verified. No physical stores are reopened. The original full parent
remains failed; other ongoing jobs and the Tier2 campaign are unchanged.
