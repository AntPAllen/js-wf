# Further exact archive working-copy recovery

At pushed `3b2999e`, 11 newly materialized committed archives of at least 8 MiB are checked against exact Git blob hashes/sizes with no open descriptors, then omitted through existing noncone sparse checkout. **280,809,472 allocated bytes** are recovered. All 786 previously tracked Go/Python/YAML/module inputs remain materialized and byte-identical; HEAD and clean status remain unchanged. Full archives remain in local/pushed Git, including the new failure proof. Failed original expansions outside Git remain untouched; qualification is unchanged.

Exact selected paths/hashes, source inventories, before/after sparse patterns and executed script are retained here. Disabling sparse checkout requires sufficient space for every omitted archive.
