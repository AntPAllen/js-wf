# Further verified archive omissions

At pushed `1e068fd`,32 materialized archives of at least8 MiB were SHA256/size-checked against exact Git blobs, with no open descriptors, then omitted using existing noncone sparse checkout. **427,372,544 allocated working bytes** were recovered. All783 previously tracked Go/Python/YAML/module inputs remained materialized and byte-identical; HEAD and clean status were unchanged. Complete archives remain in local/pushed Git; failed originals outside Git and qualification are unchanged.

Exact paths/hashes, source inventories, before/after patterns and executed script are retained here. Disabling sparse checkout requires space for all omitted archives.
