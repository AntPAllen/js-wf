# Additional verified archive omissions

At pushed `1d8b92c`, eight newly committed archive working copies were checked against exact Git blob hashes and sizes, with no open descriptors, then omitted using the existing noncone sparse checkout. **189,964,288 allocated bytes** were recovered. All781 tracked Go/Python/YAML/module inputs present before the change remain materialized and byte-identical; HEAD and clean status were unchanged. The complete archives remain in local and pushed Git. This changes local working-copy storage only; failed originals and qualification are unchanged.

`recovery.json` lists every exact path/hash, and the retained script, source inventories and sparse patterns record the operation. Disabling sparse checkout requires enough space for all omitted archives.
