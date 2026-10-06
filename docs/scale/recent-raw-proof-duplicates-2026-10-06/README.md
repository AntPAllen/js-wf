# Verified canonical archive duplicate recovery

On 2026-10-06, nine closed canonical archives were read from pushed Git at
`cdd4c7b`, including all parts, concatenated hashes, members and embedded manifests.
Current execution/manifest bindings, SDK and observed process closure, archive
bytes, working-tree part bytes and visible task descriptors were checked before
removing duplicate raw archives and excluding duplicate working-tree proof parts.

Recovered **928,423,936 allocated bytes**; recorded free space was 2,196,103,168
bytes. See `recovery.json`, the executed script and prior sparse patterns.
Permission limits of descriptor inspection are recorded explicitly.

Canonical Git evidence, original stores, donor fixtures, sources, executables,
caches and live stores remain. Reconstruct a removed raw archive from its canonical
Git parts before using a verifier that requires that raw archive path.
