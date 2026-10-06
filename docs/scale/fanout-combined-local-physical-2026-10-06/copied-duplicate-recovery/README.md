# Full copied fanout archive duplicate recovery

All six complete canonical archives at pushed c7a993a were read from Git, including
all parts, concatenations, members and embedded manifests. Current execution records,
actual SDK closure, current manifests, all duplicate bytes and visible task FDs
were verified before unlinking raw/staging duplicates and excluding worktree parts.
Descriptor permission limits are recorded explicitly.

Recovered **472,920,064 allocated bytes**; measured free
space afterward was 970,821,632 bytes. Canonical Git, manifests, original
and copied stores, sources, executables, caches and live campaigns remain.
