# Closed R5 disposable clone recovery

After the complete base-plus-delta proof was pushed at `af0cfef`, every canonical
delta part, concatenation and complete virtual tree was reverified. Every current
captured file matched its preserved size/SHA256, including the exact 640-file
copied-store set. SDK/observed server PIDs, historical Docker objects, current
running mounts and visible task descriptors were checked again. Descriptor
permission limits are recorded explicitly.

Only `/tmp/js-wf-r5-retained-profile-restored-20261005/copied-stores` was removed.
Recovered **864,817,152 allocated bytes**; measured free
space afterward was 1,524,555,776 bytes. All other captured files were
rechecked unchanged after removal. Canonical Git base/delta, original donor and
failed native fixtures, sources, executables, metadata, caches and live stores
remain. No NATS store was reopened; historical diagnostic verdicts are unchanged.

[Complete reconstruction proof](../r5-profile-derived-store-preservation-2026-10-06/).
