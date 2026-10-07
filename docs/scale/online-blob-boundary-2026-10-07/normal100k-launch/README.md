# Focused normal100k live admission

Actual SDK **265617**, source `3a7022c5b29b68cf8b52632eecaa9785bc980764`, normal binary SHA256 `c966df59d51b2bbdaa08f50330e4be84387f20e4a1b6c919bc3138f6429c7d6b`. Stable executable/birth/argv/cwd/environment admission verifies. This runs the new production blob-boundary workload at seeds1..100000 with every generated trace exactly replayed, plus its three shared regression pins. It is one workload, not the complete Tier1 graph.

The SDK uses 2 Go CPUs/1GiB, count1 and the existing normal-suite 300m ceiling. Low-priority producer service: User exedev/Nice19/CPUWeight10/MemoryMax2GiB/Restart no. Stores are modeled in memory; no native cluster is started or donor store reopened. Complete selected source and binary are retained in `/tmp/js-wf-online-blob-normal100k-v2-20261007`.

A separate terminal reviewer waits for the original producer service and verifies source blobs, actual SDK, exact seed/mode/event counts, pins, actual-log negative controls and complete archive/current inventory/closure before acceptance. Reviewer service `js-wf-online-blob-normal100k-review-20261007.service`. Launch admission does not qualify the terminal result or online GC safety.

[Earlier launcher failures](../normal100k-failed-launches/README.md) remain failed and preserved; v2 creates the missing sample directory. The isolated 24-hour journal test continues unchanged. Main runtime sources stay frozen until this finite producer finishes.
