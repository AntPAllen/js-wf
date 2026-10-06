# Working-tree archive duplicate recovery

At pushed `bfa1008`, 17 complete canonical archives were re-read from Git, including all parts, concatenation hashes and full file inventories. Old tar hardlinks were checked against previously verified member contents. The byte-identical working-tree parts were then excluded using sparse checkout; canonical Git blobs remain readable and unchanged.

Recovered 1,079,451,648 allocated bytes; 1,509,875,712 bytes free at completion. Unsupported inventories were skipped. Only working-tree part duplicates were removed: original/failed fixtures, raw archives, executables, source, metadata, caches and live stores were retained. Launch archive verification does not establish a live campaign verdict or process closure. Visible descriptor checks include their permission limits.

See `recovery.json` for exact inventories, skipped groups and descriptor observations; `executed-recovery.py` and `patterns-before.txt` preserve the operation.
