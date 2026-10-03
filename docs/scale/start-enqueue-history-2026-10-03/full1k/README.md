# Current checker complete default1k qualification

Exact source9de1e726f6633e29a52f8b7b45e44b4ae1ac08c4 passes every compiled
top-level simulator test except the two documented trace-only skips:168 passes,
269 pins and121,000 seeded bodies across the unchanged121 workloads.

The producer executes one retained non-race binary from the simulator directory.
Before/after source inventories agree and every source hash is checked against
the exact Git revision. Binary SHA256 and build metadata are verified. Raw Go
events are independently checked by the existing inventory/seed coverage verifier.

The archive includes raw events, binary, inventory, source hashes, command working
directories and the executed producer. All archive member SHA256 values are
checked by reopening the temporary archive before atomic rename.

This qualifies the default1k suite, including the corrected Start checker and
two fixed replay pins. Current-source full race,100k and real matrices remain open.
