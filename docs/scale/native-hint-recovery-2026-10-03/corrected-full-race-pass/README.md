# Complete corrected simulator race suite

Exact source64bef042f3ea1935faf19105c9ece0a6acf6b57e passes the complete simulator
race suite in2063.82s:167 top-level passes, two documented trace-only skips,
267 pins and121,000 seeded bodies. Actual execution uses the simulator directory
for relative fixtures and a60m aggregate timeout. Coverage and liveness gates
remain unchanged.

All973 before/after source hashes match Git. Retained binary SHA256 and Go build
metadata confirm race instrumentation. Raw test events, inventory, execution
contexts, binary and source manifests are archived; every member SHA256 was
checked by reopening the archive before atomic rename.

This result precedes the Start enqueue-history checker correction and its two
new pins. It does not qualify the newer269-pin corpus or clear full release.
