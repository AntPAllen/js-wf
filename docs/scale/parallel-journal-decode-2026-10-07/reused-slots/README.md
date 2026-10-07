# Reuse the decode destination

The worker now decodes directly into an existing bounded batch entry slot. A failed decode clears and removes its slot, preserving the successful ordered prefix. Completed batches clear all retained message/entry references before reuse. This removes the compiler-confirmed per-record worker destination escape from the first candidate; the real journal decoder still has its own allocations.

All ordering, mixed-encoding, filtering, first-error, partial-prefix, visitor-error, bounded-backpressure and cancellation/worker-join controls passed count20 in normal and race builds. Compiler escape analysis no longer reports a worker-local Entry moving to the heap. [Review](review.json) binds the decoder bytes, toolchain and compiler output hash. This is not a measured heap or throughput reduction.

The preceding full native run remains accepted at d86b0e4, before this change. Full native capacity and performance for the reused-slot source are pending. Existing public checker APIs still select serial decoding; the original20s attempt and full238560/2630779 workload remain required.
