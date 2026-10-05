# Full-population reduction CPU profile preparation

TestAuditReductionFullPopulationCost uses the capacity dataset's same12-entry
JSON template for400,000 subjects /4.8M entries in three phases: decode only,
predecoded protocol/map reduction, and decode plus reduction. Reduction phases
include production sorted sixteen-journal finish with a constant in-memory
terminal lookup and require full400k/4.8M/400k journal reports.

This reuses raw encoded bytes and has contiguous subject order. No transport,
INV scan, KV watch, retained-state reads or fault model is exercised. Runtime
identity validation/profile instrumentation is included in timings. It cannot
qualify capacity or replace a native fullaudit.

Explicit four-core/GOGC200/2GiB profile matches the failed transport comparison.
Selected Git source/actual executable/closure captured; CPU labels distinguish
phases. Opt-in compile/skip passes; profile execution pending.
