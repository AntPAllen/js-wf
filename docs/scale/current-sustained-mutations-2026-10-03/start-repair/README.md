# Corrected sustained Start repair component

Run `37154381145`, job `111295104283`, succeeds at exact source
`649c6ce8d40c10521da1a4f0feda066db4dc0332`. Artifact `11285922594`
(`sustained-production-start-repair`, 2,069,757 bytes) retains the original pair.
Independent revision-bound review verifies all 608 reported source files, the
exact production overlay, raw admission/fault/history/latency reports, intended
semantic failure, and compilation/unrelated-failure controls.

The baseline completes 97 batches, 2,716 invocations and 29,910 entries, with
19 leader kills over 640.138908462 seconds. The mutant completes 94 batches,
2,632 invocations and 28,992 entries, with 19 kills over 638.010838149 seconds.
Each runs the full 600-second mixed workload before its same-store challenge.
Raw baseline receipts show orphan invocation generation 2,745 repaired by a
dispatch tagged `start:guardshort.repair-orphan:2745`; the mutant retains orphan
generation 2,659 with no dispatch. This is the intended disabled-repair failure.
Worst terminal/progress p99 are 17.361650029/7.055789348 seconds.

The 29-member archive retains downloaded originals, completed job metadata/log,
actual reviewer bytes, independent verdict and summary. Every member was reopened
and SHA-verified before atomic rename. Physical stores and actual executables
were not uploaded. The original failed anchor job remains rejected.
The [separate combined review](../../sustained-component-verifier-2026-10-03/accepted/)
qualifies all six source-equivalent components; full matrix and 24h gates remain open.
