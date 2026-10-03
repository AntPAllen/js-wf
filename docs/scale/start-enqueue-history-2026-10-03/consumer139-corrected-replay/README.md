# Corrected consumer seed 139 replay

[Run37125258217](https://github.com/AntPAllen/js-wf/actions/runs/37125258217)
executes source `c0dc4d707e3c4c9d04dfb189fda0f2da0d3fcb97`, seed139/10m.
The named test passes in619.45s:97 full cohorts,2716 terminal invocations,
29947 journal entries and19 scheduled consumer-leader faults. All19 target
named partition consumers with positive pending/acknowledgment counts.

Independent review verifies terminal job/test/package metadata, every raw fault
and all12804 latency samples, recomputes the aggregate and six-cell terminal
and progress statistics exactly, and reruns all three production history models
on3492 original SDK operations. Checker source matches the campaign revision.
Aggregate terminal p99 is7.310798702s; worst type terminal/progress p99 are
11.988388370s /7.051786386s. No progress event exceeds30s.

All uploaded originals, metadata and review utilities are in originals.tar.gz;
each member was SHA256-readback checked before atomic rename. Extract into a
temporary directory and run its review.py with that directory and a repository
checkout as arguments. The named final passing test establishes that its raw
integrity and queue-drain assertions ran; physical stores were not uploaded,
so this is not an independent retained-store audit. The original rejected seed139
remains rejected. This replay does not qualify the default1..200/full matrix.
