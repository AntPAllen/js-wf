# Full current-graph Tier 1 acceptance

At exact431c934acb943c12a9c8d3c41ac70a2ea287c5b2, the full simulator package
passes in166.468s:162 top-level passes, two documented trace-only skips, all264
source pins and all118 source-inventoried seeded workloads completing exact
seeds1..1000 (118,000 bodies). Aggregate120,033 schedules,1,790,011 scheduler
choices and26,888,132 transport events. Compiled test inventory and the source
AST seed inventory are checked by `check-tier1-suite.py` against actual JSON
execution. Every571 recorded source file matches its exact committed Git blob.
All original files are retained losslessly with SHA256 readback verification.

This clears full1k for this graph. Current-graph100k run37028379931 is launched
at this same source, not accepted. Earlier116/117 campaigns retain their scope.
