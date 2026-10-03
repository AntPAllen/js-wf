# Full Tier3 artifact identity correction

The four rows retaining complete clock/upgrade stores used only the seed range
in their uploaded artifact name. In a full matrix, 64 uploads therefore selected
only 16 distinct names. The same name in different immutable artifact uploads
would collide. Focused single-row campaigns were unaffected.

Names now include both the row and seed range. A workflow-bound regression test
enumerates the actual full 200-seed plan and checks all 64 upload identities.
The old workflow at `f05d7ba` fails with `16 != 64`; corrected workflow passes
all five planner controls. This changes no production Go/module inputs and
qualifies no runtime workload. Original full dispatch `37156883771` is rejected
for this workflow defect; its cancellation has been requested before replacement.

The five-member archive contains original/corrected workflow bytes, executed
test and actual negative/positive results. All members reopen and SHA-verify.
Cancellation completion and replacement dispatch are recorded separately once
confirmed; request acceptance is not evidence of terminal cancellation.
