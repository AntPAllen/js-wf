# Current-source sustained determinism mutation component

Campaign `37149508529`, job `111281642953`, completed successfully at exact
`c4fed061bc614488d4f89b53b216b756490f7da0`. Public artifact `11284093479`
(`sustained-production-determinism`, 2,083,784 bytes) contains the original pair.
The overall six-category campaign was still live at this observation.

Independent `check_category` review from
`scripts/check-sustained-mutation-campaign.py` accepts this component. Runner and
reviewer match the recorded revision; all 608 reported source files match Git,
and the overlay is the exact single production mutation. Both raw phase reports
regenerate identically, including actual parent/package verdicts, completed
intermediate audits, preserved final cohorts, leader fault chronology and every
nanosecond latency sample. Compilation/unrelated-failure controls are rejected.

Baseline: 96 batches, 2,688 retained terminal invocations, 29,598 entries and
19 journal-leader kills; measured workload elapsed 635.715126473 seconds.
Mutant: 98 batches, 2,744 retained terminal invocations, 30,242 entries and
19 kills; elapsed 637.493138707 seconds. Both execute the full 600-second mixed
workload before the controlled challenge on those same stores. The baseline
rejects a changed step request. Removing the determinism guard executes the
changed effect once and produces result 42/Completed, the intended I4 escape
and semantic test failure.

Totals: 5,432 invocations, 59,840 entries, 38 faults. Worst per-workflow terminal
and progress p99 are 15.360881559 and 7.056596705 seconds.

The 27-member archive retains all downloaded artifact files, completed job log,
campaign/artifact observations, independent verdict and summary. Each member was
reopened and SHA-verified before atomic rename; hashes are in `manifest.json`.
Physical stores and actual executables were not uploaded or independently
reopened. This accepts only the determinism category at the recorded source.
Together with the reviewed lease component, two of six categories are accepted;
the remaining four categories, full matrix/200-seed gates and 24h soak are open.
