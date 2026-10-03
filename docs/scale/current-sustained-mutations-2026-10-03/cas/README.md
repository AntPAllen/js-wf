# Current-source sustained CAS mutation component

Campaign `37149508529`, job `111281642898`, completed successfully at exact
`c4fed061bc614488d4f89b53b216b756490f7da0`. Public artifact `11284483693`
(`sustained-production-cas`, 2,028,727 bytes) contains the original pair.
The overall six-category campaign was still live at this observation.

Independent `check_category` review from
`scripts/check-sustained-mutation-campaign.py` accepts this component. Runner and
reviewer match that revision; all 608 reported source files match Git, and the
overlay is the exact single production change. Both raw phase reports regenerate
identically. Actual parent/package verdicts, intermediate audits, preserved
cohorts, leader fault chronology, raw latency samples, compilation and unrelated
failure rejection controls verify.

Baseline: 96 batches, 2,688 retained terminal invocations, 29,641 entries and
19 journal-leader kills; measured workload elapsed 615.332991918 seconds.
Mutant: 97 batches, 2,716 retained terminal invocations, 29,928 entries and
19 kills; elapsed 616.939446866 seconds. Both execute the full 600-second mixed
workload before the controlled challenge on those same stores. The baseline
admits one CAS winner and rejects the other. The missing-CAS mutant retains
receipts at stream sequences 30,077 and 30,078 on the same invocation subject;
both decode to identical index-2 `StepCompleted` entries at epoch 64,029 with
result 42. The raw-state checker rejects the duplicate logical index, producing
the intended semantic test failure.

Totals: 5,404 invocations, 59,569 entries, 38 faults. Worst per-workflow terminal
and progress p99 are 13.031542858 and 7.054885611 seconds.

All downloaded files, completed job log, observations, independent verdict,
decoded receipts and summary are retained in the 27-member archive. Each member
was reopened and SHA-verified before atomic rename; hashes are in `manifest.json`.
Physical stores and actual executables were not uploaded or independently
reopened. This accepts only CAS at the recorded source. Together with lease and
determinism, three of six categories are accepted; enqueue, Start repair, purge,
the independent full matrix/200-seed gates and 24-hour soak remain open.
