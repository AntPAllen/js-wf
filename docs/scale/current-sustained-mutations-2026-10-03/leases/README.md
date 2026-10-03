# Current-source sustained lease mutation component

Campaign `37149508529`, job `111281642881`, completed successfully at exact
`c4fed061bc614488d4f89b53b216b756490f7da0`. Public artifact `11283967724`
(`sustained-production-leases`, 1,995,483 bytes) contains the original pair.
The overall six-category campaign was still live at this observation.

Independent `check_category` review from
`scripts/check-sustained-mutation-campaign.py` accepts this component. The runner
and reviewer match that exact revision, all 608 reported source files match Git,
the compiled overlay is the exact single production change, and both raw phase
reports regenerate identically. Actual named-test/package actions, intermediate
checkpoints, final cohorts, fault chronology, nanosecond latency samples, and
compilation/unrelated-failure rejection controls verify.

Baseline: 92 batches, 2,576 retained terminal invocations, 28,380 entries,
19 journal-leader kills; measured workload elapsed 618.408624447 seconds.
Mutant: 95 batches, 2,660 retained terminal invocations, 29,343 entries,
19 kills; measured elapsed 622.889936306 seconds. Each executes the full
600-second workload before the controlled same-store challenge. The baseline
rejects its rival lease; private worker lease keys admit a rival at epoch 62,667,
producing the intended mutation escape and semantic test failure.

Totals: 5,236 invocations, 57,723 entries, 38 faults. Worst per-workflow terminal
and progress p99 are 12.734851098 and 7.305135807 seconds.

The 27-member archive retains all downloaded artifact files, completed job log,
campaign/artifact observations, independent verdict and summary. Every member
was reopened and SHA-verified before atomic rename; hashes are in `manifest.json`.
Physical stores and actual executables were not uploaded or independently
reopened. This accepts only the lease category at the recorded source; the other
five categories, full-matrix/200-seed gates and 24-hour soak remain required.
