# Current-source sustained purge mutation component

Campaign `37149508529`, job `111281643016`, completed successfully at exact
`c4fed061bc614488d4f89b53b216b756490f7da0`. Public artifact `11285318192`
(`sustained-production-purge`, 2,043,105 bytes) contains the original pair.
The overall campaign remains rejected because Start repair failed before its
baseline executed; that category has a separate corrected-runner retry.

Independent `check_category` review accepts this component. All 608 reported
source files match the executed revision. The overlay exactly matches the single
production edit in that revision's mutation registry; actual reviewer/runner
bytes are additionally retained. Both raw phase reports regenerate identically,
including actual parent/package actions, intermediate audits, preserved cohorts,
fault chronology and nanosecond latency samples. Compilation/unrelated-failure
controls are rejected.

Baseline: 97 batches, 2,716 retained terminal invocations, 29,939 entries and
19 journal-leader kills; measured workload elapsed 619.859122256 seconds.
Mutant: 95 batches, 2,660 retained terminal invocations, 29,309 entries and
19 kills; elapsed 620.017556156 seconds. Both execute the full 600-second mixed
workload before the controlled same-store challenge. The baseline resumes purge
and safely reuses the ID: original invocation sequence 2,717, reused sequence
2,745, old journal ending at 30,108, new journal at 30,330–30,333 with fresh
indexes 0–3/result 42. The invocation-first mutant loses the invocation while
leaving journal/state unchanged and the purge marker retained; retry cannot
resume, producing the intended semantic test failure.

Totals: 5,376 invocations, 59,248 entries, 38 faults. Worst per-workflow terminal
and progress p99 are 14.034845404 and 7.055703865 seconds.

The 29-member archive retains all downloaded files, job log, observations,
verifier bytes, independent verdict, raw generation receipts and summary. Every
member was reopened and SHA-verified before atomic rename; hashes are recorded
in `manifest.json`. Physical stores and actual executables were not uploaded
or independently reopened. Five mutation components are accepted; corrected
Start repair, complete six-category qualification, full matrix/200-seed gates
and 24-hour soak remain open.
