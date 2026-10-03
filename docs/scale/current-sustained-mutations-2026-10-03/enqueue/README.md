# Current-source sustained enqueue mutation component

Campaign `37149508529`, job `111281642977`, completed successfully at exact
`c4fed061bc614488d4f89b53b216b756490f7da0`. The original
`sustained-production-enqueue` public artifact `11285208116` is 2,050,375 bytes; its ID and
complete API observation are retained in the archive. The overall campaign is
not accepted because its Start-repair job failed before workload execution.

Independent `check_category` review accepts this component. All 608 reported
source files match the executed Git revision; the production overlay is the
exact single change from that revision's mutation registry. Exact reviewer and
runner bytes used for independent checking are additionally retained. Both raw
phase reports regenerate identically, including actual parent/package actions,
intermediate audits, preserved final cohorts, fault chronology and nanosecond
latency samples. Compilation/unrelated-failure controls are rejected.

Baseline: 90 batches, 2,520 retained terminal invocations, 27,763 entries and
19 journal-leader kills; measured workload elapsed 622.921378744 seconds.
Mutant: 97 batches, 2,716 retained terminal invocations, 29,934 entries and
19 kills; elapsed 632.875790071 seconds. Both execute the full 600-second mixed
workload before the controlled challenge on those same stores. Raw receipts
establish one retained dispatch from 64 acknowledged baseline calls, with its
expected message-ID header. Removing the header retains 64 distinct sequences
for the same invocation, each without a message ID, producing the intended
semantic test failure.

Totals: 5,236 invocations, 57,697 entries, 38 faults. Worst per-workflow terminal
and progress p99 are 14.537925462 and 7.054302844 seconds.

All downloaded files, job log, observations, verifier bytes, independent verdict,
raw receipts and summary are retained in the 29-member archive. Every member
was reopened and SHA-verified before atomic rename; hashes are in `manifest.json`.
Physical stores and actual executables were not uploaded or independently
reopened. Enqueue, CAS, determinism and lease are four accepted components;
Start repair, purge, full matrix/200-seed gates and 24-hour soak remain open.
