# Twelve sustained journal-fault seeds: 37–48

Source `9ac3ad44267b68d1a1ec63ce47e1fb11d9c00204`, hosted campaign
[37128612905](https://github.com/AntPAllen/js-wf/actions/runs/37128612905),
completed successful job `leader (journal, 37-48)`; public artifact
`11278339784` (`matrix-journal-37-48-10m`). The whole campaign is still active.

Independent review checks the exact job conclusion and artifact/source binding,
all twelve named-test/package completions, full 600-second durations, exact
seed membership, and recorded fault schedules. Each seed records 19 cuts at
30-second scheduled intervals with ordered scheduled/killed/healed timestamps,
valid node identities and no fault errors.

Raw nanosecond latency samples regenerate aggregate and per-workflow terminal
p99, progress p99/max and over-30-second counts. Terminal IDs are unique and
counts agree with the named test's final retained-state audit. All three
production history models independently accept every uploaded Start/Signal/Await
history. Their current sources and dependency declarations are verified identical
to the recorded campaign source before building the review utility. Verification
scripts are loaded from that exact Git revision and retained in the archive.

| Result | Value |
| --- | ---: |
| Independently reviewed seeds | 12 |
| Completed invocations | 31,584 |
| Audited journal entries | 348,032 |
| Recorded leader faults | 228 |
| Worst per-workflow terminal p99 | 18.516417018 s |
| Worst per-workflow progress p99 | 7.528705382 s |

All raw uploaded files—including operation timings and dispatch observations—are
retained unchanged. Review checks histories/latencies/faults and named-test
final integrity/drain assertions; it does not reconstruct physical stores from
operation timing logs. Every archive member is reopened and SHA256 verified
before publication.

No actual test binary, before/after compiled source inventories or physical
stores were uploaded by this workflow. GitHub metadata binds the source revision;
it is not independent compiled-binary provenance. This proves these twelve older
source journal-row seeds. It does not qualify all 200 seeds, the other rows,
latest runtime scanner corrections, or the 24-hour soak.

To rerun the recorded review, extract the archive and run its
`independent-review.py <extracted-root> <repository>` with the repository containing
the recorded revision. The script requires matching history-model source and
module declarations. Metadata is a retained observation of the completed job,
not a claim that the overall campaign finished.
