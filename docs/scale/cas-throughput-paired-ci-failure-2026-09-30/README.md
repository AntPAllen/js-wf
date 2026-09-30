# Paired CAS throughput gate failure

[Actions run 36776741900](https://github.com/AntPAllen/js-wf/actions/runs/36776741900)
failed at candidate `9b71aaf3603ebeffd3780c993ffe9763b38b3483`, compared with the
fixed baseline `4fa311954f42d1325a46dad12bb5d128dad2b747`. All six measurements
completed and passed count/runtime validation. The hot-subject median ratio
was 0.713537, below the unchanged 0.8 gate; parallel ratio was 0.992376.

| Measurement order | Hot appends/s | Parallel appends/s |
| --- | ---: | ---: |
| baseline 1 | 2152.42 | 21449.78 |
| candidate 1 | 2152.75 | 21470.77 |
| candidate 2 | 1530.69 | 21147.43 |
| baseline 2 | 2164.43 | 21313.70 |
| baseline 3 | 1548.53 | 21062.37 |
| candidate 3 | 1535.84 | 21151.21 |

Both revisions show the faster and slower hot-subject rates. Source comparison
shows no changes in `journal/`, `provision/`, `cmd/wf-cas-bench/`, `go.mod`,
`go.sum`, or the benchmark's in-process `testcluster.Start` fixture between
these revisions; process-fixture diagnostics changed separately. This weakens
an explanation based on changed append code, but does not prove a cause or
clear the regression gate. The benchmark does not record the hot stream's
leader placement relative to its pinned client, a concrete diagnostic gap for
these measurements. Retained JSON includes all samples and the failed summary.
