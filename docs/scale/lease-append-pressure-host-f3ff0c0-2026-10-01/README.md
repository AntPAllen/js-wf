# Hosted fixed-placement pressure pass after an unchanged fixture

Source f3ff0c05dc4659b5f45bf0a7f0ad663d3e14884f,
[workflow 36820757096](https://github.com/AntPAllen/js-wf/actions/runs/36820757096).
The pressure and lease-disk-contract jobs passed; the independent mixed job was
still live when these artifacts were downloaded. All six pressure rows completed.

The final delayed two-owner row issued 48 renewals/appends per owner; KV update
sums were 6.600/3.531 seconds, max renewals 177/107 ms and local gate sums below
one millisecond. The trace records physical delayed writes. This remains a
stable two-node synthetic method-pressure control, excluding the mixed worker,
signal/run pipelines, third-node catch-up and recovery transitions.

Fixture code is unchanged from the earlier failed f976aff hosted job. The next
passing run does not explain its API 503/10008 journal-tail lookup response and
does not reproduce the mixed ~30-second KV sums. Both outcomes are retained;
no production retry, fencing or latency gate was changed for this pass.
