# Hosted fixed-placement pressure failure

Source f976afff2aa8d5e70631c8a9de9e0e429f312e11, job 110231605533 of
[run 36819438269](https://github.com/AntPAllen/js-wf/actions/runs/36819438269).
Downloaded uploaded artifacts while the independent mixed job was still live.

The three no-delay rows and delayed renewal-only row completed. The delayed
one-owner coupled row stopped after 26 calls: production journal append returned
`ErrUnknown` from its tail lookup, with API 503 / error 10008, "JetStream system
temporarily unavailable". The row had accumulated 1.919 seconds in renewal and
1.831 seconds in append. Its maximum renewal was 122.750 ms. No delayed two-owner
row completed. The tracer confirms actual 70 ms delayed writes.

This is a failed hosted pressure gate, not a reproduction of the ~30-second
mixed KV sums. The response locates the failure in a journal metadata/read path;
it does not establish server-side cause. Raw rows, disk trace and final monitoring
are preserved. Do not treat local pressure passes as hosted confirmation or
silently retry this failure into success. The independent lease-disk-contract
job passed on the same workflow.
