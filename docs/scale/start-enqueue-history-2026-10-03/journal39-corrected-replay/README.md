# Corrected journal seed 39 replay

[Run37125258436](https://github.com/AntPAllen/js-wf/actions/runs/37125258436) uses exact
`c0dc4d707e3c4c9d04dfb189fda0f2da0d3fcb97`, seed39/10m. Named test passes in633.49s:
98 cohorts/2744 terminal invocations/30218
entries/19 scheduled faults.

Independent review verifies terminal metadata, all12936 raw
latency samples and fault timing, reproduces all aggregate/six-cell metrics and
checks all3528 SDK operations through the three production
history models with source identical to the campaign. Aggregate terminal p99=
7.455421495s; worst type terminal/progress=
12.062828583s /
7.023239319s. No progress exceeds30s.

All uploaded originals, metadata and review utilities are archived, with every
member SHA256-readback checked before atomic rename. Extract to a temporary
directory and run its review.py with that directory and a repository checkout.
Passing named-test completion establishes that final raw integrity and drain
assertions ran; no physical stores were uploaded, so this is not an independent
retained-store audit. Original rejected shards remain rejected; full1..200 is open.
