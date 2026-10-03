# Corrected consumer seed 90 replay

[Run37125258423](https://github.com/AntPAllen/js-wf/actions/runs/37125258423)
executes exact `c0dc4d707e3c4c9d04dfb189fda0f2da0d3fcb97`, seed90/10m.
The named test passes in633.46s:94 full cohorts,2632 terminal invocations,
29028 journal entries and19 scheduled, positively active consumer-leader cuts.

Independent review verifies terminal metadata and named-test/package completion,
all12408 raw latency samples and fault timestamps, and reproduces aggregate and
six-cell p99/count/max metrics. All3388 SDK operations pass the three production
history models using checker source identical to the campaign revision.
Aggregate terminal p99=7.323693188s; worst type terminal/progress p99=
15.738811529s /7.038449663s; no progress event exceeds30s.

All uploaded originals, metadata and review utilities are archived with every
member SHA256-readback checked before atomic rename. Extract into a temporary
directory and run its review.py with that directory and the repository checkout
as arguments. Passing named-test completion establishes that final retained
integrity and drain assertions ran; physical stores were not uploaded, so this
is not an independent retained-store audit. The original rejected seed90 stays
rejected; this replay does not qualify the default1..200/full matrix.
