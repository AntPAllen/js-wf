# Corrected ahead-clock seed 55 passes the sustained row

[Run 37115560031](https://github.com/AntPAllen/js-wf/actions/runs/37115560031)
is independently accepted at exact `29ad0b2181a4c9101fd0dc6349ed1c48b5a500d9`:
one ten-minute seed with common timer clock and required pending Sleep cuts.
This includes the production typed native-hint recovery and corrected Docker
exit observer. The original rejected seed 55 remains preserved alongside
seed 29; this new result does not establish the original server-side cause.

- 644 terminal invocations and 7,089 journal entries.
- 19 actual journal-leader SIGKILL boundaries, each with an admitted pending
  positive Sleep and source removal before its duration elapsed.
- 195 bounded server-clock observations covering every server at each boundary.
- Worst per-type terminal p99 9.937996908 s; progress p99 16.311279844 s.
- Two completed checkpoint cohort audits, final audit and queue drain.
- Zero fencing records matching all five final in-process worker counters.
- All 3,847 archive members verified against SHA256 manifest; all 702 clean
  before/after source hashes matched exact Git bytes.
- Row report, event explanations and fencing timeline regenerate byte for byte
  from the original archive. Common clock, cut admission, physical peer role
  and controller latency verifiers all pass.

The complete original physical-store archive is retained here. Its misleading
`rolling-` filename is the producer's existing archive convention, shared by
rolling and clock rows. `retained-files.json` records readback hashes of all
retained files. The reviewer streams the archive without extracting stores:

```sh
python3 docs/scale/r5-ahead200-2026-10-03/recovered-seed55-pass/review.py \
  --root docs/scale/r5-ahead200-2026-10-03/recovered-seed55-pass \
  --output /tmp/ahead55-review.json
```

The corrected seeds 1–200 campaign is launched at `63fbc03` as
[37117571555](https://github.com/AntPAllen/js-wf/actions/runs/37117571555), with
the same ten-minute duration, common clock and timer-cut requirements. It is
unaccepted until terminal evidence for the complete consecutive range passes.
This focused replay does not clear the 200-seed gate, full matrices or 24-hour
soak.
