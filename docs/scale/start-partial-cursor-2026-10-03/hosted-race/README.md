# Hosted complete race suite after the Start prefix fix

[Run 37139806593](https://github.com/AntPAllen/js-wf/actions/runs/37139806593)
is terminal SUCCESS at exact `0d3fabf33615b3bcb7e092c872f5eb7d40520369`.
Its `sim` job succeeds; public artifact 11280966420 is unexpired and named
`tier1-race-evidence`.

Independent review accepts 2,134.761 s package time, 170 top-level passes,
two documented trace-only skips, all 283 pins and all 121 scalable workloads
completing seeds 1–1000 (121,000 bodies). Aggregate fixed/repeated counts are
123,183 schedules, 1,798,545 choices and 27,196,095 transport events.

All 1,018 clean before/after source hashes match exact Git. The retained actual
executable is race-instrumented, with SHA256
`db3185b2f99fe649b14d81cf15c122a4ae1c24c837b2d045ec03b7763953a281`.
Its compiled inventory, source-derived seeded-loop inventory and pin inventory
verify; raw Go JSON regenerates the report byte-identically. Every one of the
19 archived members was reopened and SHA256 verified before atomic publication.
Downloaded executable permission was restored for inventory inspection; bytes
were not changed. Original artifact members, terminal/artifact metadata and
independent reviewer/report are retained together.

```sh
tar -xzf originals.tar.gz -C /path/to/empty-directory
python3 /path/to/empty-directory/review.py /path/to/empty-directory --repo /home/exedev/js-wf
```

This qualifies the Start prefix runtime graph at `0d3fabf`. Later signal,
suspended, native timer and fallback timer corrections require their own complete
race/extended qualification. It does not qualify the latest runtime or real
fault matrices/24h soak.
