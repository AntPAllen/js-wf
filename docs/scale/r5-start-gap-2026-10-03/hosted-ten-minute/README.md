# Accepted ten-minute forced Start gaps through five-peer upgrades

Both exactc5125cd810d407e26a778ef102f2cb093ff233c6 rows complete all five
retained-store upgrades and actual client-process Start gaps. Each has1820
terminal invocations,55 native fail-closed rejections, six checkpoint audits,
five worker counter cross-checks and no fencing records. All six workflow cells,
Start/Await/Signal histories, final integrity/drain and latency gates pass.

| Profile/run | Verified archive members | Journal entries | Worst type terminal/progress p99 | Maximum gap kill→terminal |
|---|---:|---:|---:|---:|
| SIGKILL/37132399483 |3911|20037|14.495/14.478s|20.751s|
| Graceful/37132401225 |3898|20040|20.163/20.146s|23.604s|

Every original member and723 clean before/after source hashes are checked.
Independent review reproduces row/explanation/fencing reports exactly and
requires each requested shutdown, all five gap proofs and actual Start scan
policy/progress. The copied archive is reopened and every member hashed before
atomic rename. Physical stores remain intact; this review does not independently
reopen them. Temporal failed-scan classifications are descriptive and do not
identify server root causes.

Reproduce from repository root:

```sh
python3 docs/scale/r5-start-gap-2026-10-03/hosted-ten-minute/sigkill/review.py --root docs/scale/r5-start-gap-2026-10-03/hosted-ten-minute/sigkill --shutdown sigkill --run-id 37132399483 --output /tmp/r5-sigkill-review.json
python3 docs/scale/r5-start-gap-2026-10-03/hosted-ten-minute/ldm/review.py --root docs/scale/r5-start-gap-2026-10-03/hosted-ten-minute/ldm --shutdown ldm --run-id 37132401225 --output /tmp/r5-ldm-review.json
```

Accepted scope is seed1/10m atc5125cd for these two profiles.200 seeds,24h and
the newer partial-prefix runtime graph remain open. Neither original rejected
ten-minute run is promoted or altered.
