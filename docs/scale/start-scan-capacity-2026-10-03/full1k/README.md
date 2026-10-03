# Full1k Start scan capacity graph and retained normal producer

Exact534bcd99e00b288df1368adc6b4fe6bc5f46ae63 passes127.861s:169 top-level
tests,2 documented trace-only skips,271 pinned regressions, and all121 seeded
workloads completing seeds1–1,000. The fixed capacity model executes16 seeds and
has both39s legacy and1.9s configured-policy pins. It adds no scalable workload.

The archive retains the actual normal executable, before/after source hashes,
commands, build settings, raw Go JSON, complete inventories, suite report,
independent reviewer and relocated controls. Every member was reopened and
hashed before the archive was renamed. Independent review verifies995 hashes
against the recorded Git tree, executable SHA/build settings/test listing,
source-derived seeded inventory and exact report regeneration.

Four negative controls reject wrong source/binary hashes, omitted pin and
substituted seed count. The relocated positive passes. This is full1k normal
qualification, distinct from earlier-source100k/race and real-cluster rows.

Extract into a fresh directory, then reproduce (substitute its path):

```sh
python3 review.py /tmp/restored-evidence --repo /home/exedev/js-wf
python3 check-portable-controls.py /tmp/restored-evidence --repo /home/exedev/js-wf --output /tmp/portable-controls.json
```
