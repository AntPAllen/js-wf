# Hosted all-six sustained-harness smoke accepted

[Run37018302340](https://github.com/AntPAllen/js-wf/actions/runs/37018302340)
is independently accepted at `026d574c45627438d18eee3110b466dca1a85062`.
Every category completes its actual35s original mixed row, admits its controlled
counterexample on the same retained stores with a further verified SIGKILL,
and preserves its original completed cohort. All six intact tests pass and
all six exact production mutants fail with the required semantic evidence.

The independent campaign verifier requires all seven successful jobs, complete
source inventories matching Git, exact production/negative-control overlays,
actual named execution, per-class workload counts and raw-sample p99, original
fault/history/dispatch/latency artifacts, preserved cohort counts and decoded
raw CAS/enqueue/start/purge-generation receipts. Every regenerated phase report
equals the original uploaded report. The campaign report explicitly sets
`shortened_smoke=true`, `clears_sustained_six_mutation_gate=false` and
`clears_full_release=false`.

Each original cohort contains224 invocations, except the start-repair mutant's
196-invocation cohort. These exclude the additional guard cohort. All original
cohorts remain valid after their guard challenge; count/state checks do not prove
byte identity of every prior record.

`originals.tar.gz` losslessly retains135 original artifact/log/metadata/report
files. Every member was read back and SHA256 verified against its original bytes;
see `sha256.json`. `independent-campaign.json` contains the complete regenerated
report and raw proofs.

The actual all-six ten-minute campaign
[37021348438](https://github.com/AntPAllen/js-wf/actions/runs/37021348438)
is launched at `3a9f370e1b7f1de1256f518bcdcefa41b7a9603a`. It remains unaccepted.
The source update adds the campaign verifier/tests and workflow rejection checks;
production Go and the modeled transport graph are unchanged. The full200-seed
matrix and24-hour full-matrix soak remain separate requirements.

Verification command:

```sh
python3 scripts/check-sustained-mutation-campaign.py \
  --metadata /tmp/js-wf-sustained-smoke-ci-37018302340/terminal.json \
  --artifacts /tmp/js-wf-sustained-smoke-ci-37018302340 --duration 35s \
  --output /tmp/js-wf-sustained-smoke-ci-37018302340/independent-campaign.json
```
