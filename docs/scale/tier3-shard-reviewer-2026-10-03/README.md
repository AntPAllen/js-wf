# Complete Tier3 shard review

`scripts/check-tier3-matrix-shard.py` reviews a complete requested ten-minute
seed range while its parent campaign is still running or failed. It accepts
only an actual successful terminal job with matching run/source/row/range,
matching artifact identity and exact ordered execution headers. It regenerates
every seed's row report, checkpoint checks, event explanations and fencing
review from raw evidence and compares uploaded bytes. Missing seeds, shortened
runs, failed/raw-substituted cases and modifications during review fail closed.

Clock rows require positive timer cuts and five common-clock probes. Upgrade
rows require forced Start-gap progress and an explicit SIGKILL or Lame Duck
profile. For worker-clock, server-clock and rolling-upgrade rows, the complete
captured before/after Go/Python/workflow/module inventory must equal exact Git
bytes. Batch Git reads avoid hundreds of separate source subprocesses.

Use GitHub REST run/job/artifact JSON and the exact completed job log, plus an
extracted artifact root. For rows with separate original-store archives,
verify their manifest and restore required original executable/source/config
files before review. Metadata or the smaller JSON-only upload cannot substitute
for required raw proof. Retain each archive's own hash/member verification;
this tool hashes the provided extracted inventory, not the remote ZIP.

Example for a complete full-matrix shard:

```sh
python3 scripts/check-tier3-matrix-shard.py \
  --run run.json --job job.json --artifact-metadata artifact.json \
  --job-log job.log --artifact-root extracted-originals \
  --row journal --first 27 --last 39 --output shard-review.json
```

The output must be fresh and outside the original artifact root. Every result
leaves parent-campaign qualification, whole-row qualification and full Tier3
release false. It cannot assemble partial cases from a failed shard, certify
remaining seeds, or turn a failed parent campaign green. Production history,
integrity and physical-drain assertions retain their exact named-test
provenance boundary; there is no independent Porcupine rerun or broker-store
reopen in this supplemental tool. The full-matrix reviewer and mandatory
24-hour acceptance remain separate.

## Calibration and controls

Seven unit controls pass in 0.014 s. Synthetic successful raw-review stubs are
used only to test binding/scope and argument propagation; they are not workload
evidence. Controls reject incorrect job/source/artifact bindings, missing or
duplicate seeds, incomplete raw evidence, raw failures, changed originals,
missing release profiles and source inventories that disagree with Git.

Actual failed job 111303440191 is rejected using retained authoritative API
metadata. Both of its captured 750-file source ledgers exactly match Git
`98a9254`. An actual prior successful case, run 37132399483/job 111235305334 at
`c5125cd`, regenerates all unchanged SIGKILL reports: 1,820 invocations,
20,037 entries and five upgrades. Requesting Lame Duck on those same original
inputs fails and produces no output. This calibrates the reviewer against
previously accepted evidence; it executes no new workload and clears no new gate.

The 15-member [originals archive](originals.tar.gz) retains executed reviewer and
tests, logs, authoritative metadata, source/input hash ledgers and actual
positive/rejected reviews. [Manifest](manifest.json) records every member hash;
all members were reopened and SHA-verified before atomic publication.
Underlying prior raw inputs remain in the unchanged
[accepted Start-gap originals](../r5-start-gap-2026-10-03/hosted-ten-minute/).
