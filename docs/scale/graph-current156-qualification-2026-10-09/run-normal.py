"""Run full frozen156 normal qualification once, retaining actual child status."""
import datetime
import hashlib
import json
from pathlib import Path
import subprocess

base = Path(__file__).resolve().parent
checkout = Path('/home/exedev/js-wf-current156-qualification')
root = Path('/home/exedev/js-wf-tier1-full156-normal1000-20261009')
revision = 'fd95754'
assert not root.exists()
assert subprocess.check_output(['git', 'rev-parse', '--short=7', 'HEAD'], cwd=checkout, text=True).strip() == revision
assert not subprocess.check_output(['git', 'status', '--porcelain'], cwd=checkout)
command = ['python3', 'scripts/check-tier1-race.py', '--root', str(root), '--seeds', '1000', '--no-race']
record = dict(started=datetime.datetime.now(datetime.timezone.utc).isoformat(), command=command,
              working_directory=str(checkout), source=subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=checkout, text=True).strip(),
              supervisor_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
              scope='Complete frozen156 normal Tier-1 source, all841 pins and directed groups; not full race/extended/native acceptance.')
def save():
    (base / 'normal-launch.json').write_text(json.dumps(record, indent=2) + '\n')
save()
with (base / 'supervisor.log').open('w') as output:
    child = subprocess.run(command, cwd=checkout, stdout=output, stderr=subprocess.STDOUT)
record.update(actual_exit_code=child.returncode, finished=datetime.datetime.now(datetime.timezone.utc).isoformat())
save()
child.check_returncode()
result = json.loads((root / 'tier1-result.json').read_text())
assert result['pinned_regressions_pass'] == 841
assert result['per_workload_seed_proof']['workloads'] == 156
assert result['per_workload_seed_proof']['completed_bodies'] == 156000
record['runner_verified_complete_normal'] = True
save()
