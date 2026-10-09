import datetime
import json
import os
from pathlib import Path
import subprocess
import time

base = Path(__file__).resolve().parent
checkout = Path('/home/exedev/js-wf-current151-qualification')
root = Path('/home/exedev/js-wf-tier1-full151-normal1000-20261009')
assert not root.exists()
revision = subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=checkout, text=True).strip()
assert revision == 'e5018af' or revision.startswith('e5018af')
assert not subprocess.check_output(['git', 'status', '--porcelain'], cwd=checkout)
def stamp():
    return datetime.datetime.now(datetime.timezone.utc).isoformat()
def process_identity(pid):
    proc = Path('/proc') / str(pid)
    try:
        fields = (proc / 'stat').read_text().split(') ', 1)[1].split()
        if fields[0] == 'Z':
            return None
        return {'pid': pid, 'start_ticks': fields[19], 'arguments': [item.decode() for item in (proc / 'cmdline').read_bytes().split(b'\0') if item]}
    except FileNotFoundError:
        return None
waits = []
for pid in [3229620, 3229051]:
    process = process_identity(pid)
    if process is not None:
        assert any('js-wf-tier1-full149-race1000-20261009' in arg for arg in process['arguments']), process
        waits.append(process)
command = ['python3', 'scripts/check-tier1-race.py', '--root', str(root), '--seeds', '1000', '--no-race']
record = {'source': revision, 'command': command, 'working_directory': str(checkout), 'root': str(root), 'queued_at': stamp(), 'wait_for_observed_processes': waits, 'scope': 'Full current 151-family/810-pin normal qualification; prior frozen race campaigns are independent.'}
(base / 'normal-launch.json').write_text(json.dumps(record, indent=2) + '\n')
while any(process_identity(item['pid']) == item for item in waits):
    time.sleep(5)
record['started_at'] = stamp()
(base / 'normal-launch.json').write_text(json.dumps(record, indent=2) + '\n')
with (base / 'normal-supervisor.log').open('w') as out:
    result = subprocess.run(command, cwd=checkout, stdout=out, stderr=subprocess.STDOUT)
record['finished_at'] = stamp()
record['exit_code'] = result.returncode
(base / 'normal-launch.json').write_text(json.dumps(record, indent=2) + '\n')
print('normal supervisor actual exit', result.returncode)
