"""Run one complete current159 stage; race requires independently accepted normal."""
import datetime
import json
import os
from pathlib import Path
import subprocess
import sys

base = Path(__file__).resolve().parent
source, mode = sys.argv[1:]
assert mode in ('normal', 'race')
checkout = Path('/home/exedev/js-wf-current159-tier1-qualification-20261010')
root = Path('/home/exedev/js-wf-current159-tier1-20261010')
assert subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=checkout, text=True).strip() == source
assert not subprocess.check_output(['git', 'status', '--porcelain'], cwd=checkout)
assert not (root / (mode+'1000')).exists()
if mode == 'race':
    normal = json.loads((base / 'normal-review.json').read_text())
    assert normal['accepted'] and normal['source'] == source
    normal_state = json.loads((base / 'normal-state.json').read_text())
    props = subprocess.check_output(['systemctl', '--user', 'show', 'js-wf-current159-tier1-normal-20261010-qualified.service',
                                    '-p', 'LoadState', '-p', 'MainPID', '-p', 'ExecMainStatus', '-p', 'InvocationID'], text=True)
    assert 'LoadState=loaded\n' in props and 'MainPID=0\n' in props and 'ExecMainStatus=0\n' in props
    assert 'InvocationID='+normal_state['invocation']+'\n' in props
root.mkdir(exist_ok=True)
command = ['python3', 'scripts/check-tier1-race.py', '--root', str(root / (mode+'1000')),
           '--seeds', '1000', '--shards', '8'] + (['--no-race'] if mode == 'normal' else [])
def stamp():
    return datetime.datetime.now(datetime.timezone.utc).isoformat()
state = dict(source=source, mode=mode, checkout=str(checkout), root=str(root), pid=os.getpid(),
             invocation=os.environ.get('INVOCATION_ID'), started=stamp(), command=command, phase='running')
def save():
    temporary=base / (mode+'-state.tmp')
    temporary.write_text(json.dumps(state, indent=2)+'\n')
    temporary.replace(base / (mode+'-state.json'))
save()
with (root / (mode+'-driver.log')).open('wb') as output:
    child = subprocess.Popen(command, cwd=checkout, stdout=output, stderr=subprocess.STDOUT)
    state['child_pid'] = child.pid
    save()
    state['exit'] = child.wait()
state.update(phase='closed', finished=stamp())
save()
sys.exit(state['exit'])
