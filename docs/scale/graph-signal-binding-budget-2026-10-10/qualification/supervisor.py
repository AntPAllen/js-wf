"""Retain actual clean-source compiler/test exits and a race executable."""
import datetime
import gzip
import hashlib
import json
import os
from pathlib import Path
import subprocess

base = Path(__file__).resolve().parent
source = json.loads((base / 'source.json').read_text())
checkout = Path(source['checkout'])
root = Path('/home/exedev/js-wf-signal-binding-budget-artifacts-20261010')
root.mkdir()

def stamp():
    return datetime.datetime.now(datetime.timezone.utc).isoformat()

def inventory():
    assert subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=checkout, text=True).strip() == source['source']
    assert not subprocess.check_output(['git', 'status', '--porcelain'], cwd=checkout)
    names = subprocess.check_output(['git', 'ls-files'], cwd=checkout, text=True).splitlines()
    return {n: hashlib.sha256((checkout / n).read_bytes()).hexdigest() for n in names
            if n.split('/')[0] not in ('docs', 'scripts', '.github') and (n.endswith('.go') or n in ('go.mod', 'go.sum') or '/testdata/' in n)}

before = inventory()
(base / 'source-before.json.gz').write_bytes(gzip.compress(json.dumps(before, sort_keys=True).encode(), mtime=0))
configuration = dict(GOMAXPROCS='2', GOMEMLIMIT='1GiB', GOWORK='off', GOFLAGS='')
env = {k: v for k, v in os.environ.items() if not k.startswith('WF_')}
env.update(configuration)
binary = root / 'client.test'
commands = [
    ['go', 'test', '-race', '-p=1', '-c', '-o', str(binary), './client'],
    [str(binary), '-test.v', '-test.run=^(TestGraphCanonicalSignalBinding(BudgetRecovery|ActualDeadlineRecovery)|TestGraphCanonicalSignalPublicationRecoveryCuts)$', '-test.count=1', '-test.timeout=2m'],
]
state = dict(**source, root=str(root), unit='js-wf-signal-binding-budget-20261010.service',
             invocation=os.environ['INVOCATION_ID'], supervisor_pid=os.getpid(),
             started_utc=stamp(), configuration=configuration, terminal=False, receipts=[])

def save():
    tmp = base / 'state.tmp'
    tmp.write_text(json.dumps(state, indent=2) + '\n')
    tmp.replace(base / 'state.json')

save()
code = 0
for index, command in enumerate(commands):
    with (base / ('compile.log' if index == 0 else 'race.log')).open('xb') as output:
        child = subprocess.Popen(command, cwd=checkout, env=env, stdout=output, stderr=subprocess.STDOUT)
        state.update(child_pid=child.pid, phase='compile' if index == 0 else 'test')
        save()
        code = child.wait()
    state['receipts'].append(dict(command=command, child_pid=child.pid, actual_exit=code, finished_utc=stamp()))
    save()
    if code:
        break
after = inventory()
(base / 'source-after.json.gz').write_bytes(gzip.compress(json.dumps(after, sort_keys=True).encode(), mtime=0))
assert before == after
if binary.exists():
    state['binary'] = dict(path=str(binary), sha256=hashlib.sha256(binary.read_bytes()).hexdigest(),
                           build_info=subprocess.check_output(['go', 'version', '-m', str(binary)], text=True))
state.update(terminal=True, child_exit=code, inputs_unchanged=True, ended_utc=stamp())
save()
raise SystemExit(code)
