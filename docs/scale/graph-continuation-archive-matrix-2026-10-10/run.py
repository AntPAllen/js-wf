"""Run all existing archive cases using an already qualified frozen race binary."""
import datetime
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess

base = Path(__file__).resolve().parent
checkout = Path('/home/exedev/js-wf-cancel-pending-child-qualification')
root = Path('/home/exedev/js-wf-archive-matrix-20261010')
prior = Path('/home/exedev/js-wf-cancel-pending-child-20261010')
source = '57ee0b7187252a6a88b24ef655798cdfe3cf4b33'
binary = prior / 'worker-race.test'
assert not root.exists()
assert subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=checkout, text=True).strip() == source
assert not subprocess.check_output(['git', 'status', '--porcelain'], cwd=checkout)
provenance = json.loads((prior / 'binary.json').read_text())
assert hashlib.sha256(binary.read_bytes()).hexdigest() == provenance['sha256']
assert '-race=true' in provenance['build_info']
names = subprocess.check_output(['git', 'ls-files'], cwd=checkout, text=True).splitlines()
names = [n for n in names if n.endswith(('.go', '.py', '.yml')) or n in ('go.mod', 'go.sum') or n.startswith('sim/testdata/')]

def inventory():
    return {n: hashlib.sha256((checkout / n).read_bytes()).hexdigest() for n in names}

def stamp():
    return datetime.datetime.now(datetime.timezone.utc).isoformat()

before = dict(source=source, files=inventory())
assert before == json.loads((prior / 'source-before.json').read_text()) == json.loads((prior / 'source-after.json').read_text())
root.mkdir()
(root / 'source-before.json').write_text(json.dumps(before, indent=2) + '\n')
(root / 'binary.json').write_text(json.dumps(provenance, indent=2) + '\n')
state = dict(source=source, working_directory=str(checkout), root=str(root), binary=str(binary), binary_compile_receipts=str(prior), started=stamp(), supervisor_pid=os.getpid(), systemd_invocation=os.environ.get('INVOCATION_ID'), phase='execute', accepted=False)

def save():
    target = base / 'state.json'
    temporary = target.with_suffix('.json.new')
    temporary.write_text(json.dumps(state, indent=2) + '\n')
    temporary.replace(target)

command = [str(binary), '-test.run=^TestNativeGraphContinuationArchiveCollection$', '-test.count=1', '-test.timeout=10m', '-test.v=true']
env = dict(os.environ, GOMAXPROCS='2', GOMEMLIMIT='512MiB')
state['command'] = dict(command=command, working_directory=str(checkout), started=stamp(), env=dict(GOMAXPROCS='2', GOMEMLIMIT='512MiB'))
save()
try:
    with (root / 'race.log').open('w') as output:
        child = subprocess.Popen(command, cwd=checkout, env=env, stdout=output, stderr=subprocess.STDOUT)
        state['command']['pid'] = child.pid
        save()
        code = child.wait()
    state['command'].update(actual_exit_code=code, finished=stamp())
    if code == 0:
        text = (root / 'race.log').read_text()
        expected = {f'{domain}/{mode}' for domain in ('R1', 'R3Domain') for mode in ('state', 'signals', 'child', 'buffered-child')}
        passed = re.findall(r'--- PASS: TestNativeGraphContinuationArchiveCollection/(\S+) ', text)
        assert len(passed) == 8 and set(passed) == expected and 'WARNING: DATA RACE' not in text
        assert text.count('ARCHIVE_COLLECTION stage=next ') == 8 and text.count('ARCHIVE_COLLECTION stage=finish ') == 8
        assert text.count('ARCHIVE_CHILD_RECLAIMED terminal_receipts=2 result_bytes=700000') == 4
        assert text.count('SDK initial/next/finish=map[finish:1 initial:1 next:1] effects=2 ') == 8
        state['complete_eight_cases'] = True
finally:
    after = dict(source=source, files=inventory())
    (root / 'source-after.json').write_text(json.dumps(after, indent=2) + '\n')
    state.update(source_unchanged=after == before, binary_unchanged=hashlib.sha256(binary.read_bytes()).hexdigest() == provenance['sha256'], finished=stamp(), phase='terminal')
    save()
    assert state['source_unchanged'] and state['binary_unchanged']
assert state.get('complete_eight_cases'), 'Existing native archive matrix failed; preserve actual exit and raw log.'
