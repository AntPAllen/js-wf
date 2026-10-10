"""Durable frozen eight-case cancelled-parent/pending-child native race qualification."""
import datetime
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess

base = Path(__file__).resolve().parent
checkout = Path('/home/exedev/js-wf-cancel-pending-child-qualification')
root = Path('/home/exedev/js-wf-cancel-pending-child-20261010')
source = '57ee0b7187252a6a88b24ef655798cdfe3cf4b33'
assert not root.exists()
assert subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=checkout, text=True).strip() == source
assert not subprocess.check_output(['git', 'status', '--porcelain'], cwd=checkout)
names = subprocess.check_output(['git', 'ls-files'], cwd=checkout, text=True).splitlines()
names = [n for n in names if n.endswith(('.go', '.py', '.yml')) or n in ('go.mod', 'go.sum') or n.startswith('sim/testdata/')]

def inventory():
    return {n: hashlib.sha256((checkout / n).read_bytes()).hexdigest() for n in names}

def stamp():
    return datetime.datetime.now(datetime.timezone.utc).isoformat()

root.mkdir()
before = inventory()
(root / 'source-before.json').write_text(json.dumps(dict(source=source, files=before), indent=2) + '\n')
state = dict(source=source, working_directory=str(checkout), root=str(root), started=stamp(), supervisor_pid=os.getpid(), systemd_invocation=os.environ.get('INVOCATION_ID'), phase='compile', commands=[], accepted=False)

def save():
    target = base / 'state.json'
    temporary = target.with_suffix('.json.new')
    temporary.write_text(json.dumps(state, indent=2) + '\n')
    temporary.replace(target)

def run(command, filename):
    env = dict(os.environ, GOMAXPROCS='2', GOMEMLIMIT='512MiB')
    row = dict(command=command, working_directory=str(checkout), started=stamp(), env=dict(GOMAXPROCS='2', GOMEMLIMIT='512MiB'))
    state['commands'].append(row)
    with (root / filename).open('w') as output:
        child = subprocess.Popen(command, cwd=checkout, env=env, stdout=output, stderr=subprocess.STDOUT)
        row['pid'] = child.pid
        save()
        code = child.wait()
    row.update(actual_exit_code=code, finished=stamp())
    save()
    return code

save()
try:
    binary = root / 'worker-race.test'
    code = run(['go', 'test', '-c', '-race', './worker', '-o', str(binary)], 'compile.log')
    assert code == 0
    info = subprocess.check_output(['go', 'version', '-m', str(binary)], text=True)
    assert '-race=true' in info
    (root / 'binary.json').write_text(json.dumps(dict(sha256=hashlib.sha256(binary.read_bytes()).hexdigest(), build_info=info), indent=2) + '\n')
    state['phase'] = 'execute'
    code = run([str(binary), '-test.run=^TestNativeGraphContinuationCancelPendingChild$', '-test.count=1', '-test.timeout=10m', '-test.v=true'], 'race.log')
    state['test_exit'] = code
    if code == 0:
        text = (root / 'race.log').read_text()
        expected = {f'{domain}/archive={archive}/failed={failed}' for domain in ('R1', 'R3Domain') for archive in ('false', 'true') for failed in ('false', 'true')}
        passed = re.findall(r'--- PASS: TestNativeGraphContinuationCancelPendingChild/(\S+) ', text)
        assert len(passed) == 8 and set(passed) == expected and 'WARNING: DATA RACE' not in text
        assert text.count('PENDING_CHILD_CHECKPOINT stage=next locals_count=1 child_calls=0 outcomes=0 buffered_signals=0') == 8
        assert text.count('PENDING_CHILD_CHECKPOINT stage=finish locals_count=2 child_calls=0 outcomes=0 buffered_signals=0') == 8
        assert text.count('PENDING_CHILD_SUSPENDED ') == 8 and text.count('PENDING_PARENT_CANCEL_WAKEUP_RECOVERED reenqueued=1') == 8
        assert text.count('PENDING_PARENT_CANCELLED ') == 8 and text.count('PENDING_PARENT_CANCEL_CHILD_INDEPENDENT ') == 8
        assert text.count('PENDING_PARENT_CANCEL_COLLECTION child_outcome_preserved=true') == 4
        state['complete_eight_cases'] = True
finally:
    after = inventory()
    (root / 'source-after.json').write_text(json.dumps(dict(source=source, files=after), indent=2) + '\n')
    state.update(source_unchanged=after == before, finished=stamp(), phase='terminal')
    save()
    assert after == before
assert state.get('complete_eight_cases'), 'Native matrix failed; preserve actual exits and raw log.'
