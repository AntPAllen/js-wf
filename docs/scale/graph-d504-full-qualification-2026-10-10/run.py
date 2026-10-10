"""One immutable native/default-normal/default-race pipeline with exit receipts."""
import datetime
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess

base = Path(__file__).resolve().parent
checkout = Path('/home/exedev/js-wf-d504a33-qualification')
root = Path('/home/exedev/js-wf-d504-full-qualification-20261010')
source = 'd504a337a8b9102a0b32673d457939c4b82fcc8f'
assert not root.exists()
assert subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=checkout, text=True).strip() == source
assert not subprocess.check_output(['git', 'status', '--porcelain'], cwd=checkout)
names = subprocess.check_output(['git', 'ls-files'], cwd=checkout, text=True).splitlines()
names = [name for name in names if name.endswith(('.go', '.py', '.yml')) or name in ('go.mod', 'go.sum') or name.startswith('sim/testdata/')]
def inventory():
    return {name: hashlib.sha256((checkout / name).read_bytes()).hexdigest() for name in names}
def stamp():
    return datetime.datetime.now(datetime.timezone.utc).isoformat()
root.mkdir()
before = inventory()
(root / 'source-before.json').write_text(json.dumps(dict(source=source, files=before), indent=2) + '\n')
state = dict(source=source, working_directory=str(checkout), root=str(root), started=stamp(), supervisor_pid=os.getpid(), systemd_invocation=os.environ.get('INVOCATION_ID'), phase='native_compile', commands=[], scope='Eight native failed-child cases; complete156-family/841-pin normal1000 and race1000. Race starts only after full normal success. Every broader original requirement remains separate.')
def save():
    target = base / 'pipeline.json'
    temporary = target.with_suffix('.json.new')
    temporary.write_text(json.dumps(state, indent=2) + '\n')
    temporary.replace(target)
def run(command, log, processors):
    env = dict(os.environ, GOMAXPROCS=str(processors), GOMEMLIMIT='512MiB')
    row = dict(command=command, working_directory=str(checkout), started=stamp(), env=dict(GOMAXPROCS=str(processors), GOMEMLIMIT='512MiB'))
    state['commands'].append(row)
    with log.open('w') as output:
        child = subprocess.Popen(command, cwd=checkout, env=env, stdout=output, stderr=subprocess.STDOUT)
        row['pid'] = child.pid
        save()
        result = child.wait()
    row.update(actual_exit_code=result, finished=stamp())
    save()
    return result
save()
try:
    native = root / 'native'
    native.mkdir()
    binary = native / 'worker-race.test'
    code = run(['go', 'test', '-c', '-race', './worker', '-o', str(binary)], native / 'compile.log', 4)
    state['native_compile_exit'] = code
    if code == 0:
        info = subprocess.check_output(['go', 'version', '-m', str(binary)], text=True)
        assert '-race=true' in info
        (native / 'binary.json').write_text(json.dumps(dict(sha256=hashlib.sha256(binary.read_bytes()).hexdigest(), build_info=info), indent=2) + '\n')
        state['phase'] = 'native_execute'
        code = run([str(binary), '-test.run=^TestNativeGraphContinuationFailedChildPromise$', '-test.count=1', '-test.timeout=10m', '-test.v=true'], native / 'race.log', 4)
        state['native_exit'] = code
        if code == 0:
            text = (native / 'race.log').read_text()
            cases = set(re.findall(r'--- PASS: TestNativeGraphContinuationFailedChildPromise/(\S+) ', text))
            expected = {f'{domain}/archive={archive}/buffered={buffered}' for domain in ('R1', 'R3Domain') for archive in ('false', 'true') for buffered in ('false', 'true')}
            assert cases == expected and 'WARNING: DATA RACE' not in text
            assert text.count('child_calls=1 preserved_error=planned_child_failure parent_result=43') == 8
            assert text.count('ARCHIVE_CHILD_RECLAIMED terminal_receipts=1 result_bytes=0') == 4
            state['native_complete_eight_cases'] = True
    state['phase'] = 'normal1000'
    save()
    normal = root / 'normal1000'
    code = run(['python3', 'scripts/check-tier1-race.py', '--root', str(normal), '--seeds', '1000', '--no-race'], root / 'normal-supervisor.log', 2)
    state['normal_exit'] = code
    if code == 0:
        result = json.loads((normal / 'tier1-result.json').read_text())
        assert result['source'] == source and result['pinned_regressions_pass'] == 841
        assert result['per_workload_seed_proof']['workloads'] == 156 and result['per_workload_seed_proof']['completed_bodies'] == 156000
        state['normal_complete'] = True
        state['phase'] = 'race1000'
        save()
        race = root / 'race1000'
        code = run(['python3', 'scripts/check-tier1-race.py', '--root', str(race), '--seeds', '1000'], root / 'race-supervisor.log', 2)
        state['race_exit'] = code
        if code == 0:
            result = json.loads((race / 'tier1-result.json').read_text())
            assert result['source'] == source and result['pinned_regressions_pass'] == 841
            assert result['per_workload_seed_proof']['workloads'] == 156 and result['per_workload_seed_proof']['completed_bodies'] == 156000
            state['race_complete'] = True
    else:
        state['race_not_started_reason'] = 'Full frozen normal actual exit was nonzero.'
finally:
    after = inventory()
    (root / 'source-after.json').write_text(json.dumps(dict(source=source, files=after), indent=2) + '\n')
    state['source_unchanged'] = after == before
    state['finished'] = stamp()
    state['phase'] = 'terminal'
    save()
    assert after == before
assert state.get('native_complete_eight_cases') and state.get('normal_complete') and state.get('race_complete'), state
