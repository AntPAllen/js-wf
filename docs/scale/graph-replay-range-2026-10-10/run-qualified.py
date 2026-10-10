"""Qualify frozen export source with retained race/native and normal corpus binaries."""
import datetime
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess

base = Path(__file__).resolve().parent
checkout = Path('/home/exedev/js-wf-replay-range-qualification')
root = Path('/home/exedev/js-wf-replay-range-native-20261010')
source = 'b3b9ce5894c6dacd1e197729a3c01eb5dee06422'
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
before = dict(source=source, files=inventory())
(root / 'source-before.json').write_text(json.dumps(before, indent=2) + '\n')
state = dict(source=source, working_directory=str(checkout), root=str(root), started=stamp(), supervisor_pid=os.getpid(), systemd_invocation=os.environ.get('INVOCATION_ID'), phase='worker_compile', commands=[], binaries={}, accepted=False)

def save():
    target = base / 'qualified-state.json'
    temporary = target.with_suffix('.json.new')
    temporary.write_text(json.dumps(state, indent=2) + '\n')
    temporary.replace(target)

def run(command, filename, cwd=checkout):
    env = dict(os.environ, GOMAXPROCS='2', GOMEMLIMIT='512MiB')
    row = dict(command=command, working_directory=str(cwd), started=stamp(), env=dict(GOMAXPROCS='2', GOMEMLIMIT='512MiB'))
    state['commands'].append(row)
    with (root / filename).open('w') as output:
        child = subprocess.Popen(command, cwd=cwd, env=env, stdout=output, stderr=subprocess.STDOUT)
        row['pid'] = child.pid
        save()
        code = child.wait()
    row.update(actual_exit_code=code, finished=stamp())
    save()
    return code

def compile_binary(package, name, race):
    binary = root / name
    assert run(['go', 'test', '-c', *(['-race'] if race else []), package, '-o', str(binary)], name + '-compile.log') == 0
    info = subprocess.check_output(['go', 'version', '-m', str(binary)], text=True)
    assert ('-race=true' in info) == race
    state['binaries'][name] = dict(sha256=hashlib.sha256(binary.read_bytes()).hexdigest(), build_info=info, race_instrumented=race)
    save()
    return binary

save()
try:
    binary = compile_binary('./worker', 'worker-race.test', True)
    state['phase'] = 'worker_controls'
    assert run([str(binary), '-test.run=^TestGraphReplaySnapshot', '-test.count=1', '-test.timeout=2m', '-test.v=true'], 'worker-race.log') == 0
    text = (root / 'worker-race.log').read_text()
    assert 'range_gets=108 point_gets=237 elapsed=24.6s pin_ttl=4s pin_writes=11' in text
    assert 'WARNING: DATA RACE' not in text and '--- FAIL:' not in text
    state['worker_complete'] = True
    state['phase'] = 'cli_compile'
    binary = compile_binary('./cmd/wf', 'cli-race.test', True)
    state['phase'] = 'native_cursor_export'
    assert run([str(binary), '-test.run=^TestNativeGraphCursorOperatorHistory$', '-test.count=1', '-test.timeout=10m', '-test.v=true'], 'native-cursor-race.log') == 0
    text = (root / 'native-cursor-race.log').read_text()
    expected = {f'v{version}/R{replicas}-domain' for version in (4, 5, 6) for replicas in (1, 3)}
    passed = re.findall(r'--- PASS: TestNativeGraphCursorOperatorHistory/(\S+) ', text)
    assert len(passed) == 6 and set(passed) == expected and 'WARNING: DATA RACE' not in text
    assert text.count('CURSOR_REPLAY_EXPORT ') == 6
    state['native_complete'] = True
    state['phase'] = 'corpus_compile'
    binary = compile_binary('./sim', 'sim-normal.test', False)
    state['phase'] = 'corpus_normal'
    assert run([str(binary), '-test.run=^TestPinnedRegressionCorpus$', '-test.count=1', '-test.timeout=5m', '-test.v=true'], 'corpus-normal.log', checkout / 'sim') == 0
    text = (root / 'corpus-normal.log').read_text()
    passed = re.findall(r'--- PASS: TestPinnedRegressionCorpus/(\S+) ', text)
    assert len(passed) == 841 and len(set(passed)) == 841 and '--- FAIL:' not in text
    state['corpus_complete'] = True
finally:
    after = dict(source=source, files=inventory())
    (root / 'source-after.json').write_text(json.dumps(after, indent=2) + '\n')
    state.update(source_unchanged=after == before, finished=stamp(), phase='terminal')
    save()
    assert after == before
assert state.get('worker_complete') and state.get('native_complete') and state.get('corpus_complete')
