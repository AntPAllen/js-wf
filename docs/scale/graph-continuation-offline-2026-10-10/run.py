"""Retain frozen source, race binary, CLI/plugin and offline native exports."""
import datetime, hashlib, json, os, subprocess
from pathlib import Path
base = Path(__file__).resolve().parent
checkout = Path('/home/exedev/js-wf-offline-replay-qualification')
root = Path('/home/exedev/js-wf-offline-replay-20261010')
source = subprocess.check_output(['git', 'rev-parse', '94b9750'], cwd=checkout, text=True).strip()
assert subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=checkout, text=True).strip() == source
assert not subprocess.check_output(['git', 'status', '--porcelain'], cwd=checkout)
assert not root.exists()
root.mkdir()
artifacts = root / 'artifacts'
artifacts.mkdir()
names = subprocess.check_output(['git', 'ls-files'], cwd=checkout, text=True).splitlines()
names = [n for n in names if n.endswith(('.go', '.py', '.yml')) or n in ('go.mod', 'go.sum') or n.startswith('sim/testdata/')]
def inventory():
    return {n: hashlib.sha256((checkout / n).read_bytes()).hexdigest() for n in names}
def stamp():
    return datetime.datetime.now(datetime.timezone.utc).isoformat()
state = dict(source=source, checkout=str(checkout), root=str(root), started=stamp(), pid=os.getpid(), invocation=os.environ.get('INVOCATION_ID'), commands=[], accepted=False)
def save():
    (base / 'state.json').write_text(json.dumps(state, indent=2) + '\n')
(root / 'source-before.json').write_text(json.dumps(dict(source=source, files=inventory()), indent=2) + '\n')
env = dict(os.environ, GOMAXPROCS='2', GOMEMLIMIT='512MiB', WF_GRAPH_OFFLINE_ROOT=str(artifacts))
def run(args, filename, cwd):
    row = dict(command=args, cwd=str(cwd), started=stamp())
    state['commands'].append(row)
    save()
    with (root / filename).open('wb') as output:
        row['exit'] = subprocess.call(args, cwd=cwd, env=env, stdout=output, stderr=subprocess.STDOUT)
    row['finished'] = stamp()
    save()
    return row['exit']
code = run(['go', 'test', '-race', '-c', '-o', str(root / 'worker-race.test'), './worker'], 'compile.log', checkout)
if code == 0:
    code = run(['go', 'tool', 'test2json', '-t', '-p', 'js-wf/worker', str(root / 'worker-race.test'), '-test.v=test2json', '-test.run=^TestNativeGraphContinuationOfflineReplay$', '-test.count=1', '-test.timeout=10m'], 'events.jsonl', checkout / 'worker')
(root / 'source-after.json').write_text(json.dumps(dict(source=source, files=inventory()), indent=2) + '\n')
state['artifacts'] = {str(p.relative_to(root)): dict(bytes=p.stat().st_size, sha256=hashlib.sha256(p.read_bytes()).hexdigest()) for p in root.rglob('*') if p.is_file() and (p.name in ('worker-race.test', 'wf', 'handler.so') or p.name.endswith('.json') and p.parent.parent == artifacts)}
state.update(finished=stamp(), exit=code)
save()
raise SystemExit(code)
