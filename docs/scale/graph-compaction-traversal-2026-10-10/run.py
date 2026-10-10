"""Qualify a frozen compaction component; never substitutes for actual-cap gates."""
import datetime
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import sys

base = Path(__file__).resolve().parent
source = sys.argv[1]
checkout = Path('/home/exedev/js-wf-compaction-traversal-qualification')
root = Path('/home/exedev/js-wf-compaction-traversal-qualification-20261010')
assert subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=checkout, text=True).strip() == source
assert not subprocess.check_output(['git', 'status', '--porcelain'], cwd=checkout)
assert not root.exists()
root.mkdir()
def stamp():
    return datetime.datetime.now(datetime.timezone.utc).isoformat()
state = dict(source=source, checkout=str(checkout), root=str(root), pid=os.getpid(),
             invocation=os.environ.get('INVOCATION_ID'), started=stamp(), commands=[], binaries={}, accepted=False)
driver = Path(__file__).read_bytes()
(root/'executed-run.py').write_bytes(driver)
state['driver_sha256'] = hashlib.sha256(driver).hexdigest()
def save():
    (base / 'qualification-state.json').write_text(json.dumps(state, indent=2)+'\n')
names = subprocess.check_output(['git', 'ls-files'], cwd=checkout, text=True).splitlines()
names = [n for n in names if n.endswith(('.go', '.py', '.yml')) or n in ('go.mod', 'go.sum') or n.startswith('sim/testdata/')]
def inventory():
    return dict(source=source, files={n: hashlib.sha256((checkout/n).read_bytes()).hexdigest() for n in names})
(root/'source-before.json').write_text(json.dumps(inventory(), indent=2)+'\n')
for name in ('go-tmp', 'native-tmp'):
    (root/name).mkdir()
env = dict(os.environ, GOMAXPROCS='2', GOMEMLIMIT='512MiB', SIM_SEEDS='1000',
           SIM_COVERAGE_SUMMARY='1', SIM_PROGRESS='1', WF_GRAPH_CONTINUATION_LIMIT_BUDGET='64',
           GOTMPDIR=str(root/'go-tmp'), TMPDIR=str(root/'native-tmp'))
state['environment'] = {k: env[k] for k in ('GOMAXPROCS', 'GOMEMLIMIT', 'SIM_SEEDS', 'SIM_COVERAGE_SUMMARY', 'SIM_PROGRESS', 'WF_GRAPH_CONTINUATION_LIMIT_BUDGET', 'GOTMPDIR', 'TMPDIR')}
def run(args, filename, cwd, seeds='1000'):
    state['phase'] = filename
    row = dict(command=args, cwd=str(cwd), started=stamp(), seeds=seeds)
    state['commands'].append(row)
    save()
    with (root/filename).open('wb') as output:
        row['exit'] = subprocess.call(args, cwd=cwd, env=dict(env, SIM_SEEDS=seeds), stdout=output, stderr=subprocess.STDOUT)
    row['finished'] = stamp()
    save()
    return row['exit']
def compile_binary(name, package, race):
    binary = root/(name+'.test')
    result = run(['go', 'test', '-p=1']+(['-race'] if race else [])+['-c', '-o', str(binary), package], name+'-compile.log', checkout)
    if result == 0:
        state['binaries'][name] = dict(path=str(binary), race=race, sha256=hashlib.sha256(binary.read_bytes()).hexdigest(),
            build_info=subprocess.check_output(['go', 'version', '-m', str(binary)], text=True))
        save()
    return binary, result
def execute(binary, package, selector, filename, seeds='1000'):
    return run(['go', 'tool', 'test2json', '-t', '-p', 'js-wf/'+package, str(binary), '-test.v=test2json',
                '-test.run='+selector, '-test.count=1', '-test.timeout=300m'], filename, checkout/package, seeds)
failed = False
graph, result = compile_binary('graph-race', './internal/graphpublication', True)
failed |= result != 0
if result == 0:
    failed |= execute(graph, 'internal/graphpublication', '^TestGraphPrefixCompaction', 'graph-events.jsonl') != 0
path = checkout/'internal/graphpublication/owned.go'
needle = 'func (p Protocol) verifyOwnedGrant(ctx context.Context, destination string, base Root, payload OwnedPayload) error {'
body = path.read_text()
assert body.count(needle) == 1
mutant = root/'disabled-owned.go'
mutant.write_text(body.replace(needle, needle+'\n return nil // Negative control: bypass original grant.', 1))
overlay = root/'overlay.json'
overlay.write_text(json.dumps({'Replace': {str(path): str(mutant)}}))
negative = run(['go', 'test', '-p=1', '-race', '-overlay='+str(overlay), './internal/graphpublication',
                '-run', '^TestGraphPrefixCompactionRechecksOriginalGrant$', '-count=1', '-v'], 'negative.log', checkout)
text = (root/'negative.log').read_text()
expected = {a+'/'+b for a in ('prepare', 'commit') for b in ('missing', 'destination', 'location')}
leaves = re.findall(r'--- FAIL: TestGraphPrefixCompactionRechecksOriginalGrant/(\S+) ', text)
state['negative_verified'] = negative == 1 and len(leaves) == 6 and set(leaves) == expected and 'build failed' not in text and 'WARNING: DATA RACE' not in text
failed |= not state['negative_verified']
worker, result = compile_binary('worker-race', './worker', True)
failed |= result != 0
if result == 0:
    failed |= execute(worker, 'worker', '^TestNativeGraphContinuationGlobalLimitAndTerminalSlot$/R1/archive=true$', 'native64-events.jsonl') != 0
sim, result = compile_binary('sim-race', './sim', True)
failed |= result != 0
if result == 0:
    failed |= execute(sim, 'sim', '^TestSeededGraphCheckpointCompactionReplay$', 'race1000-events.jsonl') != 0
sim, result = compile_binary('sim-normal', './sim', False)
failed |= result != 0
if result == 0:
    failed |= execute(sim, 'sim', '^TestPinnedRegressionCorpus$', 'pins-events.jsonl') != 0
    failed |= execute(sim, 'sim', '^TestSeededGraphCheckpointCompactionReplay$', 'normal100000-events.jsonl', '100000') != 0
(root/'source-after.json').write_text(json.dumps(inventory(), indent=2)+'\n')
state.update(phase='closed', finished=stamp(), exit=int(failed))
save()
sys.exit(state['exit'])
