"""Run retained corrected actor race, then expiry race, under one user service."""
import datetime
import hashlib
import json
import os
from pathlib import Path
import subprocess

repo = Path('/home/exedev/js-wf')
base = Path(__file__).resolve().parent
root = Path('/home/exedev/js-wf-focused-race-systemd-20261009')
assert not root.exists()
root.mkdir()
env = dict(os.environ, GOMAXPROCS='2', GOMEMLIMIT='512MiB', SIM_SEEDS='1000',
           SIM_PROGRESS='1', SIM_COVERAGE_SUMMARY='1', FAULT_TRACE_OUT=str(root/'failure.json'))
results = []

def call(command, cwd, name):
    with (root/name).open('w') as out:
        child = subprocess.run(command, cwd=cwd, env=env, stdout=out, stderr=subprocess.STDOUT)
    results.append(dict(command=command, working_directory=str(cwd), log=name,
                        actual_exit_code=child.returncode, finished_at=datetime.datetime.now(datetime.timezone.utc).isoformat()))
    (root/'command-results.json').write_text(json.dumps(results, indent=2)+'\n')
    child.check_returncode()

def verify_inputs(checkout, manifest):
    for name, digest in manifest['files'].items():
        assert hashlib.sha256((checkout/name).read_bytes()).hexdigest() == digest, name

actor = Path('/home/exedev/js-wf-signal-actors-qualified-20261009')
checkout = Path('/home/exedev/js-wf-signal-actors-qualification')
before = json.loads((actor/'source-before.json').read_text())
assert subprocess.check_output(['git','rev-parse','HEAD'],cwd=checkout,text=True).strip() == before['revision']
assert not subprocess.check_output(['git','status','--porcelain'],cwd=checkout)
verify_inputs(checkout,before)
binary = actor/'race.test'
build = json.loads((actor/'race-binary.json').read_text())
assert '-race=true' in build['build_info']
assert hashlib.sha256(binary.read_bytes()).hexdigest() == build['sha256']
env['SIM_GRAPH_SIGNAL_ACTORS_ROOT'] = str(root/'pins')
(root/'pins').mkdir()
call(['go','tool','test2json','-t','-p','js-wf/sim',str(binary),
      '-test.run=^(TestSeededGraphSignalOperationActorsReplay|TestGraphSignalCallerRetryAfterContention)$',
      '-test.count=1','-test.timeout=90m','-test.v=test2json'],checkout/'sim','actor-events.jsonl')
events = [json.loads(line) for line in (root/'actor-events.jsonl').read_text().splitlines()]
passes = {e.get('Test') for e in events if e['Action']=='pass'}
assert {'TestSeededGraphSignalOperationActorsReplay','TestGraphSignalCallerRetryAfterContention'} <= passes
assert not any(e['Action']=='fail' for e in events)
assert 'TIER1_SEEDS test=TestSeededGraphSignalOperationActorsReplay first=1 last=1000 completed=1000 requested=1000' in ''.join(e.get('Output','') for e in events)
call([str(binary),'-test.run=^TestPinnedRegressionCorpus$/^graph-signal-actors-caller-retry[.]json$',
      '-test.count=1','-test.timeout=5m','-test.v'],checkout/'sim','actor-pin.log')
verify_inputs(checkout,before)
(root/'actor-qualified.json').write_text(json.dumps(dict(source=before['revision'],completed_bodies=1000,binary_sha256=build['sha256'],scope='focused family, directed control and caller retry pin only'),indent=2)+'\n')

expiry = Path('/home/exedev/js-wf-signal-expiry-actors-20261009')
binary = expiry/'cartesian-race.test'
build = json.loads((expiry/'cartesian-race-binary.json').read_text())
assert '-race=true' in build['build_info']
assert hashlib.sha256(binary.read_bytes()).hexdigest() == build['sha256']
observed = json.loads((expiry/'observed-source.json').read_text())
for name,digest in observed['files'].items():
    assert hashlib.sha256(subprocess.check_output(['git','show',f'e971d01:{name}'],cwd=repo)).hexdigest()==digest,name
assert json.loads((expiry/'normal-exit.json').read_text())['exit_code']==0
assert 'TIER1_SEEDS test=TestSeededGraphSignalExpiryActorsReplay first=1 last=1000 completed=1000 requested=1000' in (expiry/'normal1000.log').read_text()
env.pop('SIM_GRAPH_SIGNAL_ACTORS_ROOT')
call([str(binary),'-test.run=^TestSeededGraphSignalExpiryActorsReplay$',
      '-test.count=1','-test.timeout=300m','-test.v'],repo/'sim','expiry-race.log')
assert 'TIER1_SEEDS test=TestSeededGraphSignalExpiryActorsReplay first=1 last=1000 completed=1000 requested=1000' in (root/'expiry-race.log').read_text()
(root/'expiry-qualified.json').write_text(json.dumps(dict(source='e971d01',completed_bodies=1000,binary_sha256=build['sha256'],scope='focused expiry family only; retained binary post-launch source observation limitation'),indent=2)+'\n')
