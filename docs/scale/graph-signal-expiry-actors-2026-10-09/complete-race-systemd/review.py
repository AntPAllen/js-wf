"""Independently review expiry output, actual command exit and retained cohort."""
from pathlib import Path
import hashlib
import itertools
import json
import re
import subprocess

base = Path(__file__).resolve().parent
repo = Path('/home/exedev/js-wf')
original = Path('/home/exedev/js-wf-signal-expiry-actors-20261009')
source = subprocess.check_output(['git','rev-parse','e971d01^{commit}'],cwd=repo,text=True).strip()
observed = json.loads((base/'observed-source.json').read_text())
names = list(observed['files'])
objects = subprocess.check_output(['git','cat-file','--batch'],cwd=repo,
    input=''.join(f'{source}:{name}\n' for name in names).encode())
offset = 0
for name in names:
    end = objects.index(b'\n',offset)
    header = objects[offset:end].split()
    assert len(header)==3 and header[1]==b'blob', name
    size = int(header[2]); start = end+1
    assert hashlib.sha256(objects[start:start+size]).hexdigest()==observed['files'][name],name
    offset = start+size+1
assert offset==len(objects)
binary = original/'cartesian-race.test'
build = json.loads((base/'cartesian-race-binary.json').read_text())
assert hashlib.sha256(binary.read_bytes()).hexdigest()==build['sha256']
actual_build = subprocess.check_output(['go','version','-m',str(binary)],text=True)
assert '-race=true' in actual_build and '-race=true' in build['build_info']
command = json.loads((base/'command-result.json').read_text())
assert command['actual_exit_code']==0
assert command['working_directory']==str(repo/'sim')
assert command['command']==[str(binary),'-test.run=^TestSeededGraphSignalExpiryActorsReplay$',
    '-test.count=1','-test.timeout=300m','-test.v']
log = (base/'race.log').read_text()
assert re.findall(r'TIER1_SEEDS test=TestSeededGraphSignalExpiryActorsReplay first=(\d+) last=(\d+) completed=(\d+) requested=(\d+)',log)==[('1','1000','1000','1000')]
assert not re.search(r'^--- FAIL:|WARNING: DATA RACE|panic:',log,re.M)
elapsed = float(re.search(r'^--- PASS: TestSeededGraphSignalExpiryActorsReplay \(([\d.]+)s\)',log,re.M).group(1))
assert re.search(r'^PASS$',log,re.M)
assert re.findall(r'completed=(\d+) requested=1000',log)[:-1]==[str(n) for n in range(100,1001,100)]
counts = dict((key,int(count)) for key,count in re.findall(r'([^ ]+):(\d+)',
    re.search(r'combinations: map\[([^\]]+)\]',log).group(1)))
expected = {'/'.join(parts) for parts in itertools.product(
    ['healthy','source_drop','source_lost_ack','enqueue_drop','enqueue_lost_ack'],
    ['live','expire_intents'],['healthy','drop_before_commit','lose_ack_after_commit'])}
assert set(counts)==expected and sum(counts.values())==1000 and min(counts.values())>0
captures = {}
for path in sorted((original/'pins').glob('*.json')):
    trace = json.loads(path.read_text())
    assert trace['workload']=='graph_signal_expiry_actors'
    clock,root = [trace['decisions'][i]['chosen'] for i in (1,2)]
    assert path.name==clock+'-'+root+'.json'
    cuts = [e for e in trace['transport'] if e['operation']=='graph_publication_intent_expiry_cut']
    assert bool(cuts)==(clock=='expire_intents')
    captures[path.name] = dict(seed=trace['seed'],sha256=hashlib.sha256(path.read_bytes()).hexdigest(),intent_expiry_cuts=len(cuts))
assert len(captures)==6
normal = (original/'normal1000.log').read_text()
assert json.loads((original/'normal-exit.json').read_text())['exit_code']==0
assert 'TIER1_SEEDS test=TestSeededGraphSignalExpiryActorsReplay first=1 last=1000 completed=1000 requested=1000' in normal
result = dict(source_cohort=source,git_matched_observed_inputs=len(names),actual_child_exit=0,
    completed_bodies=1000,exact_replays=1000,elapsed_seconds=elapsed,combinations=counts,
    binary_sha256=build['sha256'],actual_race_instrumentation=True,
    raw_log_sha256=hashlib.sha256((base/'race.log').read_bytes()).hexdigest(),normal_captures=captures,
    source_snapshot_limitation='Observed after launch, not pre-compilation; later five registered runtime traces excluded from original manifest. Current worktree has later changes and is not asserted unchanged. Race working directory was mutable main/sim; the selected compiled family uses in-memory fixtures, not current corpus replay.',
    scope='Focused retained expiry-family normal/race cohort only; no full current/all-pin/extended/native or production acceptance.')
(base/'executed-review.json').write_text(json.dumps(result,indent=2)+'\n')
print(json.dumps(result))
