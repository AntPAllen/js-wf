"""Independently inspect actual focused race outputs and frozen provenance."""
from pathlib import Path
import json,hashlib,subprocess,re

root=Path('/home/exedev/js-wf-focused-race-systemd-20261009')
original=Path('/home/exedev/js-wf-signal-actors-qualified-20261009')
checkout=Path('/home/exedev/js-wf-signal-actors-qualification')
source=json.loads((original/'source-before.json').read_text())
assert source['revision']=='cc8363da0f99dafbaff6eec454b85cc00fa62a63'
assert subprocess.check_output(['git','rev-parse','HEAD'],cwd=checkout,text=True).strip()==source['revision']
assert not subprocess.check_output(['git','status','--porcelain'],cwd=checkout)
names=list(source['files'])
for name in names:assert hashlib.sha256((checkout/name).read_bytes()).hexdigest()==source['files'][name],name
objects=subprocess.check_output(['git','cat-file','--batch'],cwd=checkout,input=''.join(f'{source["revision"]}:{n}\n' for n in names).encode())
offset=0
for name in names:
    end=objects.index(b'\n',offset);size=int(objects[offset:end].split()[-1]);start=end+1
    assert hashlib.sha256(objects[start:start+size]).hexdigest()==source['files'][name],name
    offset=start+size+1
assert offset==len(objects)
events=[json.loads(line) for line in (root/'actor-events.jsonl').read_text().splitlines()]
passes={e.get('Test'):e.get('Elapsed') for e in events if e['Action']=='pass'}
assert {'TestSeededGraphSignalOperationActorsReplay','TestGraphSignalCallerRetryAfterContention',None}<=passes.keys()
assert not any(e['Action']=='fail' for e in events)
output=''.join(e.get('Output','') for e in events)
assert re.findall(r'TIER1_SEEDS test=TestSeededGraphSignalOperationActorsReplay first=(\d+) last=(\d+) completed=(\d+) requested=(\d+)',output)==[('1','1000','1000','1000')]
results=json.loads((root/'command-results.json').read_text())
for name,record in zip(['actor-events.jsonl','actor-pin.log'],results[:2]):assert record['log']==name and record['actual_exit_code']==0 and record['working_directory']==str(checkout/'sim')
assert len(results)>=2
assert '--- PASS: TestPinnedRegressionCorpus/graph-signal-actors-caller-retry.json' in (root/'actor-pin.log').read_text()
binary=original/'race.test';build=json.loads((original/'race-binary.json').read_text())
assert hashlib.sha256(binary.read_bytes()).hexdigest()==build['sha256']
assert '-race=true' in subprocess.check_output(['go','version','-m',str(binary)],text=True)
print(json.dumps({'source':source['revision'],'git_matched_unchanged_inputs':len(names),'actual_command_exits':[r['actual_exit_code'] for r in results[:2]],'completed_bodies':1000,'package_elapsed':passes[None],'binary_sha256':build['sha256']},indent=2))
