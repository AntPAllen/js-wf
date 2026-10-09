from pathlib import Path
import hashlib,json,re,subprocess
base=Path(__file__).resolve().parent
repo=base.parents[2]
root=Path('/home/exedev/js-wf-signal-operation-actors-20261009')
snapshot=json.loads((base/'observed-source.json').read_text())
source='ffce18c'
for name,digest in snapshot['files'].items():
 assert hashlib.sha256((repo/name).read_bytes()).hexdigest()==digest,name
 content=subprocess.check_output(['git','show',f'{source}:{name}'],cwd=repo)
 assert hashlib.sha256(content).hexdigest()==digest,name
for mode in ['normal','race']:
 proof=json.loads((base/f'{mode}-binary.json').read_text())
 assert hashlib.sha256((root/f'{mode}.test').read_bytes()).hexdigest()==proof['sha256']
 assert ('-race=true' in proof['build_info'])==(mode=='race')
records=json.loads((base/'pin-command-results.json').read_text())
for mode in ['normal','race']:
 rows=[r for r in records if r['log'].startswith(f'pin-{mode}-')]
 assert len(rows)==5
 for r in rows:
  assert r['exit_code']==0
  log=(base/r['log']).read_text()
  assert re.search(r'^--- PASS: TestReplayFaultTrace \(',log,re.M)
review=json.loads((base/'directed-trace-review.json').read_text())
for name,r in review.items():
 p=root/'pins'/name
 assert hashlib.sha256(p.read_bytes()).hexdigest()==r['sha256']
 assert len(r['actors'])==5 and r['actor_resumptions']>0 and r['choices_with_multiple_enabled_actors']>0
normal=(root/'normal.log').read_text()
exit_record=json.loads((root/'normal-exit.json').read_text())
assert exit_record['exit_code']==0
assert 'TIER1_SEEDS test=TestSeededGraphSignalOperationActorsReplay first=1 last=1000 completed=1000 requested=1000' in normal
assert re.search(r'^--- PASS: TestSeededGraphSignalOperationActorsReplay \(',normal,re.M)
assert not re.search(r'^--- FAIL:|WARNING: DATA RACE|panic:',normal,re.M)
result={'source_byte_matched_to':source,'observed_source_inputs':len(snapshot['files']),'normal_completed_bodies':1000,'normal_exact_replays':1000,'directed_normal_replays':5,'directed_race_replays':5,'full_family_race':'pending','source_snapshot_limitation':'captured after compilation during live run','scope':'focused new family; complete current suite, all pins, extended and broader requirements not accepted'}
(base/'normal-review.json').write_text(json.dumps(result,indent=2)+'\n')
print(json.dumps(result))
