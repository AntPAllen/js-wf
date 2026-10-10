import hashlib,json,re,subprocess
from pathlib import Path
base=Path(__file__).resolve().parent
repo=base.parents[2]
source=subprocess.check_output(['git','rev-parse','HEAD'],cwd=repo,text=True).strip()
before=json.loads((base/'corpus-source-before.json').read_text())
assert before==json.loads((base/'corpus-source-after.json').read_text())
process=subprocess.Popen(['git','cat-file','--batch'],cwd=repo,stdin=subprocess.PIPE,stdout=subprocess.PIPE)
try:
 for name,digest in before.items():
  process.stdin.write((source+':'+name+'\n').encode());process.stdin.flush()
  head=process.stdout.readline().split();assert len(head)==3 and head[1]==b'blob',name
  raw=process.stdout.read(int(head[2]));assert process.stdout.read(1)==b'\n'
  assert hashlib.sha256(raw).hexdigest()==digest,name
finally:
 process.stdin.close();assert process.wait()==0
launch=json.loads((base/'corpus-race-command.json').read_text())
assert all(row['actual_exit_code']==0 for row in launch['commands'])
binary=Path(launch['commands'][1]['command'][0])
assert hashlib.sha256(binary.read_bytes()).hexdigest()==launch['binary_sha256']
assert '-race=true' in launch['build_info']
log=(base/'corpus-race.log').read_text()
assert '--- FAIL:' not in log and 'WARNING: DATA RACE' not in log
assert len(re.findall(r'--- PASS: TestPinnedRegressionCorpus/\S+ ',log))==841
assert '--- PASS: TestGraphSignalCallerRetryAfterContention (15.80s)' in log
normal=(base/'migrated-pin-corpus.log').read_text()
assert '--- FAIL:' not in normal and len(re.findall(r'--- PASS: TestPinnedRegressionCorpus/\S+ ',normal))==841
migration=json.loads((base/'trace-migration.json').read_text())
assert len(migration['changes'])==193
changed=set()
for row in migration['changes']:
 name=row['path'];changed.add(name)
 old=subprocess.check_output(['git','show','b7d3a14:'+name],cwd=repo)
 new=subprocess.check_output(['git','show',source+':'+name],cwd=repo)
 assert hashlib.sha256(old).hexdigest()==row['old_sha256']
 assert hashlib.sha256(new).hexdigest()==row['new_sha256']
 a,b=json.loads(old),json.loads(new)
 assert a['seed']==b['seed']==row['seed']
 assert a['version']==b['version'] and a['step_limit']==b['step_limit']
 assert a.get('disabled_actors')==b.get('disabled_actors')
 assert (a['decisions']==b['decisions'])==row['all_decisions_unchanged']
 assert a['decisions'][0]==b['decisions'][0]
 if name.endswith('graph-signal-actors-caller-retry.json'):
  assert b['workload']=='graph_signal_caller_retry'
  assert sum(e['operation']=='graph_caller_admission_conflict' for e in b['transport'])==2041
  assert any(e['operation']=='signal_caller_retry_recovered' and e.get('outcome')=='caller_resubmitted' and e.get('subject')=='second' for e in b['transport'])
assert sum(row['all_decisions_unchanged'] for row in migration['changes'])==187
pins={name for name in before if name.startswith('sim/testdata/regressions/')}
assert len(pins)==841
for name in pins-changed:
 assert subprocess.check_output(['git','show','b7d3a14:'+name],cwd=repo)==(repo/name).read_bytes(),name
assert len(pins-changed)==648
baseline_fail=(base/'original-actor-pins.log').read_text()
baseline_pass=(base/'accepted155-original-actor-pins.log').read_text()
assert len(re.findall(r'--- FAIL: TestPinnedRegressionCorpus/\S+ ',baseline_fail))==6
assert len(re.findall(r'--- PASS: TestPinnedRegressionCorpus/\S+ ',baseline_pass))==6
for root,sha in [('/home/exedev/js-wf-tier1-full156-normal1000-20261009','3b4cf8df6d62b20c7bb0326f5120ba93dab798a8d72a55678fcca5baa4689408'),('/home/exedev/js-wf-tier1-full155-normal1000-20261009','b273eab1af052400de5eb3c51e47cb0f537fc93e8cd61f3ea0fa4c8c4f1ea8a4')]:
 assert hashlib.sha256((Path(root)/'sim.test').read_bytes()).hexdigest()==sha
negative=json.loads((base/'caller-resubmit-negative-result.json').read_text())
assert negative['actual_exit_code']==1
assert 'completion effects=1:' in (base/'caller-resubmit-negative.log').read_text()
assert hashlib.sha256((base/'caller-resubmit-negative-trace.json').read_bytes()).hexdigest()==negative['sha256']
result=dict(verdict='PASS current saved corpus normal/race and explicit caller-retry recovery control',source=source,source_inputs_git_verified_unchanged=len(before),normal_pins=841,race_pins=841,race_corpus_seconds=122.61,caller_retry_race_seconds=15.80,binary_sha256=launch['binary_sha256'],unchanged_pins=648,changed_transport_traces_preserving_every_decision=187,explicitly_changed_actor_schedules=6,caller_admission_rejections=2041,required_negative_control=True,scope='Saved corpus plus directed caller retry only. No complete current seeded/default/extended, native failed-child matrix, fault/scale/release, public continuation or production collection acceptance.')
(base/'corpus-review.json').write_text(json.dumps(result,indent=2)+'\n')
print(json.dumps(result))
