import sys,json,shutil,copy,re
from pathlib import Path
sys.dont_write_bytecode=True
repo=Path('/home/exedev/js-wf');out=repo/'docs/scale/retained-cohort-boundary-2026-10-07/qualification';out.mkdir(exist_ok=False)
shutil.copy2(__file__,out/'executed-review.py')
roots={'normal100k':Path('/tmp/js-wf-retained-cohort-tier1-20261008'),'race1000':Path('/tmp/js-wf-retained-cohort-tier1-race-20261008'),'native':Path('/tmp/js-wf-native-cohort-20261008')}
for label,root in roots.items():
 target=out/label;target.mkdir()
 proof=root.with_name(root.name+'-proof')
 for p in proof.iterdir():shutil.copy2(p,target/p.name)
 for p in root.glob('*.json'):shutil.copy2(p,target/p.name)
 for p in root.glob('*.py'):shutil.copy2(p,target/p.name)
 if label=='native':
  names=['integrity-actual.log','integrity-binary.json','integrity-actual-sdk.json','integrity-execution.json']
  for name in names:shutil.copy2(root/name,target/name)
  p=root/'cohort-stores/TestNativeCapturedCohortIncludesAcknowledgedTail/cohort-proof.json';shutil.copy2(p,target/'cohort-proof.json')
 else:
  run=root/label
  for p in run.glob('*.json'):shutil.copy2(p,target/p.name)
  shutil.copy2(run/'actual.log',target/'actual.log')
  log=(run/'actual.log').read_text();seeds=100000 if label=='normal100k' else 1000
  assert f'TIER1_SEEDS test=TestSeededRetainedCohortBoundaryReplay first=1 last={seeds} completed={seeds} requested={seeds}' in log
  assert log.count('--- PASS: TestPinnedRegressionCorpus/retained-cohort-')==9 and 'PASS' in log.splitlines()
  assert not any(s in log for s in ['--- FAIL:','--- SKIP:','DATA RACE'])
def verify(p):
 assert p['replicas']==3 and p['parent_budget_seconds']==180
 assert p['acknowledged_witness']==822 and p['captured_cutoff']==840 and p['weak_metadata_tail']==812
 for name,n,e in [('weak_cut_report',812,3248),('point_report',840,3360),('batched_report',840,3360),('weak_cut_missed_damage_report',812,3248)]:
  assert p[name]=={'Invocations':n,'Journals':n,'Entries':e,'Terminal':n}
 assert p['damaged_tail_report']=={'Invocations':840,'Journals':839,'Entries':3356,'Terminal':839}
 assert 'expected=840' in p['damaged_tail_error'] and p['damaged_tail_subject']=='wf.jrn.audit.cohort-0839'
 assert p['actual_invocation_info']['state']['last_seq']==840
 assert len(p['peers'])==3 and len({x['id'] for x in p['peers']})==3 and all(x['version']=='2.15.0' for x in p['peers'])
p=json.loads((out/'native/cohort-proof.json').read_text());verify(p)
controls=[]
for field in ['replicas','parent_budget_seconds','acknowledged_witness','captured_cutoff','weak_metadata_tail']:
 bad=copy.deepcopy(p);bad[field]+=1
 try:verify(bad)
 except AssertionError:controls.append(field)
 else:raise AssertionError('accepted mutation')
for field in ['weak_cut_report','point_report','batched_report','weak_cut_missed_damage_report','damaged_tail_report']:
 bad=copy.deepcopy(p);bad[field]['Invocations']-=1
 try:verify(bad)
 except AssertionError:controls.append(field)
 else:raise AssertionError('accepted report mutation')
(out/'review.json').write_text(json.dumps({'native_proof_valid':True,'field_substitution_controls_rejected':controls,'focused_normal_schedules':100000,'focused_race_schedules':1000,'pins_replayed_per_profile':9,'scope':'Controlled native R3 stale-metadata adapter and exact shared Tier1 replay. Not an original native partition campaign pass or final-source full125 qualification.'},indent=2)+'\n')
media=out.parent/'seed31-original-media';media.mkdir()
root=Path('/tmp/js-wf-candidate-seed31-invocation-media-20261008')
for name in ['restore-proof.json','media-review.json','executed-restore.py','executed-review.py','readonly-parser.go.txt']:shutil.copy2(root/name,media/name)
print('REVIEWED',len(controls))
