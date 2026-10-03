from pathlib import Path
import argparse,hashlib,json,subprocess
p=argparse.ArgumentParser();p.add_argument('--root',type=Path,required=True);p.add_argument('--output',type=Path,required=True);a=p.parse_args();r=a.root
rev='0b69e915'
source=json.loads((r/'source-before.json').read_text());rev=source['revision']
assert rev.startswith('0b69e91') and source['files']==json.loads((r/'source-after.json').read_text())
for name,digest in source['files'].items():assert hashlib.sha256(subprocess.check_output(['git','show',rev+':'+name])).hexdigest()==digest,name
original=subprocess.check_output(['git','show',rev+':worker/worker.go']).decode();needle='ScheduleIsHint: w.nativeSchedules';assert original.count(needle)==1
assert (r/'required-hint-control.go.txt').read_text()==original.replace(needle,'ScheduleIsHint: false')
model='TestWorkerTimerHintFailureRecovery';sdk='TestDomainNativeHintFailureUsesDurableRepair'
for mode in ['positive','negative']:
 es=[json.loads(l) for l in (r/(mode+'-events.jsonl')).read_text().splitlines()]
 assert not any(e['Action'] in ['skip','build-fail'] for e in es)
 text=''.join(e.get('Output','') for e in es);assert 'panic: test timed out' not in text
 def actions(test):return [e['Action'] for e in es if e.get('Test')==test and e['Action'] in ['pass','fail']]
 package={e['Package']:e['Action'] for e in es if not e.get('Test') and e['Action'] in ['pass','fail']}
 if mode=='positive':
  assert package=={'js-wf/wf':'pass','js-wf/sim':'pass','js-wf/integration':'pass'}
  for test in [model,sdk,'TestWorkerTimerHintOwnershipFailureNotSuppressed','TestPinnedRegressionCorpus','TestNativeTimerHintFailureRepairsFromDurableSuspension']:assert actions(test)==['pass']
  assert sum(e['Action']=='pass' and e.get('Test','').startswith(sdk+'/') for e in es)==20
  assert sum(e['Action']=='pass' and e.get('Test','').startswith('TestPinnedRegressionCorpus/') for e in es)==267
  for case in ['false','true']:
   assert actions(model+'/unapplied_publish_'+case)==['pass']
   trace=json.loads((r/'traces'/('timer-error-unapplied-'+case+'.json')).read_text())
   checks=[e for e in trace['transport'] if e['operation']=='check_timer_hint_recovery']
   assert len(checks)==1 and checks[0]['at_ms']==2000
   assert checks[0]['outcome']=='unapplied_publish='+case
  assert text.count('exact_replay=true production_timer=true')==2
 else:
  assert package=={'js-wf/sim':'fail'} and actions(model)==['fail'] and actions(model+'/unapplied_publish_false')==['fail']
  assert 'missing durable timer suspension:' in text
report=dict(revision=rev,source_files_verified=len(source['files']),accepted=True,actual_sdk_cases=20,
 actual_recovery_cases=2,ownership_failure_not_suppressed=True,pinned_regressions=267,exact_replay=True,
 actual_three_node_injected_comparison=True,compiled_missing_suspension_control=True,
 full_seed_gate=False,original_server_cause_confirmed=False,physical_all_five_replica_drain=False)
a.output.write_text(json.dumps(report,indent=2)+'\n');print(json.dumps(report))
