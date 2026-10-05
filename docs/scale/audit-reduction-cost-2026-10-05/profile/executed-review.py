from pathlib import Path
import json,hashlib,subprocess,shutil,re,time
repo=Path('/home/exedev/js-wf');root=Path('/tmp/js-wf-audit-reduction-full-population-profile-20261005');out=repo/'docs/scale/audit-reduction-cost-2026-10-05/profile';out.mkdir(parents=True)
def sha(p):
 with p.open('rb') as f:return hashlib.file_digest(f,'sha256').hexdigest()
end=time.monotonic()+8*60
while not (root/'source-after.json').exists():
 assert time.monotonic()<end,'Observer timeout; inspect same profile handle'
 time.sleep(1)
e=json.loads((root/'execution.json').read_text());assert e['status']=='passed' and e['exit_code']==0 and not Path('/proc/'+str(e['pid'])).exists()
b=json.loads((root/'binary.json').read_text());assert sha(root/'integrity.test')==b['sha256']==e['sha256'];assert '-race=true' not in e['build_info'] and 'vcs.modified=false' in e['build_info']
assert '\tdep\tgithub.com/nats-io/nats.go\tv1.54.0' in e['build_info'] and '\tdep\tgithub.com/nats-io/nats-server/v2\tv2.15.0' in e['build_info']
a=json.loads((root/'source-before.json').read_text());assert a==json.loads((root/'source-after.json').read_text()) and a['revision']==e['source']
for name,digest in a['files'].items():
 assert sha(root/'source'/name)==digest
 assert hashlib.sha256(subprocess.check_output(['git','show',e['source']+':'+name],cwd=repo)).hexdigest()==digest
log=(root/'native.log').read_text();passed={name:float(elapsed) for name,elapsed in re.findall(r'^--- PASS: (\S+) \((\d+\.\d+)s\)$',log,re.M)}
expected={'TestAuditReductionFullPopulationCost'}
assert set(passed)==expected and '\nPASS\n' in log and '--- FAIL:' not in log
result=json.loads((root/'originals/TestAuditReductionFullPopulationCost/reduction-cost.json').read_text())
assert [x['Mode'] for x in result['Results']]==['decode_only','reduce_predecoded','decode_and_reduce']
assert all(x['Records']==4800000 for x in result['Results'])
env=json.loads((root/'commands.json').read_text())['env'];assert env['GOMAXPROCS']=='4' and env['GOGC']=='200' and env['GOMEMLIMIT']=='2GiB'
assert all(x['Report']=={'Invocations':0,'Journals':400000,'Entries':4800000,'Terminal':400000} for x in result['Results'][1:])
for label,args in [('cpu-top',['-top']),('cpu-labels',['-tags']),('allocation-top',['-top','-sample_index=alloc_space'])]:
 profile=root/('reduction-allocs.pprof' if label=='allocation-top' else 'reduction-cpu.pprof')
 data=subprocess.check_output(['go','tool','pprof',*args,str(root/'integrity.test'),str(profile)],text=True)
 (root/(label+'.txt')).write_text(data)
 shutil.copyfile(root/(label+'.txt'),out/(label+'.txt'))
r={'execution':e,'explicit_configuration':env,'actual_sdk_sha256':b['sha256'],'selected_source_inputs_verified':len(a['files']),'observed_sdk_closed':True,'native_normal_test_passes_seconds':passed,'reduction_measurements':result,'server_scope':'CPU-only diagnostic; no NATS server launched by selected test','source_capture_scope':'Selected Git Go/module inputs and actual executable metadata, not exhaustive external compiler/toolchain inputs','candidate_adopted':False,'qualifies_400k_capacity':False,'qualifies_24h':False}
shutil.copyfile(root/'originals/TestAuditReductionFullPopulationCost/reduction-cost.json',out/'reduction-cost.json')
(root/'independent-review.json').write_text(json.dumps(r,indent=2)+'\n');(out/'independent-review.json').write_text(json.dumps(r,indent=2)+'\n');shutil.copyfile(__file__,root/'executed-review.py');shutil.copyfile(__file__,out/'executed-review.py')
subprocess.run(['python3','/tmp/js-wf-preserve-read-proof-20261005.py',str(root),str(out)],check=True)
print('NATIVE_CONTROLS_REVIEW_AND_ARCHIVE_COMPLETE',flush=True)
