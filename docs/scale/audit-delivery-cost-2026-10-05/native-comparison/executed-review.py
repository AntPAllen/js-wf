from pathlib import Path
import json,hashlib,subprocess,shutil,re
repo=Path('/home/exedev/js-wf');root=Path('/tmp/js-wf-audit-delivery-cost-native-20261005');out=repo/'docs/scale/audit-delivery-cost-2026-10-05/native-comparison';out.mkdir(parents=True)
def sha(p):
 with p.open('rb') as f:return hashlib.file_digest(f,'sha256').hexdigest()
e=json.loads((root/'execution.json').read_text());assert e['status']=='passed' and e['exit_code']==0 and not Path('/proc/'+str(e['pid'])).exists()
b=json.loads((root/'binary.json').read_text());assert sha(root/'integrity.test')==b['sha256']==e['sha256'];assert '-race=true' not in e['build_info'] and 'vcs.modified=false' in e['build_info']
assert '\tdep\tgithub.com/nats-io/nats.go\tv1.54.0' in e['build_info'] and '\tdep\tgithub.com/nats-io/nats-server/v2\tv2.15.0' in e['build_info']
a=json.loads((root/'source-before.json').read_text());assert a==json.loads((root/'source-after.json').read_text()) and a['revision']==e['source']
for name,digest in a['files'].items():
 assert sha(root/'source'/name)==digest
 assert hashlib.sha256(subprocess.check_output(['git','show',e['source']+':'+name],cwd=repo)).hexdigest()==digest
log=(root/'native.log').read_text();passed={name:float(elapsed) for name,elapsed in re.findall(r'^--- PASS: (\S+) \((\d+\.\d+)s\)$',log,re.M)}
expected={'TestAuditDeliveryNativeCostComparison'}
assert set(passed)==expected and '\nPASS\n' in log and '--- FAIL:' not in log
result=json.loads((root/'originals/TestAuditDeliveryNativeCostComparison/delivery-cost.json').read_text())
assert [x['Mode'] for x in result['Results']]==['next','adapter','consume','next_recheck']
assert all(x['Records']==100000 and x['ElapsedNS']<30_000_000_000 for x in result['Results'])
env=json.loads((root/'commands.json').read_text())['env'];assert env['GOMAXPROCS']=='2' and env['GOGC']=='100' and env['GOMEMLIMIT']=='2GiB'
r={'execution':e,'explicit_configuration':env,'actual_sdk_sha256':b['sha256'],'selected_source_inputs_verified':len(a['files']),'observed_sdk_closed':True,'native_normal_test_passes_seconds':passed,'delivery_measurements':result,'server_scope':'In-process R3 current server library compiled into observed SDK; allocation totals include server activity','source_capture_scope':'Selected Git Go/module inputs and actual executable metadata, not exhaustive external compiler/toolchain inputs','candidate_adopted':False,'qualifies_400k_capacity':False,'qualifies_24h':False}
shutil.copyfile(root/'originals/TestAuditDeliveryNativeCostComparison/delivery-cost.json',out/'delivery-cost.json')
(root/'independent-review.json').write_text(json.dumps(r,indent=2)+'\n');(out/'independent-review.json').write_text(json.dumps(r,indent=2)+'\n');shutil.copyfile(__file__,root/'executed-review.py');shutil.copyfile(__file__,out/'executed-review.py')
subprocess.run(['python3','/tmp/js-wf-preserve-read-proof-20261005.py',str(root),str(out)],check=True)
print('NATIVE_CONTROLS_REVIEW_AND_ARCHIVE_COMPLETE',flush=True)
