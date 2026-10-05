from pathlib import Path
import json,hashlib,subprocess,shutil,re,time
repo=Path('/home/exedev/js-wf');root=Path('/tmp/js-wf-buffered-callback-delivery-cost-native-20261005');out=repo/'docs/scale/buffered-callback-audit-2026-10-05/failed-delivery-measurement';out.mkdir(parents=True)
def sha(p):
 with p.open('rb') as f:return hashlib.file_digest(f,'sha256').hexdigest()
end=time.monotonic()+8*60
while not (root/'source-after.json').exists():
 assert time.monotonic()<end,'Observer timeout; inspect same native handle'
 time.sleep(1)
e=json.loads((root/'execution.json').read_text());assert e['status']=='failed' and e['exit_code']==1 and not Path('/proc/'+str(e['pid'])).exists()
b=json.loads((root/'binary.json').read_text());assert sha(root/'integrity.test')==b['sha256']==e['sha256'];assert '-race=true' not in e['build_info'] and 'vcs.modified=false' in e['build_info']
assert '\tdep\tgithub.com/nats-io/nats.go\tv1.54.0' in e['build_info'] and '\tdep\tgithub.com/nats-io/nats-server/v2\tv2.15.0' in e['build_info']
a=json.loads((root/'source-before.json').read_text());assert a==json.loads((root/'source-after.json').read_text()) and a['revision']==e['source']
for name,digest in a['files'].items():
 assert sha(root/'source'/name)==digest
 assert hashlib.sha256(subprocess.check_output(['git','show',e['source']+':'+name],cwd=repo)).hexdigest()==digest
log=(root/'native.log').read_text()
assert '--- FAIL: TestAuditDeliveryNativeCostComparison (7.91s)' in log and '\nFAIL\n' in log and 'Consumers:1' in log and 'Msgs:100000' in log
observed=re.findall(r'delivery cost \{Mode:(\S+) Records:(\d+) ElapsedNS:(\d+) AllocatedBytes:(\d+) GCCycles:(\d+)\}',log)
assert [x[0] for x in observed]==['next','adapter','consume','callback_adapter','buffered_callback_adapter','next_recheck']
assert all(int(x[1])==100000 for x in observed)
assert not (root/'originals/TestAuditDeliveryNativeCostComparison/delivery-cost.json').exists()
env=json.loads((root/'commands.json').read_text())['env'];assert env['GOMAXPROCS']=='2' and env['GOGC']=='100' and env['GOMEMLIMIT']=='2GiB'
results=[{'Mode':m,'Records':int(n),'ElapsedNS':int(t),'AllocatedBytes':int(v),'GCCycles':int(g)} for m,n,t,v,g in observed]
r={'execution':e,'actual_sdk_sha256':b['sha256'],'selected_source_inputs_verified':len(a['files']),'observed_sdk_closed':True,'native_failed_cleanup':True,'final_observed_consumer_count':1,'final_observed_stream_messages':100000,'parsed_logged_delivery_measurements':results,'explicit_configuration':env,'native_result_json_absent_after_failure':True,'candidate_adopted':False,'qualifies_400k_capacity':False,'qualifies_24h':False,'scope':'Every delivery mode logged full100k; final stream consumer-count check failed; residual consumer identity/cause unconfirmed; timings do not establish speedup; in-process R3 server linked into observed SDK','source_capture_scope':'Selected Git Go/module and executable metadata, not exhaustive compiler/toolchain inputs'}
shutil.copyfile('/tmp/js-wf-review-buffered-callback-delivery-cost-native-20261005.py',root/'initial-reviewer.py')
(root/'initial-reviewer.log').write_text(subprocess.check_output(['journalctl','--user','-u','js-wf-buffered-callback-delivery-cost-native-review-20261005.service','--no-pager'],text=True))
(root/'independent-review.json').write_text(json.dumps(r,indent=2)+'\n');(out/'independent-review.json').write_text(json.dumps(r,indent=2)+'\n')
shutil.copyfile(__file__,root/'executed-review.py');shutil.copyfile(__file__,out/'executed-review.py')
subprocess.run(['python3','/tmp/js-wf-preserve-read-proof-20261005.py',str(root),str(out)],check=True)
print('FAILED_DELIVERY_CLEANUP_REVIEW_AND_ARCHIVE_COMPLETE',flush=True)
