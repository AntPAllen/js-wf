from pathlib import Path
import json,hashlib,subprocess,shutil,re,time
repo=Path('/home/exedev/js-wf');root=Path('/tmp/js-wf-parallel-latency-cohort-20261006');out=repo/'docs/scale/parallel-final-latency-2026-10-06/retained-cohort';out.mkdir(parents=True)
def sha(p):
 with p.open('rb') as f:return hashlib.file_digest(f,'sha256').hexdigest()
end=time.monotonic()+14*60
while not (root/'source-after.json').exists():
 assert time.monotonic()<end,'Observer timeout; inspect same native handle'
 time.sleep(1)
e=json.loads((root/'execution.json').read_text());assert e['status'] in ('passed','failed') and not Path('/proc/'+str(e['pid'])).exists()
qualified=e['status']=='passed' and e['exit_code']==0
b=json.loads((root/'binary.json').read_text());assert sha(root/'integration.test')==b['sha256']==e['sha256'];assert '-race=true' not in e['build_info'] and 'vcs.modified=false' in e['build_info']
assert '\tdep\tgithub.com/nats-io/nats.go\tv1.54.0' in e['build_info'] and '\tdep\tgithub.com/nats-io/nats-server/v2\tv2.15.0' in e['build_info']
a=json.loads((root/'source-before.json').read_text());assert a==json.loads((root/'source-after.json').read_text()) and a['revision']==e['source']
for name,digest in a['files'].items():
 assert sha(root/'source'/name)==digest
 assert hashlib.sha256(subprocess.check_output(['git','show',e['source']+':'+name],cwd=repo)).hexdigest()==digest
assert json.loads((root/'commands.json').read_text())['env']['WF_MATRIX_LATENCY_COHORT_ROOT']==str(root/'fixture')
log=(root/'native.log').read_text();passed={name:float(elapsed) for name,elapsed in re.findall(r'^--- PASS: (\S+) \((\d+\.\d+)s\)$',log,re.M)}
expected={'TestMatrixParallelInvocationAuditsRetainedCohort'}
if qualified:assert set(passed)==expected and '\nPASS\n' in log and '--- FAIL:' not in log
else:assert '--- FAIL:' in log
assert '\tdep\tgithub.com/nats-io/nats-server/v2\tv2.15.0' in e['build_info']
oracle=json.loads((root/'fixture/cohort.json').read_text()) if (root/'fixture/cohort.json').exists() else None
if qualified:
 assert oracle['cutoff']==87920 and oracle['completed_point_checks']==87920 and oracle['terminal_samples']==87920
 assert oracle['point_error']==oracle['report_error']=='<nil>' and oracle['elapsed_ns']<360_000_000_000
 assert oracle['report']=={'Invocations':87920,'Journals':87920,'Entries':969925,'Terminal':87920}
 assert len(oracle['samples'])==87920 and all(sum(s['event']=='terminal' for s in row)==1 for row in oracle['samples'])
servers=json.loads((root/'actual-containers/actual-servers.json').read_text());assert len(servers)==5
for server in servers:
 assert not Path('/proc/'+str(server['host_pid'])).exists()
 assert sha(root/'actual-containers'/server['sha256'])==server['sha256'] and '\tmod\tgithub.com/nats-io/nats-server/v2\tv2.15.0' in server['actual_proc_build_info']
 assert any(m['Source'].startswith(str(root/'copied-stores')) and m['Destination']=='/data' for m in server['container']['Mounts'])
copy=json.loads((root/'original-to-copy-verification.json').read_text());original=Path(copy['original'])
assert json.loads((root/'original-after-verification.json').read_text())=={'files':len(copy['files']),'all_original_bytes_unchanged':True}
assert sha(original/'originals.tar.gz')==copy['original_archive_sha256']
for name,digest in copy['files'].items():assert sha(original/name)==digest
r={'cohort_point_pass':qualified,'execution':e,'actual_sdk_sha256':b['sha256'],'selected_source_inputs_verified':len(a['files']),'observed_sdk_closed':True,'actual_servers_verified_closed':5,'original_files_unchanged':len(copy['files']),'native_test_passes_seconds':passed,'cohort':{k:v for k,v in oracle.items() if k!='samples'} if oracle else None,'scope':'Quiet verified disposable87920 real-workflow cohort, original6m point allowance/20s retained check; not original24h/full400k/currentmatrix acceptance'}

(root/'independent-review.json').write_text(json.dumps(r,indent=2)+'\n');(out/'independent-review.json').write_text(json.dumps(r,indent=2)+'\n');shutil.copyfile(__file__,root/'executed-review.py');shutil.copyfile(__file__,out/'executed-review.py')
subprocess.run(['python3','/tmp/js-wf-preserve-read-proof-20261005.py',str(root),str(out)],check=True)
print('NATIVE_CONTROLS_REVIEW_AND_ARCHIVE_COMPLETE',flush=True)
