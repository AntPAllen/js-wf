from pathlib import Path
import json,hashlib,subprocess,shutil,re,time
repo=Path('/home/exedev/js-wf');root=Path('/tmp/js-wf-bulk-latency-cohort-whole-state-20261006');out=repo/'docs/scale/state-admission-deadlines-2026-10-06/native-bulk-whole-state';out.mkdir(parents=True,exist_ok=True)
def sha(p):
 with p.open('rb') as f:return hashlib.file_digest(f,'sha256').hexdigest()
end=time.monotonic()+14*60
while not ((root/'source-after.json').exists() and (root/'original-after-verification.json').exists()):
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
recorded_env=json.loads((root/'commands.json').read_text())['env']
assert recorded_env['WF_MATRIX_LATENCY_COHORT_ROOT']==str(root/'fixture')
assert recorded_env['WF_MATRIX_CACHED_LATENCY_METADATA']=='1'
log=(root/'native.log').read_text();passed={name:float(elapsed) for name,elapsed in re.findall(r'^--- PASS: (\S+) \((\d+\.\d+)s\)$',log,re.M)}
expected={'TestMatrixParallelInvocationAuditsRetainedCohort'}
if qualified:assert set(passed)==expected and '\nPASS\n' in log and '--- FAIL:' not in log
else:assert '--- FAIL:' in log
assert '\tdep\tgithub.com/nats-io/nats-server/v2\tv2.15.0' in e['build_info']
oracle=json.loads((root/'fixture/cohort.json').read_text()) if (root/'fixture/cohort.json').exists() else None
if qualified:
 counts=oracle['metadata_handle_lookups']
 assert set(counts)=={'journal','signals','state','objects'}
 assert all(type(v) is int and v>=0 for v in counts.values())
 assert all(counts[k]==0 for k in ('journal','signals','state','objects'))
 # Diagnostic handle attempts: failed lookups may retry and snapshots may load objects.
 assert oracle['cutoff']==87920 and oracle['completed_point_checks']==0 and oracle['terminal_samples']==87920
 assert oracle['audit_mode']=='bulk' and oracle['bulk_equals_accepted_point_oracle'] and oracle['bulk_stats']['invocation_cutoff']==87920 and oracle['bulk_stats']['charged_bytes']<3*1024**3
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
inner=json.loads((repo/'docs/scale/nested-raw-proof-recovery-2026-10-06/watch-observed-journal-24h-inner-manifest.json').read_text())
assert inner['archive_sha256']==copy['original_archive_sha256']
for name,digest in copy['files'].items():assert inner['files'][name]==digest
oracle_proof=json.loads((root/'original-point-oracle-verification.json').read_text())
assert oracle_proof['all_canonical_members_verified']
point_path=Path(oracle_proof['oracle_path']);assert point_path.stat().st_size==oracle_proof['oracle_expected']['bytes'] and sha(point_path)==oracle_proof['oracle_expected']['sha256']
import sys
sys.path.insert(0,str(repo/'scripts'));import fixture_delta
baseline=fixture_delta.read_base(repo,oracle_proof['revision'],oracle_proof['canonical_metadata'])
assert baseline[oracle_proof['oracle_member']]==oracle_proof['oracle_expected']
if qualified:
 point=json.loads(point_path.read_text())
 assert point['point_error']==point['report_error']=='<nil>' and point['cutoff']==oracle['cutoff'] and point['original_completion_deadline']==oracle['original_completion_deadline']
 assert point['samples']==oracle['samples']

for name,digest in copy['files'].items():assert sha(original/name)==digest
r={'cohort_bulk_pass':qualified,'execution':e,'actual_sdk_sha256':b['sha256'],'selected_source_inputs_verified':len(a['files']),'observed_sdk_closed':True,'actual_servers_verified_closed':5,'original_files_unchanged':len(copy['files']),'native_test_passes_seconds':passed,'cohort':{k:v for k,v in oracle.items() if k!='samples'} if oracle else None,'scope':'Quiet verified disposable87920 real-workflow exact bulk versus canonical accepted point samples, original6m stage/20s retained checks; not original24h/full400k/currentmatrix acceptance'}

(root/'independent-review.json').write_text(json.dumps(r,indent=2)+'\n');(out/'independent-review.json').write_text(json.dumps(r,indent=2)+'\n');shutil.copyfile(__file__,root/'executed-review.py');shutil.copyfile(__file__,out/'executed-review.py')
fixture_delta.capture(root,out,repo,e['source'],oracle_proof['canonical_metadata'],'copied-stores/','copied-stores/')
print('NATIVE_CONTROLS_REVIEW_AND_ARCHIVE_COMPLETE',flush=True)
