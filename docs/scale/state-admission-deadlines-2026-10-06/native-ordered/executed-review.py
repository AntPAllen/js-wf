from pathlib import Path
import sys,json,subprocess,hashlib,shutil,re,time
repo=Path('/home/exedev/js-wf');root=Path('/tmp/js-wf-state-admission-cohort-20261006');out=repo/'docs/scale/state-admission-deadlines-2026-10-06/native-ordered';out.mkdir(parents=True,exist_ok=True)
sys.path.insert(0,str(repo/'scripts'));import fixture_delta
end=time.monotonic()+8*60
while not ((root/'source-after.json').exists() and (root/'original-after-verification.json').exists()):
 assert time.monotonic()<end,'Observer timeout: inspect same native handle';time.sleep(1)
def sha(p):
 with p.open('rb') as f:return hashlib.file_digest(f,'sha256').hexdigest()
e=json.loads((root/'execution.json').read_text());assert e['status']=='passed' and e['exit_code']==0 and not Path('/proc',str(e['pid'])).exists()
b=json.loads((root/'binary.json').read_text());assert b['sha256']==e['sha256']==sha(root/'integration.test') and 'vcs.modified=false' in e['build_info']
a=json.loads((root/'source-before.json').read_text());assert a==json.loads((root/'source-after.json').read_text()) and a['revision']==e['source']
for n,h in a['files'].items():assert sha(root/'source'/n)==h and hashlib.sha256(subprocess.check_output(['git','cat-file','blob',e['source']+':'+n],cwd=repo)).hexdigest()==h
assert '\tdep\tgithub.com/nats-io/nats-server/v2\tv2.15.0' in e['build_info'] and '\tdep\tgithub.com/nats-io/nats.go\tv1.54.0' in e['build_info']
log=(root/'native.log').read_text();passes=re.findall(r'^--- PASS: (\S+) \((\d+\.\d+)s\)$',log,re.M);assert passes==[('TestRetainedStateAdmissionCopiedDiagnostic','23.78')] and '\nPASS\n' in log
case=root/'fixture/TestRetainedStateAdmissionCopiedDiagnostic';proof=json.loads((case/'state-admission.json').read_text());assert [r['label'] for r in proof['results']]==['standalone-state-20s','concurrent-full87920-20s']
for r in proof['results']:
 assert r['accepted'] and r['error']=='<nil>' and r['elapsed_ns']<20*10**9
 assert r['before']['state']==r['after']['state']
 assert r['before']['state']['consumer_count']==0 and r['before']['state']['messages']==88068
 frames=r['frames'];assert frames[0]['event']=='watch_start' and frames[-1]['event']=='attempt_return'
 assert sum(f['event']=='initial_complete' for f in frames)==sum(f['event']=='watch_stopped' for f in frames)==1
 assert all(f['error']=='' for f in frames if 'error' in f)
 if r['label'].startswith('standalone'):
  assert r['values']==88068 and 19*10**9<frames[0]['budget_ns']<=20*10**9
 else:
  assert r['report']==dict(Invocations=87920,Journals=87920,Entries=969925,Terminal=87920) and 1.9*10**9<frames[0]['budget_ns']<=2*10**9
 assert r['watch_consumer_metadata_error']=='<nil>' and len(r['watch_consumer_metadata'])==1
 info=r['watch_consumer_metadata'][0];assert info['config']['num_replicas']==1 and info['config']['mem_storage'] and info['config']['ack_policy']=='none'
servers=json.loads((root/'actual-containers/actual-servers.json').read_text());assert len(servers)==5
for s in servers:assert not Path('/proc',str(s['host_pid'])).exists() and sha(root/'actual-containers'/s['sha256'])==s['sha256'] and any(m['Source'].startswith(str(root/'copied-stores')) and m['Destination']=='/data' for m in s['container']['Mounts'])
copy=json.loads((root/'original-to-copy-verification.json').read_text());original=Path(copy['original']);assert json.loads((root/'original-after-verification.json').read_text())==dict(files=len(copy['files']),all_original_bytes_unchanged=True)
for n,h in copy['files'].items():assert sha(original/n)==h
review=dict(observation_test_pass=True,native_test_seconds=float(passes[0][1]),execution=e,actual_sdk_sha256=e['sha256'],selected_git_source_inputs_verified=len(a['files']),actual_servers_verified_closed=5,original_donor_files_unchanged=len(copy['files']),ordered_admission=proof,scope='Standalone and subsequent concurrent admission each accepted at this source/profile; ordered comparison and no large bulk/fault/24h qualification; does not establish cause of earlier failure')
(root/'independent-review.json').write_text(json.dumps(review,indent=2)+'\n');(out/'independent-review.json').write_text(json.dumps(review,indent=2)+'\n');shutil.copy2(__file__,root/'executed-review.py');shutil.copy2(__file__,out/'executed-review.py')
# Complete file-byte preservation, including all current clone bytes, references
# unchanged committed files from the accepted real-cohort base rather than
# duplicating the ~2GB store again. Base and delta are independently read back.
base='docs/scale/cached-final-latency-2026-10-06/retained-cohort/archive-verification.json'
meta=fixture_delta.capture(root,out,repo,e['source'],base,'copied-stores/','copied-stores/')
print('REVIEW_AND_FULL_BASE_PLUS_DELTA_COMPLETE',json.dumps({k:meta[k] for k in ['archive_bytes','logical_files','aliased_files','aliased_bytes','archive_sha256']}),flush=True)
