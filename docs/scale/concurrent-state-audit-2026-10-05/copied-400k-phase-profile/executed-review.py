from pathlib import Path
import json,hashlib,subprocess,shutil
repo=Path('/home/exedev/js-wf');root=Path('/tmp/js-wf-400k-copied-capacity-profile-20261005');out=repo/'docs/scale/concurrent-state-audit-2026-10-05/copied-400k-phase-profile';out.mkdir(parents=True)
def sha(p):
 with p.open('rb') as f:return hashlib.file_digest(f,'sha256').hexdigest()
e=json.loads((root/'execution.json').read_text());assert e['status']=='passed' and e['exit_code']==0 and not Path('/proc/'+str(e['pid'])).exists()
b=json.loads((root/'binary.json').read_text());assert sha(root/'integrity.test')==b['sha256']==e['sha256']
a=json.loads((root/'source-before.json').read_text());assert a==json.loads((root/'source-after.json').read_text())
for n,digest in a['files'].items():
 assert sha(root/'source'/n)==digest
 assert hashlib.sha256(subprocess.check_output(['git','show',e['source']+':'+n],cwd=repo)).hexdigest()==digest
servers=json.loads((root/'actual-containers/actual-servers.json').read_text());assert len(servers)==5
for x in servers:
 assert not Path('/proc/'+str(x['host_pid'])).exists()
 assert sha(root/'actual-containers'/x['sha256'])==x['sha256']
copy=json.loads((root/'original-to-copy-verification.json').read_text());assert copy['canonical_git_manifest_verified']
donor=Path(copy['original']);original_manifest=json.loads((donor/'archive-manifest.json').read_text())
assert json.loads((root/'original-after-verification.json').read_text())['all_original_store_bytes_unchanged']
for n,digest in copy['files'].items():assert sha(donor/n)==digest==original_manifest[n]['sha256']
profile_root=root/'originals/TestConcurrentStateR5CopiedCapacityProfile';profile=json.loads((profile_root/'capacity-phases.json').read_text())
assert profile['DiagnosticOnly'] and profile['Error']=='context deadline exceeded'
assert [x['Stream'] for x in profile['Phases']]==['WF_INV','WF_JRN']
assert profile['Phases'][0]['Records']==400000 and 0<profile['Phases'][1]['Records']<4800000
assert profile['Phases'][0]['Error']=='<nil>' and profile['Phases'][1]['Error']=='context deadline exceeded'
for name,args in [('cpu-top.txt',['-top','-nodecount=25','audit-cpu.pprof']),('alloc-space-top.txt',['-top','-sample_index=alloc_space','-nodecount=25','audit-allocs.pprof']),('cpu-tags.txt',['-tags','audit-cpu.pprof'])]:
 command=['go','tool','pprof',*args[:-1],str(root/'integrity.test'),str(profile_root/args[-1])]
 with (root/name).open('w') as f:subprocess.run(command,cwd=repo,stdout=f,stderr=subprocess.STDOUT,check=True)
 shutil.copyfile(root/name,out/name)
r={'execution':e,'selected_git_source_inputs_verified':len(a['files']),'all_observed_processes_closed':True,'original_store_files_unchanged':len(copy['files']),'canonical_original_archive_sha256':copy['canonical_archive_sha256'],'server_observations':len(servers),'profile':profile,'original_audit_failed':True,'diagnostic_test_completed':True,'qualifies_24h':False,'candidate_adopted':False,'cause_attribution':'None; CPU/allocation samples and phase counts are diagnostic evidence'}
(root/'independent-review.json').write_text(json.dumps(r,indent=2)+'\n');(out/'independent-review.json').write_text(json.dumps(r,indent=2)+'\n')
shutil.copyfile(profile_root/'capacity-phases.json',out/'capacity-phases.json');shutil.copyfile(__file__,root/'executed-review.py');shutil.copyfile(__file__,out/'executed-review.py')
subprocess.run(['python3','/tmp/js-wf-preserve-read-proof-20261005.py',str(root),str(out)],check=True)
print('COPIED_CAPACITY_PROFILE_REVIEW_AND_ARCHIVE_COMPLETE',flush=True)
