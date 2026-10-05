from pathlib import Path
import json,subprocess,hashlib,time,shutil
repo=Path('/home/exedev/js-wf');root=Path('/tmp/js-wf-concurrent-state-400k-capacity-bounded-preparation-20261005');out=repo/'docs/scale/concurrent-state-audit-2026-10-05/capacity-400k'
def sha(p):
 with p.open('rb') as f:return hashlib.file_digest(f,'sha256').hexdigest()
end=time.monotonic()+105*60
while True:
 try:
  e=json.loads((root/'execution.json').read_text())
  if e['status'] in ('passed','failed') and (root/'source-after.json').exists():break
 except (FileNotFoundError,json.JSONDecodeError):pass
 assert time.monotonic()<end,'Review observation timeout; inspect same handle before restart'
 time.sleep(1)
assert not Path('/proc/'+str(e['pid'])).exists()
b=json.loads((root/'binary.json').read_text());assert sha(root/'integrity.test')==b['sha256']==e['sha256']
a=json.loads((root/'source-before.json').read_text());assert a==json.loads((root/'source-after.json').read_text())
for n,digest in a['files'].items():
 assert sha(root/'source'/n)==digest
 assert hashlib.sha256(subprocess.check_output(['git','show',e['source']+':'+n],cwd=repo)).hexdigest()==digest
servers=json.loads((root/'actual-containers/actual-servers.json').read_text());assert len(servers)==5
for x in servers:
 assert not Path('/proc/'+str(x['host_pid'])).exists()
 assert sha(root/'actual-containers'/x['sha256'])==x['sha256']
results=list(root.rglob('capacity-comparison.json'));assert len(results)==1
results=json.loads(results[0].read_text());assert [x['Mode'] for x in results]==['sequential','concurrent','sequential_recheck']
want={'Invocations':400000,'Journals':400000,'Entries':4800000,'Terminal':400000}
for x in results:
 if x['Error']=='<nil>':assert x['Report']==want
r={'execution':e,'selected_git_source_inputs_verified':len(a['files']),'all_observed_processes_closed':True,'sdk_sha256':e['sha256'],'server_observations':len(servers),'comparisons':results,'scope':'Full synthetic quiet R5 400k/4.8M capacity profile; not live 24h or fault qualification','qualifies_24h':False,'candidate_adopted':False,'original_20s_deadlines_retained':True,'unchanged_native_rerun':False}
out.mkdir(parents=True)
(root/'independent-review.json').write_text(json.dumps(r,indent=2)+'\n');(out/'independent-review.json').write_text(json.dumps(r,indent=2)+'\n')
shutil.copyfile(__file__,root/'executed-review.py');shutil.copyfile(__file__,out/'executed-review.py')
subprocess.run(['python3','/tmp/js-wf-preserve-read-proof-20261005.py',str(root),str(out)],check=True)
print('CAPACITY_REVIEW_AND_ARCHIVE_COMPLETE',e['status'],flush=True)
