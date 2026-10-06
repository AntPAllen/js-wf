from pathlib import Path
import subprocess,json,hashlib,tarfile,shutil,time
repo=Path('/home/exedev/js-wf')
root=Path('/tmp/js-wf-bulk-journal-ten-minute-joined-20261006')
watch=Path('/tmp/js-wf-bulk-journal-ten-minute-joined-watch-20261006')
proof=Path('/tmp/js-wf-bulk-journal-ten-minute-joined-proof-20261006')
out=repo/'docs/scale/sustained-bulk-final-latency-2026-10-06/joined-journal-ten-minute'
def read(p):return json.loads(p.read_text())
def sha(p):
 with p.open('rb') as f:return hashlib.file_digest(f,'sha256').hexdigest()
end=time.monotonic()+25*60
while not (root/'archive-manifest.json').exists() or not (watch/'watch-result.json').exists():
 assert time.monotonic()<end,'Observation timeout; inspect same producer before any restart'
 time.sleep(1)
execution=read(root/'execution.json')
assert execution['status'] in ('row_verified','failed')
parent_pid=read(watch/'watch-result.json')['parent_pid'];assert parent_pid and not Path('/proc/'+str(parent_pid)).exists()
assert execution['chunked_state_retained_audit'] and not execution['concurrent_state_retained_audit'] and not execution['streaming_state_retained_audit'] and not execution['batched_retained_audit']
assert execution['bulk_final_latency'] and execution['compare_bulk_point'] and not execution['cached_latency_metadata']
assert execution['duration']=='10m' and execution['row']=='journal' and not execution['race']
assert read(watch/'watch-result.json')['parent_gone']
env=read(root/'test-environment.json');assert (env['GOMAXPROCS'],env['GOGC'],env['GOMEMLIMIT'],env['WF_TIER3_CHUNKED_STATE_RETAINED_AUDIT'])==('4','500','4GiB','1')
assert env['WF_MATRIX_BULK_FINAL_LATENCY']==env['WF_MATRIX_BULK_POINT_COMPARE']=='1'
archive=read(root/'archive-manifest.json');assert sha(root/'originals.tar.gz')==archive['archive_sha256']
checked=set()
with tarfile.open(root/'originals.tar.gz') as t:
 for m in t:
  assert m.isfile()
  assert m.name not in checked
  with t.extractfile(m) as f:digest=hashlib.file_digest(f,'sha256').hexdigest()
  assert digest==archive['files'][m.name]
  assert sha(root/m.name)==digest
  checked.add(m.name)
assert checked==set(archive['files'])
before=read(root/'source-before.json');after=read(root/'source-after.json')
assert before==after and before['revision']==execution['source']
for name,digest in before['files'].items():
 assert sha(root/'source'/name)==digest
 assert hashlib.sha256(subprocess.check_output(['git','show',execution['source']+':'+name],cwd=repo)).hexdigest()==digest
binary=read(root/'binary.json');assert sha(root/'integration.test')==binary['sha256']
sdks=[json.loads(x) for x in (watch/'sdks.jsonl').read_text().splitlines()]
parent=next(x for x in sdks if x['pid']==parent_pid)
assert parent['sha256']==binary['sha256']
assert parent['profile_environment']=={'GOMAXPROCS':'4','GOGC':'500','GOMEMLIMIT':'4GiB','WF_TIER3_CHUNKED_STATE_RETAINED_AUDIT':'1','WF_MATRIX_BULK_FINAL_LATENCY':'1','WF_MATRIX_BULK_POINT_COMPARE':'1'}
for sdk in sdks:assert sdk['sha256']==binary['sha256'] and not Path('/proc/'+str(sdk['pid'])).exists()
assert 'vcs.revision='+execution['source'] in parent['build_info'] and 'vcs.modified=false' in parent['build_info']
servers=[json.loads(x) for x in (watch/'servers.jsonl').read_text().splitlines()]
assert {x['inspect']['Name'].rsplit('-n',1)[-1] for x in servers}=={'0','1','2','3','4'}
server_sha=sha(root/'fixture/cluster/nats-server')
for x in servers:
 assert x['sha256']==server_sha
 assert not Path('/proc/'+str(x['pid'])).exists()
events=[json.loads(x) for x in (root/'events.jsonl').read_text().splitlines()]
terminal=[x for x in events if x.get('Test')=='TestFiveContainerMixedJournalLeaderEveryThirtySeconds' and x['Action'] in ('pass','fail')]
assert len(terminal)==1
review={'execution':execution,'native_terminal':terminal[0],'source_inputs_verified':len(before['files']),'original_files_verified':len(checked),'original_archive_sha256':archive['archive_sha256'],'actual_sdk_pid':parent_pid,'sdk_sha256':binary['sha256'],'server_observations':len(servers),'server_sha256':server_sha,'observed_processes_closed':True,'observation_scope':'Periodic server observations began during campaign; not exhaustive lifetime coverage','clears_full_tier3_release':False,'qualifies_24h':False,'candidate_adopted':False,'stores_not_reopened':True}
proof.mkdir();out.mkdir(parents=True,exist_ok=True)
if execution['status']=='row_verified':
 import importlib.util,sys
 spec=importlib.util.spec_from_file_location('row',repo/'scripts/check-tier3-journal-row.py');module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
 verification=module.check_bulk_latency_artifacts(root/'fixture',read(root/'result.json'),True)
 review['bulk_latency_artifact_checks']=verification
 review['bulk_latency']=read(root/'fixture/bulk-latency-audit.json')
 assert review['bulk_latency']['repair_writers_stopped'] is True
 assert review['bulk_latency']['repair_stop_requested_at'] <= review['bulk_latency']['repair_writers_joined_at']
 shutil.copyfile(repo/'scripts/check-tier3-journal-row.py',proof/'independent-bulk-checker.py')
 review['independent_bulk_checker_sha256']=sha(proof/'independent-bulk-checker.py')
 assert terminal[0]['Action']=='pass' and execution['test_exit_code']==0
 commands=read(root/'commands.json')
 check=next(x['command'] for x in commands if 'scripts/check-tier3-journal-row.py' in x['command'])
 command=list(check);command[command.index('--output')+1]=str(proof/'independent-row-result.json')
 subprocess.run(command,cwd=root/'source',check=True)
 assert read(proof/'independent-row-result.json')==read(root/'result.json')
 review['row_result']=read(root/'result.json')
else:
 assert terminal[0]['Action']=='fail' or execution.get('error')
 review['failure_preserved']=True
 review['bulk_latency']=read(root/'fixture/bulk-latency-audit.json')
# Preserve all periodic observations plus their actual captured executables,
# and the review in the full closed original root before its new complete archive.
for p in watch.iterdir():
 if p.is_file():shutil.copy2(p,root/('watch-'+p.name))
shutil.copytree(watch/'server-executables',root/'actual-server-executables')
shutil.copyfile(proof/'independent-bulk-checker.py',root/'independent-bulk-checker.py') if (proof/'independent-bulk-checker.py').exists() else None
review['scope']='Original ten-minute R5 journal faults with explicit bulk final audit, every sample matches point in same6m stage, original full counts/checkpoint/p99/history/drain gates retained; no default/fullmatrix/24h qualification. Complete original fixture and observer files preserved; no stores reopened.'
(root/'independent-review.json').write_text(json.dumps(review,indent=2)+'\n');(out/'independent-review.json').write_text(json.dumps(review,indent=2)+'\n')
shutil.copyfile(__file__,root/'executed-review.py');shutil.copyfile(__file__,out/'executed-review.py')
import sys,importlib.util
spec=importlib.util.spec_from_file_location('closed',repo/'scripts/verify-tier2-closed-originals.py');closed=importlib.util.module_from_spec(spec);spec.loader.exec_module(closed)
fd=closed.verify_no_open_originals(root)
(out/'visible-fd-closure.json').write_text(json.dumps(fd,indent=2)+'\n')
sys.path.insert(0,str(repo/'scripts'));import fixture_archive
fixture_archive.capture(root,Path('/tmp/js-wf-bulk-journal-ten-minute-joined-complete-20261006.tar.gz'),out)
print('INDEPENDENT_BULK_REVIEW_AND_PRESERVATION_COMPLETE',execution['status'],flush=True)
