from pathlib import Path
import subprocess,json,hashlib,tarfile,shutil,time
repo=Path('/home/exedev/js-wf')
root=Path('/tmp/js-wf-cached-latency-journal-10m-20261006')
watch=Path('/tmp/js-wf-cached-latency-journal-10m-watch-20261006')
proof=Path('/tmp/js-wf-cached-latency-journal-10m-proof-20261006')
out=repo/'docs/scale/cached-final-latency-2026-10-06/live-journal-ten-minute'
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
assert execution['cached_latency_metadata']
assert execution['duration']=='10m' and execution['row']=='journal' and not execution['race']
assert read(watch/'watch-result.json')['parent_gone']
env=read(root/'test-environment.json');assert (env['GOMAXPROCS'],env['GOGC'],env['GOMEMLIMIT'],env['WF_TIER3_CHUNKED_STATE_RETAINED_AUDIT'])==('4','500','4GiB','1')
assert env['WF_MATRIX_CACHED_LATENCY_METADATA']=='1'
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
assert parent['profile_environment']=={'GOMAXPROCS':'4','GOGC':'500','GOMEMLIMIT':'4GiB','WF_TIER3_CHUNKED_STATE_RETAINED_AUDIT':'1','WF_MATRIX_CACHED_LATENCY_METADATA':'1'}
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
 metadata=read(root/'fixture/latency-metadata.json');assert metadata['enabled'] is True
 counts=metadata['metadata_handle_lookups'];assert set(counts)=={'journal','signals','state','objects'} and all(type(v) is int and v>=0 for v in counts.values())
 assert counts['journal']>=1 and counts['state']>=1
 review['latency_metadata']=metadata
 shutil.copy2(root/'fixture/latency-metadata.json',proof/'latency-metadata.json')
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
for p in watch.iterdir():
 if p.is_file():shutil.copy2(p,proof/('watch-'+p.name))
shutil.copytree(watch/'server-executables',proof/'actual-server-executables')
shutil.copyfile(root/'integration.test',proof/'actual-sdk.bin')
for name,digest in before['files'].items():
 destination=proof/'selected-source'/name;destination.parent.mkdir(parents=True,exist_ok=True);shutil.copyfile(root/'source'/name,destination);assert sha(destination)==digest
for name in ('execution.json','archive-manifest.json','binary.json','source-before.json','source-after.json','events.jsonl','test-environment.json','commands.json','originals.tar.gz'):
 shutil.copy2(root/name,proof/name)
(proof/'independent-review.json').write_text(json.dumps(review,indent=2)+'\n')
shutil.copy2(proof/'independent-review.json',out/'independent-review.json')
shutil.copy2(__file__,proof/'executed-review.py');shutil.copy2(__file__,out/'executed-review.py')
subprocess.run(['python3','/tmp/js-wf-preserve-read-proof-20261005.py',str(proof),str(out)],check=True)
print('INDEPENDENT_REVIEW_AND_PRESERVATION_COMPLETE',execution['status'],flush=True)
