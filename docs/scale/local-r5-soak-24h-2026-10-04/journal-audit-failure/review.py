from pathlib import Path
import json,hashlib,tarfile,subprocess
root=Path('/tmp/js-wf-soak-journal-24h-20261004');out=Path('/tmp/js-wf-soak-journal-24h-failure-review-20261004');repo=Path('/home/exedev/js-wf')
def sha(p):
 with p.open('rb') as f:return hashlib.file_digest(f,'sha256').hexdigest()
e=json.loads((root/'execution.json').read_text());assert e['status']=='failed' and e['test_exit_code']==1 and e['duration']=='24h' and e['source']=='20babb566534e3ea7aa06b5526a748c9d65c6ed5'
m=json.loads((root/'archive-manifest.json').read_text());archive=root/'originals.tar.gz';assert sha(archive)==m['archive_sha256'];seen=set();total=0
with tarfile.open(archive,'r:gz') as tar:
 for member in tar:
  assert member.isfile() and member.name in m['files'] and member.name not in seen
  seen.add(member.name);f=tar.extractfile(member);h=hashlib.sha256();n=0
  for data in iter(lambda:f.read(1024*1024),b''):h.update(data);n+=len(data)
  assert h.hexdigest()==m['files'][member.name] and n==member.size,member.name;total+=n
assert seen==set(m['files'])
before=json.loads((root/'source-before.json').read_text());after=json.loads((root/'source-after.json').read_text());assert before==after and before['revision']==e['source']
for name,digest in before['files'].items():
 assert m['files']['source/'+name]==digest
 assert hashlib.sha256(subprocess.check_output(['git','show',e['source']+':'+name],cwd=repo)).hexdigest()==digest,name
binary=json.loads((root/'binary.json').read_text());assert binary['race'] and '-race=true' in binary['build_info'] and sha(root/'integration.test')==binary['sha256']==m['files']['integration.test']
rows=[json.loads(line) for line in (root/'events.jsonl').read_text().splitlines()];failed=[r for r in rows if r['Action']=='fail' and r.get('Test')=='TestFiveContainerMixedJournalLeaderEveryThirtySeconds'];assert len(failed)==1
messages=''.join(r.get('Output','') for r in rows)
assert 'intermediate retained audit batch=110 cutoff=3080' in messages and messages.index('tier3 checkpoint failed:')<messages.index('resultmatrixfanout/tier3-1-batch-117-5= err=context canceled')
audits=json.loads((root/'fixture/checkpoint-audits.json').read_text());print('audit_data_type',type(audits).__name__)
summary=dict(source=e['source'],row='journal',seed=1,requested_duration='24h',named_test_seconds=failed[0]['Elapsed'],test_exit_code=1,failed_checkpoint_batch=110,failed_invocation_cutoff=3080,primary_error='retained audit: context deadline exceeded',audit_attempt_seconds=20,audit_total_seconds=60,secondary_fanout_cancellation=True,source_inputs=len(before['files']),source_before_after_matches_git=True,actual_sdk_sha256=binary['sha256'],actual_sdk_retained=True,archive_members=len(seen),original_member_bytes=total,archive_bytes=archive.stat().st_size,archive_sha256=m['archive_sha256'],all_original_member_hashes_verified=True,physical_stores_reopened=False,cause_confirmed=False,qualifies_24h_row=False,qualifies_full_matrix=False,scope='Actual 24h attempt failed after approximately15min; no restart or audit-budget relaxation. Complete failed originals retained; failure phase/request cause remains to be isolated.')
(out/'failure-review.json').write_text(json.dumps(summary,indent=2)+'\n');print(json.dumps(summary,indent=2))
