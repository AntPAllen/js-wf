from pathlib import Path
import json,hashlib,tarfile,importlib.util,tempfile,sys
root=Path(sys.argv[1]).resolve();repo=Path(sys.argv[2]).resolve()
metadata=json.loads((root/'terminal.json').read_text());revision='bac93561810bed2712dea7a42c487700209a9dcd'
assert metadata['status']=='completed' and metadata['conclusion']=='success' and metadata['headSha']==revision
assert len(metadata['jobs'])==1 and metadata['jobs'][0]['name']=='copied-source-contract' and metadata['jobs'][0]['status']=='completed' and metadata['jobs'][0]['conclusion']=='success'
manifest=json.loads((root/'scheduler-cleanup-manifest.json').read_text());hashes=manifest['files'];assert manifest['every_member_sha256_readback']
with tempfile.TemporaryDirectory(prefix='hosted-cleanup-review-') as temporary:
 out=Path(temporary)
 with tarfile.open(root/'scheduler-cleanup-originals.tar.gz','r:gz') as tar:
  members=tar.getmembers();assert len(members)==len(hashes) and all(m.isfile() and not Path(m.name).is_absolute() and '..' not in Path(m.name).parts for m in members)
  assert {m.name:hashlib.sha256(tar.extractfile(m).read()).hexdigest() for m in members}==hashes
  tar.extractall(out,filter='data')
 spec=importlib.util.spec_from_file_location('reviewer',repo/'scripts/review-nats-scheduler-cleanup.py');reviewer=importlib.util.module_from_spec(spec);spec.loader.exec_module(reviewer)
 review=reviewer.review(out,repo,True);original=json.loads((out/'independent-review.json').read_text());expected=dict(original)
 expected['module_cache_readback_at_review']=review['module_cache_readback_at_review'];assert review==expected
 assert review['source_revision']==revision and review['source_clean_at_execution']
 result=dict(accepted=True,run_id=37129135794,archive_members_verified=len(hashes),restored_review_matches=True,review=review)
(root/'independent-review.json').write_text(json.dumps(result,indent=2)+'\n');print(json.dumps(result,indent=2))
