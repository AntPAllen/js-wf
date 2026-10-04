from pathlib import Path,PurePosixPath
import hashlib,json,subprocess,tarfile,importlib.util,os,shutil,re
repo=Path('/home/exedev/js-wf');root=Path('/tmp/js-wf-journal-streaming-explicit-routes-normal-2g-24h-20261004')
out=repo/'docs/scale/local-r5-streaming-audit-2026-10-04/explicit-routes-normal-2g-24h-failed'
source='95b63c0e757095a66f3e15d991fafbb657e6034c'
def sha(p):
    with p.open('rb') as f:return hashlib.file_digest(f,'sha256').hexdigest()
e=json.loads((root/'execution.json').read_text());assert e['status']=='failed' and e['test_exit_code']==1 and e['source']==source
m=json.loads((root/'archive-manifest.json').read_text());seen=set()
with tarfile.open(root/'originals.tar.gz','r:gz') as t:
    for member in t:
        n=PurePosixPath(member.name);assert member.isfile() and not n.is_absolute() and '..' not in n.parts and n.as_posix()==member.name and member.name not in seen and member.name in m['files']
        assert hashlib.file_digest(t.extractfile(member),'sha256').hexdigest()==m['files'][member.name];seen.add(member.name)
assert seen==set(m['files'])
for n,d in m['files'].items():assert sha(root/n)==d
spec=importlib.util.spec_from_file_location('review','/home/exedev/js-wf/scripts/check-tier3-matrix-shard.py');module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
before=json.loads((root/'source-before.json').read_text());after=json.loads((root/'source-after.json').read_text())
assert before==after
expected=module.source_hashes(source)
listing=subprocess.check_output(['git','ls-tree','-rz','--full-tree',source],cwd=repo)
for record in listing.split(b'\0'):
    if not record:continue
    desc,name=record.split(b'\t',1);name=name.decode()
    if name.startswith('sim/testdata/'):
        mode,kind,oid=desc.split();assert kind==b'blob' and mode in [b'100644',b'100755']
        expected[name]=hashlib.sha256(subprocess.check_output(['git','show',source+':'+name],cwd=repo)).hexdigest()
assert before==dict(revision=source,files=expected)
b=json.loads((root/'binary.json').read_text());actual=json.loads((root/'actual-sdk.json').read_text());assert not b['race'] and sha(root/'integration.test')==b['sha256']==actual['sha256']
info=subprocess.check_output(['go','version','-m',str(root/'integration.test')],text=True)
assert info.splitlines()[1:]==b['build_info'].splitlines()[1:]==actual['build_info'].splitlines()[1:]
assert actual['environment']['GOMEMLIMIT']=='2GiB' and actual['environment']['GOMAXPROCS']=='2'
events=[json.loads(l) for l in (root/'events.jsonl').read_text().splitlines()]
test='TestFiveContainerMixedJournalLeaderEveryThirtySeconds'
fail=[v for v in events if v.get('Test')==test and v['Action']=='fail'];assert len(fail)==1
logs=''.join(v.get('Output','') for v in events)
assert 'tier3 checkpoint batch=1040 report={Invocations:29120 Journals:29120 Entries:321244 Terminal:29120}' in logs
assert 'intermediate retained audit batch=1050 cutoff=29400' in logs
traces=[]
for attempt in [1,2,3]:
    p=root/f'fixture/audit-batch-1050-attempt-{attempt}-trace.json';t=json.loads(p.read_text());traces.append(dict(attempt=attempt,counts=t['counts'],trace_path=str(p.relative_to(root)),sha256=sha(p)))
assert traces[0]['counts']['WF_JRN.GetMsg']['started']==2869
opened=[]
for fd in Path('/proc').glob('[0-9]*/fd/*'):
    try:link=os.readlink(fd)
    except (FileNotFoundError,PermissionError):continue
    if link.startswith(str(root)+'/'):opened.append(str(fd))
assert not opened,opened
out.mkdir(parents=True,exist_ok=False);parts=[];h=hashlib.sha256()
with (root/'originals.tar.gz').open('rb') as f:
    for i,data in enumerate(iter(lambda:f.read(25*1024*1024),b'')):
        p=out/f'originals.tar.gz.part-{i:02d}';p.write_bytes(data);d=sha(p);assert d==hashlib.sha256(data).hexdigest();h.update(p.read_bytes());parts.append(dict(path=p.name,bytes=len(data),sha256=d))
assert h.hexdigest()==sha(root/'originals.tar.gz')
(out/'manifest.json').write_text(json.dumps(dict(files=m['files'],archive_bytes=(root/'originals.tar.gz').stat().st_size,archive_sha256=sha(root/'originals.tar.gz'),parts=parts,all_members_and_originals_readback_verified=True,all_parts_and_combined_sha_readback_verified=True,visible_open_fds=opened),indent=2)+'\n')
review=dict(source=source,accepted_workload=False,test_exit_code=1,named_test_seconds=fail[0]['Elapsed'],verified_source_files=len(before['files']),actual_sdk_sha256=b['sha256'],all_build_fields_verified=True,original_files=len(seen),last_successful_checkpoint=dict(batch=1040,invocations=29120,entries=321244),failed_checkpoint=dict(batch=1050,invocation_cutoff=29400,attempts=3,attempt_budget_seconds=20,total_budget_seconds=60),traces=traces,concurrent_native_million_launch=True,server_cause_unconfirmed=True,resource_causality_unconfirmed=True,stores_independently_reopened=False,qualifies_24h=False,qualifies_full_matrix=False)
(out/'review.json').write_text(json.dumps(review,indent=2)+'\n');shutil.copyfile(__file__,out/'executed-preservation.py')
shutil.copytree('/tmp/js-wf-explicit-route-24h-first-preserver-error-20261004',out/'first-review-inventory-selector-error')
print(json.dumps({k:v for k,v in review.items() if k!='traces'}),flush=True)
