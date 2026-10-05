from pathlib import Path,PurePosixPath
import hashlib,json,tarfile,shutil,subprocess,os,sys
root=Path(sys.argv[1])
repo=Path('/home/exedev/js-wf')
r=json.loads((root/'row-review.json').read_text());first,last=r['first_seed'],r['last_seed']
out=repo/f'docs/scale/current-tier3-block-stall-2026-10-05/seeds-{first}-{last}'
def sha(p):
    with p.open('rb') as f:return hashlib.file_digest(f,'sha256').hexdigest()
r=json.loads((root/'row-review.json').read_text());models=json.loads((root/'model-review.json').read_text())
assert r['shard_qualified'] and r['seeds']==last-first+1 and r['row']=='block_disk'
assert models['all_three_models_exact_ok'] and [x['seed'] for x in models['reports']]==list(range(first,last+1)) and models['source']==r['source']
raw={str(p.relative_to(root/'raw')):p for p in (root/'raw').rglob('*') if p.is_file()}
assert set(raw)==set(r['artifact_sha256'])
for n,p in raw.items():assert sha(p)==r['artifact_sha256'][n]
for name,key in [('run.json','run'),('job.json','job'),('artifact.json','artifact_metadata'),('job.log','job_log')]:assert sha(root/name)==r['input_sha256'][key]
for report in models['reports']:
    seed=report['seed'];ops=report['operations'];assert ops==sum(bool(l.strip()) for l in (root/f'raw/tier3-matrix-range/seed-{seed}/tier3-mixed-journal/history.jsonl').read_text().splitlines())
    assert (root/f'model-seed-{seed}.stdout').read_text().splitlines()==[f'whole {n} operations={ops} verdict=Ok error=<nil>' for n in ['starts','signals','results']]
    assert not (root/f'model-seed-{seed}.stderr').read_text()
deps=json.loads((root/'model-dependencies.json').read_text())
for n,d in deps['files'].items():
    assert sha(root/'model-source'/n)==sha(Path(deps['root'])/n)==d
    assert hashlib.sha256(subprocess.check_output(['git','show',r['source']+':'+n],cwd=repo)).hexdigest()==d
binary=json.loads((root/'model-binary.json').read_text());assert sha(root/'history-review')==binary['sha256']
assert subprocess.check_output(['go','version','-m',str(root/'history-review')],text=True)==binary['go_build_info']
for n,d in r['reviewer_sha256'].items():assert sha(root/'reviewer/scripts'/n)==d
opened=[]
for fd in Path('/proc').glob('[0-9]*/fd/*'):
    try:link=os.readlink(fd)
    except (FileNotFoundError,PermissionError):continue
    if link.startswith(str(root)+'/'):opened.append(str(fd))
assert not opened,opened
shutil.copyfile(__file__,root/'preserve-proof.py')
files={str(p.relative_to(root)):p for p in root.rglob('*') if p.is_file() and p.name!='proof.tar.gz'}
assert not any(p.is_symlink() for p in root.rglob('*'))
ledger={n:dict(sha256=sha(p),bytes=p.stat().st_size) for n,p in files.items()}
archive=root/'proof.tar.gz'
with tarfile.open(archive,'w:gz',compresslevel=6) as t:
    for n,p in sorted(files.items()):t.add(p,arcname=n,recursive=False)
seen=set()
with tarfile.open(archive,'r:gz') as t:
    for member in t:
        n=PurePosixPath(member.name);assert member.isfile() and not n.is_absolute() and '..' not in n.parts and n.as_posix()==member.name and member.name not in seen
        e=ledger[member.name];assert member.size==e['bytes'] and hashlib.file_digest(t.extractfile(member),'sha256').hexdigest()==e['sha256'];seen.add(member.name)
assert seen==set(ledger)
for n,p in files.items():assert sha(p)==ledger[n]['sha256']
out.mkdir(parents=True,exist_ok=False);parts=[];combined=hashlib.sha256()
with archive.open('rb') as f:
    for i,b in enumerate(iter(lambda:f.read(25*1024*1024),b'')):
        p=out/f'proof.tar.gz.part-{i:02d}';p.write_bytes(b);d=sha(p);assert d==hashlib.sha256(b).hexdigest();combined.update(p.read_bytes());parts.append(dict(path=p.name,bytes=len(b),sha256=d))
assert combined.hexdigest()==sha(archive)
(out/'manifest.json').write_text(json.dumps(dict(files=ledger,archive_bytes=archive.stat().st_size,archive_sha256=sha(archive),parts=parts,all_members_readback_verified=True,all_parts_readback_verified=True,visible_open_fds=opened),indent=2)+'\n')
summary=dict(source=r['source'],run=r['run_id'],job=r['job_id'],artifact=r['artifact_id'],first=first,last=last,invocations=r['invocations'],journal_entries=r['journal_entries'],faults=r['confirmed_faults'],model_operations=models['operations'],model_dependencies=len(deps['files']),all_three_models_exact_ok=True,worst_terminal_p99_seconds=max(c['terminal_p99_seconds'] for s in r['reports'] for c in s['cells'].values()),worst_progress_p99_seconds=max(c['progress_p99_seconds'] for s in r['reports'] for c in s['cells'].values()),workload_sdk_retained=False,physical_stores_retained=False,captured_full_source_inventory=False,final_integrity_and_drain_scope='named-test assertions',qualifies_full_row=False,qualifies_final_source=False,qualifies_full_matrix=False,qualifies_24h=False)
(out/'summary.json').write_text(json.dumps(summary,indent=2)+'\n')
print(json.dumps(summary),flush=True)
print('PRESERVED',len(ledger),archive.stat().st_size,sha(archive),flush=True)
