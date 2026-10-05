from pathlib import Path,PurePosixPath
import json,re,subprocess,hashlib,zipfile,stat,shutil,importlib.util
repo=Path('/home/exedev/js-wf')
run=json.load(open('/tmp/js-wf-tier3-run-20261005-1252.json'))
jobs=sum([json.loads(Path('/tmp/js-wf-tier3-jobs-page'+str(p)+'-20261005-1252.json').read_text())['jobs'] for p in [1,2,3]],[])
artifacts=json.loads(Path('/tmp/js-wf-tier3-artifacts-page1-20261005-1252.json').read_text())['artifacts']
spec=importlib.util.spec_from_file_location('shard',repo/'scripts/check-tier3-matrix-shard.py');mod=importlib.util.module_from_spec(spec);spec.loader.exec_module(mod)
selected=[]
for job in jobs:
    match=re.fullmatch(r'journal \(block_disk, (\d+)-(\d+)\)',job['name'])
    if match and (job['status'],job['conclusion'])==('completed','success'):
        first,last=map(int,match.groups())
        if (first,last) in [(157,169)]:selected.append((first,last,job))
assert sorted((f,l) for f,l,_ in selected)==[(157,169)]
def fetch(url,target):
    for attempt in range(1,4):
        stage=target.with_name(target.name+f'.attempt-{attempt}')
        result=subprocess.run(['gh','api','--allow-escape-sequences',url],capture_output=True)
        stage.write_bytes(result.stdout);stage.with_name(stage.name+'.stderr').write_bytes(result.stderr)
        if result.returncode==0:shutil.copyfile(stage,target);return
    raise RuntimeError('API transfer failed: '+url)
for first,last,job in sorted(selected):
    root=Path(f'/tmp/js-wf-tier3-block-stall{first}-{last}-37164231641');root.mkdir(exist_ok=False)
    matches=[a for a in artifacts if a['name']==f'tier3-block_disk-seed-{first}-{last}'];assert len(matches)==1;artifact=matches[0]
    for name,data in [('run.json',run),('job.json',job),('artifact.json',artifact)]:root.joinpath(name).write_text(json.dumps(data,indent=2)+'\n')
    shutil.copyfile(__file__,root/'executed-batch-review.py')
    fetch(f"repos/AntPAllen/js-wf/actions/jobs/{job['id']}/logs",root/'job.log')
    fetch(f"repos/AntPAllen/js-wf/actions/artifacts/{artifact['id']}/zip",root/'raw.zip')
    with (root/'raw.zip').open('rb') as f:digest=hashlib.file_digest(f,'sha256').hexdigest()
    assert 'sha256:'+digest==artifact['digest'] and (root/'raw.zip').stat().st_size==artifact['size_in_bytes']
    mod.bind(run,job,artifact,(root/'job.log').read_text(),'block_disk',first,last)
    with zipfile.ZipFile(root/'raw.zip') as z:
        names=set();total=0
        for m in z.infolist():
            n=PurePosixPath(m.filename);assert not n.is_absolute() and '..' not in n.parts and n.as_posix()==m.filename and '\\' not in m.filename and ':' not in m.filename and m.filename not in names and not m.is_dir() and not m.flag_bits&1 and stat.S_IFMT(m.external_attr>>16) in (0,stat.S_IFREG)
            names.add(m.filename);total+=m.file_size
        assert total<1024**3;(root/'raw').mkdir();z.extractall(root/'raw')
    root.joinpath('download-review.json').write_text(json.dumps(dict(zip_sha256=digest,members=len(names),expanded_bytes=total,metadata_binding_and_canonical_unique_regular_paths_verified=True),indent=2)+'\n')
    cmd=['python3',str(repo/'scripts/check-tier3-matrix-shard.py'),'--run',str(root/'run.json'),'--job',str(root/'job.json'),'--artifact-metadata',str(root/'artifact.json'),'--job-log',str(root/'job.log'),'--artifact-root',str(root/'raw'),'--output',str(root/'row-review.json'),'--row','block_disk','--first',str(first),'--last',str(last)]
    for label,command in [('raw-review',cmd),('model-review',['python3','/tmp/js-wf-review-block-stall-models-range-20261005-1152.py',str(root)])]:
        result=subprocess.run(command,capture_output=True,text=True,cwd=repo)
        root.joinpath(label+'.stdout').write_text(result.stdout);root.joinpath(label+'.stderr').write_text(result.stderr)
        assert result.returncode==0,(root,label,result.stderr)
    # Preserve only after reviewers terminate: no open output descriptors under root.
    result=subprocess.run(['python3','/tmp/js-wf-preserve-block-stall-range-20261005.py',str(root)],capture_output=True,text=True,cwd=repo)
    Path(f'/tmp/js-wf-block-stall-{first}-{last}-preserve-20261005.log').write_text(result.stdout+result.stderr)
    assert result.returncode==0,(root,result.stdout,result.stderr)
    print('COMPLETE',first,last,result.stdout.strip(),flush=True)
print('SEEDS157-169 COMPLETE',flush=True)
