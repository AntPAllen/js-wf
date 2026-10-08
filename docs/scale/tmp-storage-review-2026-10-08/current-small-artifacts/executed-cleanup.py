import sys,json,subprocess,shutil,hashlib,datetime,importlib.util,os
from pathlib import Path
sys.dont_write_bytecode=True
repo=Path('/home/exedev/js-wf');sys.path.insert(0,str(repo/'scripts'))
import fixture_archive as fa
import boto3
base=repo/'docs/scale/tmp-storage-review-2026-10-08/current-small-artifacts'
stage=Path('/home/exedev/tmp-review-current-stage');archive=Path('/home/exedev/tmp-review-current.tar.gz')
def write(name,value): (base/name).write_text(json.dumps(value,indent=2)+'\n')
def closure(roots):
    code="import sys,json;sys.dont_write_bytecode=True;sys.path.insert(0,'/tmp');import importlib.util;from pathlib import Path;s=importlib.util.spec_from_file_location('c','/tmp/cleanup-evidence-storage-20261008.py');m=importlib.util.module_from_spec(s);s.loader.exec_module(m);r=json.load(sys.stdin);v=m.closure(list(map(Path,r)));\nfor p in Path('/proc').glob('[0-9]*/environ'):\n try: e=p.read_bytes()\n except (FileNotFoundError,ProcessLookupError): continue\n for root in r:\n  if root.encode() in e: v['blocked'].setdefault(root,[]).append(str(p))\nprint(json.dumps(v))"
    v=json.loads(subprocess.check_output(['sudo','-n','python3','-c',code],input=json.dumps([str(r) for r in roots]),text=True));assert not v['blocked'] and not v['permission_limits'],v;return v
if sys.argv[1]=='capture':
    assert not base.exists() and not stage.exists() and not archive.exists()
    roots=[Path('/tmp')/n for n in ['tmp.nvtYn8qqv8','tmp.Xza8spdAWs','tmp.4aix9GYhyi','tmp.TnXBgur0mg','tmp.VVJLOiTYfY','js-wf-graph-child-pins-20261008','js-wf-graph-child-signal-pins-20261008','js-wf-graph-reconcile-pins-20261008']]
    roots+=sorted(Path('/tmp').glob('js-wf-graph-child-failure-*'))+sorted(Path('/tmp').glob('js-wf-graph-reconcile-failure-*'))
    roots+=sorted(p for p in Path('/tmp').glob('graph-child*') if p.is_file() and p.suffix in ('.jsonl','.log'))
    for root,prefix in [(roots[5],'graph-child-'),(roots[6],'graph-child-signal-'),(roots[7],'graph-reconcile-')]:
        for p in root.glob('*.json'):
            target='sim/testdata/regressions/'+prefix+p.name
            assert p.read_bytes()==subprocess.check_output(['git','show','HEAD:'+target],cwd=repo),target
    before=closure(roots);base.mkdir();stage.mkdir()
    for r in roots:
        if r.is_dir(): shutil.copytree(r,stage/r.name)
        else: shutil.copy2(r,stage/r.name)
    proof=fa.capture(stage,archive,base,compresslevel=6)
    write('capture.json',dict(roots=list(map(str,roots)),closure=before,archive=str(archive),stage=str(stage),duplicate_pins_match_committed_bytes=True,scope='Preserves historical failed traces, closed development logs and temporary Git object directories. No test verdict changes. Dirty topology worktree and new purge fixtures retained.'))
    client=boto3.client('s3',endpoint_url='https://nameless-bird-8772.int.exe.xyz',aws_access_key_id='x',aws_secret_access_key='x')
    key='js-wf/tmp-cleanup/2026-10-08/'+proof['archive_sha256']+'.tar.gz';bucket='nameless-bird-8772'
    with archive.open('rb') as stream: client.put_object(Bucket=bucket,Key=key,Body=stream)
    expected=dict(bytes=proof['archive_bytes'],sha256=proof['archive_sha256'])
    response=client.get_object(Bucket=bucket,Key=key)
    declared,actual=fa.verify_hashed_stream(response['Body'],expected);response['Body'].close()
    assert declared==json.loads((base/'fixture-inventory.json').read_text())
    write('s3-readback.json',dict(endpoint='https://nameless-bird-8772.int.exe.xyz',bucket=bucket,key=key,full_readback=actual,all_members_verified=True))
    shutil.rmtree(stage);shutil.copyfile(__file__,base/'executed-cleanup.py')
    print(json.dumps(dict(roots=len(roots),archive=expected)),flush=True)
elif sys.argv[1]=='remove':
    cap=json.loads((base/'capture.json').read_text());roots=list(map(Path,cap['roots']));inv=json.loads((base/'fixture-inventory.json').read_text());receipt=json.loads((base/'s3-readback.json').read_text())
    head=subprocess.check_output(['git','rev-parse','HEAD'],cwd=repo,text=True).strip()
    assert head==subprocess.check_output(['git','ls-remote','origin','refs/heads/main'],cwd=repo,text=True).split()[0]
    for name in ['capture.json','fixture-inventory.json','archive-verification.json','s3-readback.json','executed-cleanup.py']:
        assert (base/name).read_bytes()==subprocess.check_output(['git','show',head+':'+str((base/name).relative_to(repo))],cwd=repo)
    client=boto3.client('s3',endpoint_url=receipt['endpoint'],aws_access_key_id='x',aws_secret_access_key='x')
    response=client.get_object(Bucket=receipt['bucket'],Key=receipt['key']);declared,actual=fa.verify_hashed_stream(response['Body'],receipt['full_readback']);response['Body'].close();assert declared==inv
    for r in roots:
        if r.is_dir(): assert fa.inventory(r)=={k[len(r.name)+1:]:v for k,v in inv['files'].items() if k.startswith(r.name+'/')},r
        else:
            st=r.stat();assert dict(bytes=st.st_size,sha256=hashlib.sha256(r.read_bytes()).hexdigest(),mode=st.st_mode&0o777,mtime_ns=st.st_mtime_ns)==inv['files'][r.name],r
    check=closure(roots)
    allocated=sum(p.stat().st_blocks*512 for r in roots for p in ([r,*r.rglob('*')] if r.is_dir() else [r]))
    usage_before=subprocess.check_output(['sudo','du','-sx','/tmp'],text=True).strip()
    for r in roots:
        if r.is_dir(): shutil.rmtree(r)
        else: r.unlink()
    archive.unlink()
    usage_after=subprocess.check_output(['sudo','du','-sx','/tmp'],text=True).strip()
    write('removal.json',dict(committed_pushed_head=head,roots=cap['roots'],fresh_s3_readback=actual,closure=check,original_allocated_bytes=allocated,tmp_before=usage_before,tmp_after=usage_after,all_removed=all(not r.exists() for r in roots),utc=datetime.datetime.now(datetime.timezone.utc).isoformat()))
    print(json.dumps(dict(freed_original_bytes=allocated,tmp_before=usage_before,tmp_after=usage_after)),flush=True)
