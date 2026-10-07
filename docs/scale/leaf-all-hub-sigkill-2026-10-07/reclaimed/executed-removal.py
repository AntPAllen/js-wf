from pathlib import Path
import sys, subprocess, json, hashlib, shutil, importlib.util, datetime, os
sys.dont_write_bytecode = True
repo = Path('/home/exedev/js-wf')
sys.path.insert(0, str(repo/'scripts'))
import fixture_archive
def module(name, filename):
    spec = importlib.util.spec_from_file_location(name, repo/'scripts'/filename)
    obj = importlib.util.module_from_spec(spec); spec.loader.exec_module(obj); return obj
s3 = module('s3', 'offload-proof-to-s3.py')
closed = module('closed', 'verify-tier2-closed-originals.py')
head = subprocess.check_output(['git','rev-parse','HEAD'],cwd=repo,text=True).strip()
assert head == subprocess.check_output(['git','ls-remote','origin','refs/heads/main'],cwd=repo,text=True).split()[0]
def blob(path):
    data = subprocess.check_output(['git','cat-file','blob',head+':'+str(path)],cwd=repo)
    assert (repo/path).read_bytes() == data
    return data
config = s3.credentials()
out = Path('/tmp/storage-leaf-all-sigkill-proof'); out.mkdir(exist_ok=False)
shutil.copyfile(__file__,out/'executed-removal.py')
report = dict(head=head, started_utc=datetime.datetime.now(datetime.timezone.utc).isoformat(),
    free_bytes_before=shutil.disk_usage('/tmp').free, removed=[],
    scope='One completed all-hub/leaf SIGKILL/lease-expiry/weak-frame fixture and staging archive only, after fresh complete S3/member/local-inventory/closure verification. Failed and accepted verdicts preserved. Live24h, hub-only leaf fixture, donors and million fixtures retained.')
def save():
    report['free_bytes_after'] = shutil.disk_usage('/tmp').free
    (out/'removal.json').write_text(json.dumps(report,indent=2)+'\n')
def closure(root):
    limits=[]
    for p in Path('/proc').glob('[0-9]*'):
        try:
            assert not (p/'exe').resolve().is_relative_to(root)
            args=(p/'cmdline').read_bytes().split(b'\0')
            assert not any(str(root).encode() in arg for arg in args),(p,args)
            for link in ('cwd','root'):
                assert not (p/link).resolve().is_relative_to(root)
        except PermissionError: limits.append(str(p))
        except (FileNotFoundError,ProcessLookupError): pass
    ids=subprocess.check_output(['docker','ps','-aq'],text=True).split()
    containers=json.loads(subprocess.check_output(['docker','inspect',*ids],text=True)) if ids else []
    for c in containers:
        for m in c['Mounts']:
            source=Path(m['Source']).resolve()
            assert not source.is_relative_to(root) and not root.is_relative_to(source)
    loops=json.loads(subprocess.check_output(['sudo','-n','losetup','--list','--json'],text=True))
    for device in loops['loopdevices']:
        assert not Path(device['back-file']).resolve().is_relative_to(root)
    mounts=json.loads(subprocess.check_output(['findmnt','--json','--output','TARGET,SOURCE'],text=True))
    def walk(rows):
        for row in rows:
            assert not Path(row['target']).resolve().is_relative_to(root)
            assert not row['source'].startswith(str(root))
            walk(row.get('children',[]))
    walk(mounts['filesystems'])
    return dict(unobservable_processes=limits,visible_descriptors=closed.verify_no_open_originals(root),
                docker_ids_including_stopped=ids,loopdevices=loops,mounts=mounts)
def remote(url, expected):
    with subprocess.Popen(['curl','--config','-','--aws-sigv4','aws:amz:us-east-1:s3',
        '--silent','--show-error','--fail','--connect-timeout','30','--max-time','1800',url],
        stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE) as p:
        p.stdin.write(config);p.stdin.close()
        try: result=fixture_archive.verify_hashed_stream(p.stdout,expected)
        except BaseException: p.kill();p.wait();raise
        error=p.stderr.read();assert p.wait()==0,error
    return result
base='docs/scale/'
items=[('js-wf-leaf-all-sigkill-20261007','leaf-all-hub-sigkill-2026-10-07/native-race',True)]
save()
for name, canonical, remove_root in items:
    path=Path('/tmp')/(name+'.tar.gz'); proof=Path(base+canonical)
    meta=json.loads(blob(proof/'archive-verification.json'))
    receipt=json.loads(blob(proof/'s3-readback.json'))
    inventory_bytes=blob(proof/meta['inventory_file']); inventory=json.loads(inventory_bytes)
    assert hashlib.sha256(inventory_bytes).hexdigest()==meta['inventory_sha256']
    assert meta['schema']=='js-wf-full-fixture-archive-v1'
    assert meta['all_archive_members_read_back'] and meta['all_current_fixture_files_unchanged_after_capture']
    expected=dict(bytes=meta['archive_bytes'],sha256=meta['archive_sha256'])
    assert receipt['archive']['full_readback']==expected
    assert receipt['canonical_metadata']==str(proof/'archive-verification.json')
    assert path.is_file() and not path.is_symlink() and path.stat().st_nlink==1
    print('VERIFY_REMOTE',name,flush=True)
    declared,actual=remote(receipt['archive']['url'],expected)
    assert declared==inventory
    with path.open('rb') as f: assert s3.digest(f)==expected
    record=dict(archive_path=str(path),canonical_proof=str(proof),archive_url=receipt['archive']['url'],
        full_remote_archive_and_every_member_verified=actual,files=len(inventory['files']),
        inventory_sha256=meta['inventory_sha256'],removed_allocated_bytes=0)
    if remove_root:
        root=Path('/tmp')/name
        print('VERIFY_CLOSED_COPY',name,flush=True)
        service=name.replace('js-wf-bulk-soak-checkpoint8520-', 'js-wf-bulk-soak-checkpoint8520-')+'.service'
        service_state=subprocess.check_output(['systemctl','show',service,'-p','ActiveState','-p','SubState','-p','MainPID','-p','ExecMainStatus','-p','Restart'],text=True)
        expected_exit = 0 if name == 'js-wf-leaf-all-sigkill-20261007' else 1
        assert 'MainPID=0\n' in service_state and ('ExecMainStatus='+str(expected_exit)+'\n') in service_state and 'Restart=no\n' in service_state
        before=closure(root)
        assert fixture_archive.inventory(root)==inventory['files']
        allocated=sum(p.stat().st_blocks*512 for p in root.rglob('*'))+root.stat().st_blocks*512
        after=closure(root)
        record.update(root=str(root),local_complete_tree_matches_committed_inventory=True,
            service=service,service_state=service_state,closure_before=before,closure_immediately_before_removal=after)
        report['pending_verified_removal']=record;save()
        shutil.rmtree(root)
        assert not root.exists()
        record['removed_allocated_bytes']+=allocated
        print('REMOVED_COPY',name,allocated,flush=True)
    record['archive_closure']=closure(path)
    allocated=path.stat().st_blocks*512
    report['pending_verified_removal']=record;save()
    path.unlink();record['removed_allocated_bytes']+=allocated
    report.pop('pending_verified_removal',None)
    report['removed'].append(record);save()
    print('REMOVED_ARCHIVE',name,allocated,flush=True)
report['removed_allocated_bytes']=sum(x['removed_allocated_bytes'] for x in report['removed'])
report['finished_utc']=datetime.datetime.now(datetime.timezone.utc).isoformat();save()
print('FINISHED',report['removed_allocated_bytes'],'free',report['free_bytes_after'],flush=True)
