from storage_review_common import *
spec=importlib.util.spec_from_file_location('s3',repo/'scripts/offload-proof-to-s3.py')
s3=importlib.util.module_from_spec(spec);spec.loader.exec_module(s3)
head=subprocess.check_output(['git','rev-parse','HEAD'],cwd=repo,text=True).strip()
assert head==subprocess.check_output(['git','ls-remote','origin','refs/heads/main'],cwd=repo,text=True).split()[0]
def blob(path):
    data = subprocess.check_output(['git','cat-file','blob',head+':'+str(path)],cwd=repo)
    assert (repo/path).read_bytes() == data
    return data
def remote(url, expected):
    with subprocess.Popen(['curl','--config','-','--aws-sigv4','aws:amz:us-east-1:s3',
        '--silent','--show-error','--fail','--connect-timeout','30','--max-time','1800',url],
        stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE) as p:
        p.stdin.write(config);p.stdin.close()
        try: result=fixture_archive.verify_hashed_stream(p.stdout,expected)
        except BaseException: p.kill();p.wait();raise
        error=p.stderr.read();assert p.wait()==0,error
    return result
config=s3.credentials()
out=Path('/tmp/storage-operator-connection-removal-20261007');out.mkdir(exist_ok=False)
shutil.copyfile(__file__,out/'executed-removal.py');shutil.copyfile('/tmp/storage_review_common.py',out/'executed-common.py')
report=dict(head=head,started_utc=datetime.datetime.now(datetime.timezone.utc).isoformat(),free_bytes_before=shutil.disk_usage('/tmp').free,removed=[],scope='Closed corrected connection-signals/native daemon regression and previous CLI baseline roots and archives; the exact clean baseline worktree is removed through Git. Fresh remote members/current inventory/process/FD/Docker/mount/loop checks. Live24h and other fixtures retained; all original verdicts unchanged.')
def save():
 report['free_bytes_after']=shutil.disk_usage('/tmp').free
 (out/'removal.json').write_text(json.dumps(report,indent=2)+'\n')
base=Path('docs/scale/operator-connection-signals-2026-10-07')
items=[('js-wf-operator-connection-signals-20261007','js-wf-operator-connection-signals-20261007.tar.gz','native-race'),('js-wf-operator-connection-regression-20261007','js-wf-operator-connection-regression-20261007.tar.gz','daemon-regression-race'),('js-wf-operator-connection-baseline-20261007','js-wf-operator-connection-baseline-20261007.tar.gz','previous-cli-baseline')]
worktrees=[Path(line.removeprefix('worktree ')) for line in subprocess.check_output(['git','worktree','list','--porcelain'],cwd=repo,text=True).splitlines() if line.startswith('worktree ')]
save()
for name,archive_name,canonical in items:
 root=Path('/tmp')/name;archive=Path('/tmp')/archive_name;proof=base/canonical
 selected_worktrees=[w for w in worktrees if w.is_relative_to(root)]
 assert selected_worktrees==([root/'source'] if canonical=='previous-cli-baseline' else [])
 if selected_worktrees:
  assert subprocess.check_output(['git','status','--porcelain'],cwd=root/'source')==b''
  assert subprocess.check_output(['git','rev-parse','HEAD'],cwd=root/'source',text=True).strip().startswith('3d04417')
 meta=json.loads(blob(proof/'archive-verification.json'));receipt=json.loads(blob(proof/'s3-readback.json'))
 manifest_bytes=blob(proof/meta['inventory_file']);inventory=json.loads(manifest_bytes)
 assert meta['schema']==fixture_archive.SCHEMA and meta['all_archive_members_read_back'] and meta['all_current_fixture_files_unchanged_after_capture']
 assert hashlib.sha256(manifest_bytes).hexdigest()==meta['inventory_sha256']
 expected=dict(bytes=meta['archive_bytes'],sha256=meta['archive_sha256'])
 assert receipt['archive']['full_readback']==expected and receipt['canonical_metadata']==str(proof/'archive-verification.json')
 review=json.loads(blob(proof/'independent-review.json'))
 assert review['baseline_all_eight_failures_confirmed' if canonical=='previous-cli-baseline' else 'accepted'] is True
 declared_unit=json.loads(blob(proof/'terminal-unit.json'))
 unit=dict(line.split('=',1) for line in subprocess.check_output(['systemctl','--user','show',root.name+'.service',*['--property='+v for v in declared_unit]],text=True).splitlines())
 assert unit==declared_unit and unit['Result']=='success' and unit['ExecMainStatus']=='0' and unit['MainPID']=='0' and unit['SubState']=='exited'
 if canonical=='previous-cli-baseline':
  assert json.loads((root/'baseline.json').read_text())['expected_baseline_failure_proven'] is True
  qualification='Previous implementation reproduces all eight signal terminations during held protocol handshake'
 else:
  assert json.loads((root/'execution.json').read_text())['exit_code']==0 and json.loads((root/'row-review.json').read_text())['rejection'] is None
  qualification='Accepted focused controlled connection race component' if canonical=='native-race' else 'Accepted original default/domain daemon race regression'
 before=closure(root);assert fixture_archive.inventory(root)==inventory['files']
 print('VERIFY_REMOTE',name,flush=True)
 declared,actual=remote(receipt['archive']['url'],expected);assert declared==inventory
 assert fixture_archive.inventory(root)==inventory['files']
 assert archive.is_file() and not archive.is_symlink() and archive.stat().st_nlink==1
 with archive.open('rb') as f:assert s3.digest(f)==expected
 after=closure(root);archive_closed=closure(archive)
 allocated=sum(p.stat().st_blocks*512 for p in root.rglob('*'))+root.stat().st_blocks*512+archive.stat().st_blocks*512
 record=dict(root=str(root),archive=str(archive),canonical_metadata=str(proof/'archive-verification.json'),receipt=str(proof/'s3-readback.json'),archive_url=receipt['archive']['url'],full_remote_archive_and_every_member_verified=actual,files=len(inventory['files']),local_complete_tree_matches_committed_inventory=True,closure_before=before,closure_immediately_before_removal=after,archive_closure=archive_closed,original_qualification=qualification,removed_allocated_bytes=allocated)
 report['pending_verified_removal']=record;save()
 if selected_worktrees:subprocess.run(['git','worktree','remove',str(root/'source')],cwd=repo,check=True)
 shutil.rmtree(root);assert not root.exists();archive.unlink()
 report.pop('pending_verified_removal',None);report['removed'].append(record);save()
 print('REMOVED',name,allocated,flush=True)
report['removed_allocated_bytes']=sum(x['removed_allocated_bytes'] for x in report['removed'])
report['finished_utc']=datetime.datetime.now(datetime.timezone.utc).isoformat();save()
print('FINISHED',report['removed_allocated_bytes'],report['free_bytes_after'],flush=True)
