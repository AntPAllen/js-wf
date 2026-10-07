from storage_review_common import *
root=Path('/tmp/js-wf-checkpoint4160-creation-stall-20261007')
out=Path('/tmp/checkpoint4160-creation-setup-failure-closed-proof-20261007');out.mkdir()
unit=subprocess.check_output(['systemctl','--user','show','js-wf-checkpoint4160-creation-stall-20261007.service','-p','MainPID','-p','ExecMainPID','-p','ExecMainStatus','-p','Result','-p','InvocationID'],text=True)
f=dict(line.split('=',1) for line in unit.splitlines());assert f==dict(MainPID='0',ExecMainPID='2555639',ExecMainStatus='2',Result='exit-code',InvocationID='433dc0e0b313488899a825edf8bc48d7')
assert not subprocess.check_output(['docker','ps','-aq','--filter','name=js-wf-route-2559092-']).strip()
assert not Path('/proc/2559092').exists()
record=dict(original_unit=unit,native_execution=json.loads((root/'execution.json').read_text()),closure=closure(root),setup_failure=True,native_creation_reached=False,qualifies_24h=False,scope='Original SDK6m timeout after fixture borrowed metadata2s deadline and invoked t.Fatal in snapshot goroutine. Five leftover owned containers explicitly stopped; Docker auto-remove races preserved. Initial producer archive was made with live mounts and is retained only as a nested historical artifact; closed full archive supersedes it.')
assert record['native_execution']['exit_code']==2 and record['native_execution']['source']=='74719d7b4b0460b2a3f8be19ddf756b4a39bccdf'
before=json.loads((root/'source-before.json').read_text());assert before==json.loads((root/'source-after.json').read_text())
for name,digest in before['files'].items():
 body=subprocess.check_output(['git','cat-file','blob',before['revision']+':'+name],cwd=repo)
 assert hashlib.sha256(body).hexdigest()==digest
 assert hashlib.sha256((root/'selected-source'/name).read_bytes()).hexdigest()==digest
record['selected_original_git_inputs']=len(before['files'])
external=json.loads((root/'external-source-before.json').read_text());assert external==json.loads((root/'external-source-after.json').read_text());paths=json.loads((root/'external-captured-paths.json').read_text());assert set(paths)==set(external)
for name,digest in external.items():
 with (root/paths[name]).open('rb') as stream:assert hashlib.file_digest(stream,'sha256').hexdigest()==digest
record['selected_external_inputs']=len(external)
log=(root/'native.log').read_text();assert 'native creation changed the full initial-set deadline' in log and 'test timed out after 6m0s' in log and 'snapshotResults' not in log
assert not list((root/'originals').rglob('copied-checkpoint-audit.json'))
sdk=json.loads((root/'actual-sdk.json').read_text());binary=json.loads((root/'binary.json').read_text());assert sdk['pid']==2559092 and sdk['exe_sha256']==binary['sha256']
with (root/'integrity.test').open('rb') as stream:assert hashlib.file_digest(stream,'sha256').hexdigest()==binary['sha256']
assert 'vcs.revision='+before['revision'] in binary['build_info'] and 'vcs.modified=false' in binary['build_info']
record['sdk']=sdk;record['original_native_server_observations']=json.loads((root/'actual-servers.json').read_text())
shutil.copytree('/tmp/checkpoint4160-creation-failed-container-closure-20261007',root/'manual-container-closure')
shutil.copyfile('/tmp/close-checkpoint4160-creation-fixture-20261007.py',root/'manual-container-closure/executed-second.py')
(root/'manual-container-closure/auto-remove-errors.txt').write_text('Initial stop succeeded; subsequent docker inspect returned no such object after AutoRemove. Resumed stop sequence succeeded for all remaining four containers; last inspect succeeded before AutoRemove then docker rm returned No such container. Final docker ps -aq name=js-wf-route-2559092- is empty. Both failed cleanup script versions and partial ledger/logs retained; no invented terminal exit status for auto-removed containers.\n')
oldproof=Path(str(root)+'-proof');shutil.copytree(oldproof,root/'initial-live-container-archive-proof')
oldarchive=root.with_suffix('.tar.gz');shutil.move(oldarchive,root/'initial-live-container-archive.tar.gz')
(root/'closed-setup-failure-review.json').write_text(json.dumps(record,indent=2)+'\n')
shutil.copyfile(__file__,root/'executed-closed-preserve.py');shutil.copyfile('/tmp/storage_review_common.py',root/'executed-closed-common.py')
archive=Path('/tmp/js-wf-checkpoint4160-creation-stall-closed-20261007.tar.gz')
meta=fixture_archive.capture(root,archive,out,compresslevel=1)
(out/'independent-failure-review.json').write_text(json.dumps(record,indent=2)+'\n')
shutil.copyfile(__file__,out/'executed-closed-preserve.py');shutil.copyfile('/tmp/storage_review_common.py',out/'executed-closed-common.py')
print('CLOSED_FULL_FAILURE_PRESERVED',json.dumps(meta),flush=True)
