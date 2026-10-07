import sys,json,subprocess,hashlib,shutil,io,importlib.util
from pathlib import Path
sys.dont_write_bytecode=True
repo=Path('/home/exedev/js-wf');sys.path.insert(0,str(repo/'scripts'));import fixture_archive
spec=importlib.util.spec_from_file_location('shared',repo/'scripts/run-domain-runtime-controls.py');shared=importlib.util.module_from_spec(spec);spec.loader.exec_module(shared)
root=Path('/tmp/js-wf-postgres-leaf-sql-startup-sigint-50000-20261007');proof=root.with_name(root.name+'-proof')
unit=dict(row.split('=',1) for row in subprocess.check_output(['systemctl','show',root.name+'.service','--property=LoadState,ActiveState,SubState,MainPID,ExecMainPID,ExecMainStatus,Result,InvocationID'],text=True).splitlines())
assert unit['LoadState']=='loaded' and unit['MainPID']=='0' and unit['ExecMainStatus']=='1' and unit['Result']=='exit-code'
e=json.loads((root/'execution.json').read_text());assert e['exit_code']==1 and e['status']=='failed'
before=json.loads((root/'source-before.json').read_text());assert before==json.loads((root/'source-after.json').read_text()) and before['revision']==e['source']
stream=io.BytesIO(subprocess.check_output(['git','cat-file','--batch'],cwd=repo,input=''.join(e['source']+':'+n+'\n' for n in before['files']).encode()))
for n,h in before['files'].items():
 header=stream.readline().split();assert header[1]==b'blob';b=stream.read(int(header[2]));assert stream.read(1)==b'\n';assert hashlib.sha256(b).hexdigest()==h==shared.sha(root/'selected-source'/n)==shared.sha(repo/n)
assert not stream.read()
ext=json.loads((root/'external-source-before.json').read_text());assert ext==json.loads((root/'external-source-after.json').read_text());paths=json.loads((root/'external-captured-paths.json').read_text())
for p,h in ext.items():assert shared.sha(root/paths[p])==h==shared.sha(p)
sdk=json.loads((root/'actual-sdk.json').read_text());binary=json.loads((root/'binary.json').read_text());assert sdk['exe_sha256']==binary['sha256']==shared.sha(root/'integration.test')
assert 'vcs.revision='+e['source'] in binary['build_info'] and 'vcs.modified=false' in binary['build_info'] and '-race=true' not in binary['build_info']
assert sdk['stat'].split(') ',1)[1].split()[1]==unit['ExecMainPID'] and not Path('/proc',str(sdk['pid'])).exists()
log=(root/'native.log').read_text();assert 'SQL startup residual sessions=1 err=<nil>' in log and log.rstrip().endswith('FAIL') and '--- PASS:' not in log
case=next((root/'originals').rglob('projection-fault-proof.json'));p=json.loads(case.read_text());assert p['native_test_failed'] and p['count']==50000 and 'sql_startup_cancellation' not in p
stopped=json.loads((root/'postgres-stopped.json').read_text());assert not stopped['State']['Running'] and stopped['State']['Pid']==0
media=json.loads((root/'postgres-media-copy-verification.json').read_text());assert media['all_closed_sql_media_bytes_match']
for n,h in media['files'].items():assert shared.sha(root/'postgres-stopped-data'/n)==h
meta=json.loads((proof/'archive-verification.json').read_text());inv=json.loads((proof/'fixture-inventory.json').read_text());assert fixture_archive.inventory(root)==inv['files'] and fixture_archive.verify(root.with_suffix('.tar.gz'))==inv
with root.with_suffix('.tar.gz').open('rb') as f:assert fixture_archive.digest(f)=={'bytes':meta['archive_bytes'],'sha256':meta['archive_sha256']}
closure=shared.closure(root)
out=repo/'docs/scale/postgres-leaf-sql-startup-sigint-50000-2026-10-07/initial-failure';out.mkdir()
for n in ['execution.json','actual-sdk.json','binary.json','commands.json','native.log','projector-profile.json','source-before.json','source-after.json','external-source-before.json','external-source-after.json','postgres-stopped.json','postgres-media-copy-verification.json']:shutil.copyfile(root/n,out/n)
for n in ['archive-verification.json','fixture-inventory.json']:shutil.copyfile(proof/n,out/n)
shutil.copyfile(case,out/'projection-fault-proof.json');shutil.copyfile(__file__,out/'executed-review.py')
(out/'independent-review.json').write_text(json.dumps({'source':e['source'],'unit':unit,'actual_sdk_pid':sdk['pid'],'git_inputs':len(before['files']),'external_inputs':len(ext),'sql_media_files':len(media['files']),'archive':meta,'closure':closure,'verdict':'Failed: SQL startup residual session count1 after child join, before blocker release and before full50000 workload. Retained source checks preceding assertions imply observed child exit0, but no complete startup boundary record was returned. Remote cleanup latency/cause is unconfirmed. No recovery or full50000 acceptance.'},indent=2)+'\n')
print('FAILED_SQL_STARTUP_NATIVE_PRESERVED',len(before['files']),len(ext),len(media['files']),meta['members'],flush=True)
