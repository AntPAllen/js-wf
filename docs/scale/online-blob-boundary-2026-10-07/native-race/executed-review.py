import sys
sys.dont_write_bytecode=True
from pathlib import Path
import json,hashlib,subprocess,importlib.util,io,re,shutil
repo=Path('/home/exedev/js-wf');root=Path('/tmp/js-wf-online-blob-native-20261007');meta_root=Path(str(root)+'-proof');out=Path('/tmp/js-wf-online-blob-native-independent-20261007')
sys.path.insert(0,str(repo/'scripts'));import fixture_archive
spec=importlib.util.spec_from_file_location('shared',repo/'scripts/run-domain-runtime-controls.py');shared=importlib.util.module_from_spec(spec);spec.loader.exec_module(shared)
spec=importlib.util.spec_from_file_location('native',repo/'scripts/run-blob-boundary-controls.py');native=importlib.util.module_from_spec(spec);spec.loader.exec_module(native)
read=lambda p:json.loads(p.read_text())
unit=subprocess.check_output(['systemctl','show',root.name+'.service','-p','MainPID','-p','SubState','-p','ExecMainStatus'],text=True);assert 'MainPID=0\n' in unit and 'SubState=exited\n' in unit and 'ExecMainStatus=0\n' in unit
execution=read(root/'execution.json');before=read(root/'source-before.json');rev=execution['source'];assert execution['exit_code']==0 and before==read(root/'source-after.json') and before['revision']==rev
names=subprocess.check_output(['git','ls-tree','-r','--name-only',rev],cwd=repo,text=True).splitlines();expected=[name for name in names if name.endswith(('.go','.py','.yml')) or name in ('go.mod','go.sum') or name.startswith('sim/testdata/')];assert set(expected)==set(before['files'])
blobs=io.BytesIO(subprocess.check_output(['git','cat-file','--batch'],cwd=repo,input=''.join(rev+':'+name+'\n' for name in expected).encode()))
for name in expected:
 header=blobs.readline().split();assert header[1]==b'blob';data=blobs.read(int(header[2]));assert blobs.read(1)==b'\n';assert hashlib.sha256(data).hexdigest()==before['files'][name]==shared.sha(root/'selected-source'/name)
assert not blobs.read()
sdk=read(root/'actual-sdk.json');binary=read(root/'binary.json');commands=read(root/'commands.json')
assert sdk['exe_sha256']==binary['sha256']==shared.sha(root/'retention-race.test') and sdk['args']==commands['run'] and sdk['working_directory']==str(repo)
assert sdk['environment']==dict(GOMAXPROCS='2',GOMEMLIMIT='1GiB',WF_BLOB_BOUNDARY_ROOT=str(root/'stores/scenario')) and sdk['admission']['stable_identity_observed_twice']
assert '-race=true' in binary['build_info'] and 'vcs.revision='+rev in binary['build_info'] and 'vcs.modified=false' in binary['build_info'] and '\tdep\tgithub.com/nats-io/nats-server/v2\tv2.15.0' in binary['build_info']
assert not Path('/proc',str(sdk['pid'])).exists()
log=(root/'native.log').read_text();qualification=native.verify_log(log);assert read(root/'row-review.json')==dict(qualification=qualification,rejection=None)
proofs={mode:read(root/'stores/scenario'/mode/'boundary-proof.json') for mode in ('quiescent','refresh_after_census')}
assert len({proof['server_id'] for proof in proofs.values()})==2
for mode,proof in proofs.items():
 assert proof['mode']==mode and proof['old_nuid']!=proof['fresh_nuid'] and proof['object']==proof['acknowledged_reference'] and proof['invocation_sequence']>0
 expected_sweep=dict(objects=1,referenced=1 if mode=='quiescent' else 0,eligible=0 if mode=='quiescent' else 1,deleted=0 if mode=='quiescent' else 1)
 assert proof['sweep']==expected_sweep and proof['dangling']==(mode!='quiescent')
 assert f"mode={mode} old_nuid={proof['old_nuid']} fresh_nuid={proof['fresh_nuid']} acked_ref=true deleted={expected_sweep['deleted']} dangling={str(proof['dangling']).lower()} server_id={proof['server_id']}" in log
bad=[log.replace('deleted=1 dangling=true','deleted=0 dangling=false'),log.replace('deleted=0 dangling=false','deleted=1 dangling=true'),log.replace('acked_ref=true','acked_ref=false'),log.replace('--- PASS: '+native.TEST+'/quiescent','--- SKIP: '+native.TEST+'/quiescent')]
for mode,proof in proofs.items():
 bad.append(log.replace('fresh_nuid='+proof['fresh_nuid'],'fresh_nuid='+proof['old_nuid']))
line=next(line for line in log.splitlines() if 'mode=refresh_after_census' in line);bad.extend((log.replace(line,''),log+'\n'+line))
for altered in bad:
 try:native.verify_log(altered)
 except AssertionError:pass
 else:raise AssertionError('actual native-log mutation accepted')
meta=read(meta_root/'archive-verification.json');inventory=read(meta_root/'fixture-inventory.json')
with root.with_suffix('.tar.gz').open('rb') as stream:
 declared,compressed=fixture_archive.verify_hashed_stream(stream,dict(bytes=meta['archive_bytes'],sha256=meta['archive_sha256']))
assert declared==inventory and fixture_archive.inventory(root)==inventory['files'];closure=shared.closure(root)
out.mkdir();shutil.copyfile(__file__,out/'executed-review.py')
report=dict(accepted_contract=True,online_gc_safe=False,source=rev,source_files=len(expected),actual_sdk=sdk,boundary_proofs=proofs,rejected_actual_log_mutations=len(bad),archive_files=len(inventory['files']),complete_archive=compressed,fresh_closure=closure,original_stores_not_reopened=True,scope='Controlled R1 embedded-library refresh-after-census violation and safe quiescent control; SDK and libraries race instrumented. No active-writer collection protocol implemented or broad release acceptance.')
(out/'independent-review.json').write_text(json.dumps(report,indent=2)+'\n');print('BLOB_BOUNDARY_CONTRACT_ACCEPTED',rev,sdk['pid'],len(inventory['files']),len(bad),flush=True)
