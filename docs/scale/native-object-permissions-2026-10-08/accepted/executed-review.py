import sys,json,hashlib,subprocess,shutil,copy,re,io
from pathlib import Path
sys.dont_write_bytecode=True
repo=Path('/home/exedev/js-wf');root=Path('/tmp/js-wf-native-object-permissions-20261008');out=repo/'docs/scale/native-object-permissions-2026-10-08/accepted'
sys.path.insert(0,str(repo/'scripts'));import fixture_archive
read=lambda p:json.loads(p.read_text());sha=lambda p:hashlib.sha256(p.read_bytes()).hexdigest()
before=read(root/'source-before.json');assert before==read(root/'source-after.json');rev=before['revision'];assert rev=='80822a65d5ab823f31d84da8a1d716799f19b739'
names=subprocess.check_output(['git','ls-tree','-r','--name-only',rev],cwd=repo,text=True).splitlines();selected=[n for n in names if n.endswith(('.go','.py','.yml')) or n in ('go.mod','go.sum') or n.startswith('sim/testdata/')];assert set(selected)==set(before['files'])
stream=io.BytesIO(subprocess.check_output(['git','cat-file','--batch'],input=''.join(rev+':'+n+'\n' for n in selected).encode(),cwd=repo))
for n in selected:
 header=stream.readline().split();assert header[1]==b'blob';data=stream.read(int(header[2]));assert stream.read(1)==b'\n'
 p=root/'selected-source'/n
 if p.suffix=='.go':p=p.with_suffix('.go.txt')
 assert hashlib.sha256(data).hexdigest()==before['files'][n]==sha(p)
assert not stream.read()
a=read(root/'blob-actual-sdk.json');b=read(root/'blob-binary.json');e=read(root/'blob-execution.json');commands=read(root/'blob-commands.json');log=(root/'blob-actual.log').read_text()
assert e['source']==rev and e['exit_code']==0 and a['args']==commands['run'] and a['exe_sha256']==b['sha256']==sha(root/'blob-race.test') and a['admission']['stable_identity_observed_twice'] and not Path('/proc',str(a['pid'])).exists()
assert commands['run']==[str(root/'blob-race.test'),'-test.run=^(TestAuthoritySubjectAccessRejectsInvalidNamespaces|TestNativeObjectRuntimePermissions)$','-test.v','-test.count=1','-test.timeout=3m']
assert 'vcs.revision='+rev in b['build_info'] and 'vcs.modified=false' in b['build_info'] and '-race=true' in b['build_info'] and 'github.com/nats-io/nats-server/v2\tv2.15.0' in b['build_info']
assert a['environment']==dict(GOMAXPROCS='2',GOMEMLIMIT='512MiB',WF_BLOB_AUTHORITY_ROOT=str(root/'blob-stores'))
assert a['working_directory']==str(repo)
def check_log(value):
 assert value.rstrip().endswith('PASS') and not any(s in value for s in ['--- FAIL:','--- SKIP:','DATA RACE'])
 for test in ['TestAuthoritySubjectAccessRejectsInvalidNamespaces','TestNativeObjectRuntimePermissions','TestNativeObjectRuntimePermissions/R1','TestNativeObjectRuntimePermissions/R3']:
  assert len(re.findall(r'^\s*--- PASS: '+re.escape(test)+r' \(',value,re.M))==1
check_log(log)
publish=['wf.blob.authority.root.*','wf.blob.authority.blob.*','$JS.API.STREAM.INFO.BLOB_AUTH','$JS.API.STREAM.MSG.GET.BLOB_AUTH','$O.PROTOCOL.C.*','$O.PROTOCOL.M.*','$JS.API.STREAM.INFO.OBJ_PROTOCOL','$JS.API.STREAM.MSG.GET.OBJ_PROTOCOL','$JS.API.CONSUMER.CREATE.OBJ_PROTOCOL.*.$O.PROTOCOL.C.*','$JS.FC.OBJ_PROTOCOL.>','$JS.API.CONSUMER.INFO.OBJ_PROTOCOL.*','$JS.API.CONSUMER.DELETE.OBJ_PROTOCOL.*']
subjects=['$JS.API.STREAM.DELETE.OBJ_PROTOCOL','$JS.API.STREAM.UPDATE.OBJ_PROTOCOL','$JS.API.STREAM.CREATE.OBJ_PROTOCOL','$JS.API.STREAM.PURGE.BLOB_AUTH','$JS.API.STREAM.MSG.DELETE.OBJ_PROTOCOL','$JS.API.STREAM.PURGE.OBJ_OTHER','$JS.API.STREAM.INFO.OBJ_OTHER','$O.OTHER.C.attempt','$JS.API.CONSUMER.CREATE.WF_INV.reader.wf.inv.type.id']
denied={role+':'+subject for role in ['publisher','collector'] for subject in subjects}|{'publisher:$JS.API.STREAM.PURGE.OBJ_PROTOCOL'}
controls=[]
def check(d,r):
 assert d['replicas']==r
 assert d['publisher']==dict(Publish=publish,Subscribe=['_INBOX.>'])
 assert d['collector']==dict(Publish=publish+['$JS.API.STREAM.PURGE.OBJ_PROTOCOL'],Subscribe=['_INBOX.>'])
 assert set(d['denied'])==denied
 for name,error in d['denied'].items():
  subject=name.split(':',1)[1];assert error=='nats: permissions violation: Permissions Violation for Publish to "'+subject+'"'
 assert d['bytes_read']==320000 and d['live_sweep_deleted']==0 and d['retired_sweep_deleted']==1 and d['remaining']==[]
 root=d['published'];assert root['Head']==1 and len(root['Token'])==32 and root['Data']=='Y2Fub25pY2Fs'
 digest=hashlib.sha256(b'native-role-data'*20000).hexdigest();assert set(root['Blobs'])=={digest};ref=root['Blobs'][digest]
 assert ref['Generation']==1 and ref['Object'].startswith(digest+'/1/')
 tomb=d['tombstone'];assert tomb['name']==ref['Object'] and tomb['bucket']=='PROTOCOL' and tomb['deleted'] and tomb['size']==0 and tomb['chunks']==0 and d['tombstone_sequence']>0
for r in [1,3]:
 p=root/'blob-stores'/('TestNativeObjectRuntimePermissions-R'+str(r))/'object-permissions-proof.json';d=read(p);check(d,r)
 changes=[('wrong_replica',lambda x:x.update(replicas=2)),('publisher_purge',lambda x:x['publisher']['Publish'].append('$JS.API.STREAM.PURGE.OBJ_PROTOCOL')),('collector_authority_purge',lambda x:x['collector']['Publish'].append('$JS.API.STREAM.PURGE.BLOB_AUTH')),('broad_read',lambda x:x['publisher']['Subscribe'].append('>')),('missing_denial',lambda x:x['denied'].pop('publisher:$JS.API.STREAM.PURGE.OBJ_PROTOCOL')),('not_permission_error',lambda x:x['denied'].update({'publisher:$JS.API.STREAM.PURGE.OBJ_PROTOCOL':'timeout'})),('short_read',lambda x:x.update(bytes_read=1)),('live_deleted',lambda x:x.update(live_sweep_deleted=1)),('retired_not_deleted',lambda x:x.update(retired_sweep_deleted=0)),('remaining_object',lambda x:x.update(remaining=[{}])),('head_reset',lambda x:x['published'].update(Head=0)),('tombstone_missing',lambda x:x['tombstone'].update(deleted=False)),('tombstone_reused',lambda x:x['tombstone'].update(name='other')),('tombstone_no_ack',lambda x:x.update(tombstone_sequence=0))]
 for name,change in changes:
  bad=copy.deepcopy(d);change(bad)
  try:check(bad,r)
  except (AssertionError,KeyError):controls.append('R'+str(r)+'/'+name)
  else:raise AssertionError('accepted '+name)
for name,bad in [('missing_R3',log.replace('--- PASS: TestNativeObjectRuntimePermissions/R3','--- OMIT: TestNativeObjectRuntimePermissions/R3')),('race_report',log+'\nDATA RACE\n')]:
 try:check_log(bad)
 except AssertionError:controls.append(name)
 else:raise AssertionError('bad log accepted')
staging=root.with_name(root.name+'-proof');meta=read(staging/'archive-verification.json');inv=read(staging/'fixture-inventory.json');assert fixture_archive.inventory(root)==inv['files']
with root.with_suffix('.tar.gz').open('rb') as f:declared,actual=fixture_archive.verify_hashed_stream(f,dict(bytes=meta['archive_bytes'],sha256=meta['archive_sha256']))
assert declared==inv
out.mkdir(parents=True)
for n in ['source-before.json','source-after.json','blob-actual-sdk.json','blob-binary.json','blob-execution.json','blob-commands.json','blob-actual.log','executed-producer.py','closure.json']:shutil.copy2(root/n,out/n)
for n in ['archive-verification.json','fixture-inventory.json']:shutil.copy2(staging/n,out/n)
for r in [1,3]:shutil.copy2(root/'blob-stores'/('TestNativeObjectRuntimePermissions-R'+str(r))/'object-permissions-proof.json',out/('R'+str(r)+'-permissions-proof.json'))
shutil.copy2(__file__,out/'executed-review.py')
report=dict(source=rev,source_inputs=len(selected),actual_sdk=a['pid'],execution=e,archive=actual,actual_positive_mutations_rejected=controls,scope='Trusted native publisher/collector role permissions and actual embedded R1/R3 publication/read/live-preservation/retirement cases at frozen source. Collector purge payload restriction is not enforced by subject permissions. Embedded SDK build/version is bound; no independent per-peer varz/process identity ledger or hermetic module cache provenance. Not administrator ownership, ObjectStore/account-import/domain policy, runtime migration, full126 graph, full native/release/online GC acceptance.')
(out/'review.json').write_text(json.dumps(report,indent=2)+'\n');print(json.dumps(dict(source_inputs=len(selected),mutations=len(controls),execution=e)),flush=True)
