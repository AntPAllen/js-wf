import sys,json,hashlib,subprocess,shutil,copy,re,io
from pathlib import Path
sys.dont_write_bytecode=True
repo=Path('/home/exedev/js-wf');root=Path('/tmp/js-wf-authority-runtime-permissions-20261008');out=repo/'docs/scale/authority-runtime-permissions-2026-10-08/accepted'
sys.path.insert(0,str(repo/'scripts'));import fixture_archive
read=lambda p:json.loads(p.read_text());sha=lambda p:hashlib.sha256(p.read_bytes()).hexdigest()
before=read(root/'source-before.json');assert before==read(root/'source-after.json');rev=before['revision'];assert rev=='53fd63936ff9c433d313217d00dae5099c7fbba0'
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
assert commands['run']==[str(root/'blob-race.test'),'-test.run=^(TestAuthoritySubjectAccessRejectsInvalidNamespaces|TestNativeAuthorityRuntimePermissions)$','-test.v','-test.count=1','-test.timeout=3m']
assert 'vcs.revision='+rev in b['build_info'] and 'vcs.modified=false' in b['build_info'] and '-race=true' in b['build_info'] and 'github.com/nats-io/nats-server/v2\tv2.15.0' in b['build_info']
assert a['environment']==dict(GOMAXPROCS='2',GOMEMLIMIT='512MiB',WF_BLOB_AUTHORITY_ROOT=str(root/'blob-stores'))
assert a['working_directory']==str(repo)
def check_log(value):
 assert value.rstrip().endswith('PASS') and not any(s in value for s in ['--- FAIL:','--- SKIP:','DATA RACE'])
 for test in ['TestAuthoritySubjectAccessRejectsInvalidNamespaces','TestNativeAuthorityRuntimePermissions','TestNativeAuthorityRuntimePermissions/R1','TestNativeAuthorityRuntimePermissions/R3']:
  assert len(re.findall(r'^\s*--- PASS: '+re.escape(test)+r' \(',value,re.M))==1
check_log(log)
publish=['wf.blob.authority.root.*','wf.blob.authority.blob.*','$JS.API.STREAM.INFO.BLOB_AUTH','$JS.API.STREAM.MSG.GET.BLOB_AUTH']
denied={'$JS.API.STREAM.'+suffix for suffix in ['UPDATE.BLOB_AUTH','DELETE.BLOB_AUTH','CREATE.BLOB_AUTH','PURGE.BLOB_AUTH','MSG.DELETE.BLOB_AUTH','CREATE.OTHER','INFO.OTHER','MSG.GET.OTHER']}|{'wf.blob.authority.unrecognized.value','wf.inv.type.id'}
controls=[]
def check(d,r):
 assert d['replicas']==r
 assert d['access']==dict(Publish=publish,Subscribe=['_INBOX.>'])
 assert set(d['denied'])==denied
 for subject,error in d['denied'].items():assert error=='nats: permissions violation: Permissions Violation for Publish to "'+subject+'"'
 assert d['restart_denial']==d['denied']['$JS.API.STREAM.DELETE.BLOB_AUTH']
 assert d['retired']==d['after_restart']==dict(Head=2,Token='',Blobs=None,Data=None)
 assert d['provisioner_control_deleted'] is True
 config=d['before']['config'];assert config['name']=='BLOB_AUTH' and config['subjects']==['wf.blob.authority.>'] and config['num_replicas']==r and config['storage']=='file' and config['retention']=='limits' and config['max_msgs_per_subject']==1 and config['max_age']==0 and config['deny_delete'] and config['deny_purge']
 assert d['before']['state']['messages']==2 and d['before']['state']['last_seq']==4
 assert d['keys']==[hashlib.sha256(b'retained').hexdigest()]
 assert d['blob']['Revision']==1 and d['blob']['Fence']['Generation']==1 and d['blob']['Fence']['Phase']=='uploading' and d['blob']['Fence']['Intents']['pending']['Expected']==1
for r in [1,3]:
 p=root/'blob-stores'/('TestNativeAuthorityRuntimePermissions-R'+str(r))/'authority-permissions-proof.json';d=read(p);check(d,r)
 changes=[('wrong_replica',lambda x:x.update(replicas=2)),('broad_publish',lambda x:x['access']['Publish'].append('>')),('broad_subscribe',lambda x:x['access']['Subscribe'].append('>')),('missing_denial',lambda x:x['denied'].pop('$JS.API.STREAM.DELETE.BLOB_AUTH')),('not_permission_error',lambda x:x['denied'].update({'$JS.API.STREAM.DELETE.BLOB_AUTH':'timeout'})),('restart_denial_missing',lambda x:x.update(restart_denial='')),('head_reset',lambda x:x['retired'].update(Head=0)),('restart_regression',lambda x:x['after_restart'].update(Head=1)),('no_privileged_control',lambda x:x.update(provisioner_control_deleted=False)),('unsafe_retention',lambda x:x['before']['config'].update(max_age=1)),('physical_reset',lambda x:x['before']['state'].update(last_seq=0)),('missing_census',lambda x:x.update(keys=[])),('generation_reset',lambda x:x['blob']['Fence'].update(Generation=0))]
 for name,change in changes:
  bad=copy.deepcopy(d);change(bad)
  try:check(bad,r)
  except (AssertionError,KeyError):controls.append('R'+str(r)+'/'+name)
  else:raise AssertionError('accepted '+name)
for name,bad in [('missing_R3',log.replace('--- PASS: TestNativeAuthorityRuntimePermissions/R3','--- OMIT: TestNativeAuthorityRuntimePermissions/R3')),('race_report',log+'\nDATA RACE\n')]:
 try:check_log(bad)
 except AssertionError:controls.append(name)
 else:raise AssertionError('bad log accepted')
staging=root.with_name(root.name+'-proof');meta=read(staging/'archive-verification.json');inv=read(staging/'fixture-inventory.json');assert fixture_archive.inventory(root)==inv['files']
with root.with_suffix('.tar.gz').open('rb') as f:declared,actual=fixture_archive.verify_hashed_stream(f,dict(bytes=meta['archive_bytes'],sha256=meta['archive_sha256']))
assert declared==inv
out.mkdir(parents=True)
for n in ['source-before.json','source-after.json','blob-actual-sdk.json','blob-binary.json','blob-execution.json','blob-commands.json','blob-actual.log','executed-producer.py','closure.json']:shutil.copy2(root/n,out/n)
for n in ['archive-verification.json','fixture-inventory.json']:shutil.copy2(staging/n,out/n)
for r in [1,3]:shutil.copy2(root/'blob-stores'/('TestNativeAuthorityRuntimePermissions-R'+str(r))/'authority-permissions-proof.json',out/('R'+str(r)+'-permissions-proof.json'))
shutil.copy2(__file__,out/'executed-review.py')
report=dict(source=rev,source_inputs=len(selected),actual_sdk=a['pid'],execution=e,archive=actual,actual_positive_mutations_rejected=controls,scope='Named trusted native adapter credentials, actual embedded R1/R3 permissions/restart cases at frozen source. Embedded SDK build/version is bound; no independent per-peer varz/process identity ledger or hermetic module cache provenance. Not administrator ownership, ObjectStore/account-import/domain policy, runtime migration, full126 graph, full native/release/online GC acceptance.')
(out/'review.json').write_text(json.dumps(report,indent=2)+'\n');print(json.dumps(dict(source_inputs=len(selected),mutations=len(controls),execution=e)),flush=True)
