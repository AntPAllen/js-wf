from pathlib import Path
import json,hashlib,subprocess,sys,base64,re
import tarfile,tempfile,shutil
archive_root=Path(sys.argv[1]).resolve();repo=Path(sys.argv[2]).resolve()
manifest=json.loads((archive_root/'start-upgrade-gap-manifest.json').read_text())
terminal=json.loads((archive_root/'terminal.json').read_text())
assert terminal['status']=='completed' and terminal['conclusion']=='success'
expected=manifest['files'];actual={}
workspace=tempfile.TemporaryDirectory(prefix='start-upgrade-review-');r=Path(workspace.name)
with tarfile.open(archive_root/'start-upgrade-gap-originals.tar.gz','r:gz') as archive:
 for member in archive:
  name=member.name
  assert member.isfile() and name not in actual and not Path(name).is_absolute() and '..' not in Path(name).parts
  data=archive.extractfile(member);digest=hashlib.sha256()
  keep=name.endswith(('.json','.jsonl','.go.txt'))
  target=r/name
  if keep:target.parent.mkdir(parents=True,exist_ok=True)
  dest=target.open('wb') if keep else None
  try:
   while chunk:=data.read(1024*1024):
    digest.update(chunk)
    if dest:dest.write(chunk)
  finally:
   if dest:dest.close()
  actual[name]=digest.hexdigest()
assert actual==expected and manifest['every_member_sha256_readback']
read=lambda p:json.loads(p.read_text())
b=read(r/'source-before.json');a=read(r/'source-after.json');assert a==b and b['revision']==terminal['headSha']
for n,h in b['files'].items():assert hashlib.sha256(subprocess.check_output(['git','show',b['revision']+':'+n],cwd=repo)).hexdigest()==h
source=subprocess.check_output(['git','show',b['revision']+':reconcile/starts.go'],cwd=repo,text=True);needle='return p.client.Enqueue(ctx, typ, id, "start:"+typ+"."+id+":"+strconv.FormatUint(sequence, 10))'
assert (r/'skip-gap-repair.go.txt').read_text()==source.replace(needle,'if typ == "mixed-upgrade" && strings.HasPrefix(id, "gap-upgrade-") { return nil }\n\t'+needle)
# Read actual embedded build settings from each preserved binary, one at a time.
with tarfile.open(archive_root/'start-upgrade-gap-originals.tar.gz','r:gz') as archive:
 for mode in ('positive','skip-repair'):
  target=r/'retained.test'
  with target.open('wb') as dest:shutil.copyfileobj(archive.extractfile(mode+'/integration.test'),dest)
  info=read(r/mode/'binary.json')
  actual_info=subprocess.check_output(['go','version','-m',str(target)],text=True)
  assert actual_info.splitlines()[1:]==info['build_info'].splitlines()[1:]
  assert '-race=true' in actual_info
  target.unlink()
test='TestMixedVersionRollingUpgradeFallback';cases=[];recoveries=[]
for mode,want in [('positive','pass'),('skip-repair','fail')]:
 e=[json.loads(l) for l in (r/mode/'events.jsonl').read_text().splitlines()]
 assert not any(x['Action'] in ('skip','build-fail') for x in e)
 assert [x['Action'] for x in e if x.get('Test')==test and x['Action'] in ('pass','fail')]==[want]
 assert [x['Action'] for x in e if not x.get('Test') and x['Action'] in ('pass','fail')]==[want]
 output=''.join(x.get('Output','') for x in e);assert 'panic:' not in output and 'timed out' not in output
 info=read(r/mode/'binary.json');assert actual[mode+'/integration.test']==info['sha256'] and '-race=true' in info['build_info']
 profiles=['old-peer-first','auto-fallback-on-new-peer'] if mode=='positive' else ['old-peer-first']
 for profile in profiles:
  gap=r/mode/'artifacts'/profile/'start-gap';p=read(gap/'killed-gap.json');receipt=p['receipt'];inv=p['retained']
  assert p['signal']=='SIGKILL' and 'killed' in p['wait_error'] and receipt['pid']>0 and receipt['version']=='2.11.17' and receipt['server_id']
  assert p['journal_absent'] and p['run_messages']==0 and receipt['invocation']==inv
  assert inv['Sequence']>0 and base64.b64decode(inv['Data'])==b'null'
  sha=[v for k,v in inv['Header'].items() if k.lower()=='wf-input-sha256'];assert sha==[[hashlib.sha256(b'null').hexdigest()]]
  assert receipt['invoked']<=receipt['at']<=p['kill_started']<=p['killed']
  assert read(gap/'after-upgrade.json')==inv
  if mode=='positive':
   t=read(gap/'terminal.json');entry=json.loads(base64.b64decode(t['Data']));assert entry['kind']=='Completed' and entry['payload']['inv_seq']==inv['Sequence'] and base64.b64decode(entry['payload']['result'])==b'"done"'
   log=''.join(x.get('Output','') for x in e if x.get('Test')==test+'/'+profile)
   m=re.search(r'START_UPGRADE_GAP_REPAIRED seq=(\d+) terminal_seq=(\d+) result="done" recovery=([0-9.]+)s',log);assert m and int(m[1])==inv['Sequence'] and int(m[2])==t['Sequence'] and 0<float(m[3])<30
   recoveries.append(dict(profile=profile,kill_to_verified_terminal_seconds=float(m[3])))
 if mode=='skip-repair':assert output.count('post-upgrade process-gap repair produced no dispatch or journal')==1
 cases.append(dict(mode=mode,verdict=want,package_seconds=next(x['Elapsed'] for x in e if not x.get('Test') and x['Action']==want)))
review=dict(accepted=True,source=b['revision'],source_files_verified=len(b['files']),cases=cases,recoveries=recoveries,scope='R3 mixed-version actual Start process crash, retained-store upgrade, production scan repair and30s recovery; sustained R5 gap coverage/200 seeds/24h remain open.')
review['archive_members_verified']=len(actual)
(archive_root/'independent-review.json').write_text(json.dumps(review,indent=2)+'\n');print(json.dumps(review,indent=2))
