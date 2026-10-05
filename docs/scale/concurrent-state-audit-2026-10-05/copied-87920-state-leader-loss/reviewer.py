from pathlib import Path
import json,hashlib,subprocess,tarfile
r=Path('/tmp/js-wf-concurrent-state-87920-leader-loss-copied-20261005')
def read(n):return json.loads((r/n).read_text())
def sha(p):return hashlib.file_digest(p.open('rb'),'sha256').hexdigest()
e=read('execution.json');assert e['status']=='passed' and e['exit_code']==0 and e['live_servers_captured']
assert sha(r/'integrity.test')==e['sha256'] and not Path('/proc/'+str(e['pid'])).exists()
b=read('source-before.json');assert b==read('source-after.json') and b['revision']==e['source']
for n,h in b['files'].items():assert sha(r/'source'/n)==h==hashlib.sha256(subprocess.check_output(['git','show',e['source']+':'+n])).hexdigest()
servers=read('actual-containers/actual-servers.json');assert len(servers)==5
for i,x in enumerate(servers):
 assert sha(r/f'actual-containers/server-{i}')==x['sha256'] and not Path('/proc/'+str(x['host_pid'])).exists()
 assert '\tmod\tgithub.com/nats-io/nats-server/v2\tv2.15.0\t' in x['actual_proc_build_info']
 assert any(m['Source'].startswith(str(r/'copied-stores')) and m['Destination']=='/data' for m in x['container']['Mounts'])
copy=read('original-to-copy-verification.json');after=read('original-after-verification.json');assert copy['all_original_and_copy_bytes_match_verified_archive'] and after['all_original_store_bytes_still_match'] and len(copy['files'])==after['files']
original=Path(copy['original']);assert sha(original/'originals.tar.gz')==copy['original_archive_sha256']
verified=set()
with tarfile.open(original/'originals.tar.gz','r|gz') as t:
 for m in t:
  n=m.name.removeprefix('./')
  if n not in copy['files']:continue
  assert hashlib.file_digest(t.extractfile(m),'sha256').hexdigest()==copy['files'][n]==sha(original/n);verified.add(n)
assert verified==set(copy['files'])
p=next((r/'originals').rglob('state-watch-leader-loss.json'));proof=json.loads(p.read_text());fault=proof['fault']
assert proof['error']=='<nil>' and proof['elapsed_ns']<20_000_000_000 and proof['report']=={'Invocations':87920,'Journals':87920,'Entries':969925,'Terminal':87920}
assert fault['triggered'] and fault['node']==3 and fault['injection_error']==''
assert fault['consumer']['num_pending']==86144 and fault['consumer']['cluster']['leader'].endswith('-n3')
kill=fault['kill'];assert kill['node']==3 and kill['state']=='dead' and kill['concurrent_observation'] and kill['source_stopped']>kill['kill_started']
assert len(proof['attempts'])==2 and not proof['attempts'][0]['complete'] and proof['attempts'][1]['complete'] and proof['attempts'][1]['records']==88068
log=(p.parent/'server-3-before-kill.log');assert log.is_file()
review={'source':e['source'],'actual_sdk_pid':e['pid'],'actual_sdk_sha256':e['sha256'],'selected_git_inputs':len(b['files']),'observed_servers':5,'original_store_files_unchanged':len(verified),'cohort_report':proof['report'],'elapsed_ns':proof['elapsed_ns'],'fault':kill,'watch_pending_at_kill':86144,'watch_attempts':proof['attempts'],'scope':'Complete copied87,920 cohort concurrent audit with observed pending state-watch leader SIGKILL under original20s limit; no writers, no original24h qualification or candidate adoption.'}
(r/'independent-review.json').write_text(json.dumps(review,indent=2)+'\n');print(json.dumps(review))
