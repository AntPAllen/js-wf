"""Check terminal component evidence, unchanged inputs, and preserved failure scope."""
import gzip, hashlib, json, pathlib, re, subprocess
base=pathlib.Path(__file__).resolve().parent
repo=base.parents[2]
before=json.loads((base/'observed-before.json').read_text())
for path,digest in before['files'].items():
 assert hashlib.sha256((repo/path).read_bytes()).hexdigest()==digest,path
after=json.loads((base/'observed-after.json').read_text())
assert before==after
baseline={}
for row in subprocess.check_output(['git','ls-tree','-r',before['base']],cwd=repo).splitlines():
 metadata,name=row.split(b'\t',1)
 baseline[name.decode()]=metadata.split()[2].decode()
changed=[]
for path in before['files']:
 data=(repo/path).read_bytes()
 blob=hashlib.sha1(b'blob '+str(len(data)).encode()+b'\0'+data).hexdigest()
 if baseline.get(path)!=blob: changed.append(path)
assert set(changed)=={'journal/graph_compaction_native_test.go','journal/graph_compaction_process_test.go'},changed
reports={}
for mode in ('normal','race'):
 raw=gzip.decompress((base/(mode+'.log.gz')).read_bytes())
 log=raw.decode()
 assert not re.search(r'WARNING: DATA RACE|^FAIL|^--- FAIL:|^--- SKIP:',log,re.M)
 tops=re.findall(r'^--- PASS: (\w+) ',log,re.M)
 assert set(tops)=={'TestNativeGraphCheckpointArchiveReopenAndCollection','TestProcessGraphCheckpointArchiveKillAndCollection'} and len(tops)==2
 children=re.findall(r'^    --- PASS: (\w+/R[13]-domain) ',log,re.M)
 assert len(children)==4 and len(set(children))==4
 assert log.count('ARCHIVE_AUTHORITY_RECOVERED head=27 readers=1 unchanged=true')==4
 assert log.count('PROCESS_CHECKPOINT_ARCHIVE successive_compactions=2 all_servers_sigkill_observed=true')==2
 kills=re.findall(r'ARCHIVE_PROCESS_KILL node=(\d+) pid=(\d+) signal=killed',log)
 assert len(kills)==4 and sorted(int(node) for node,pid in kills)==[0,0,1,2]
 reopens=re.findall(r'ARCHIVE_PROCESS_REOPEN node=(\d+) old_pid=(\d+) new_pid=(\d+)',log)
 assert len(reopens)==4 and all(old!=new for node,old,new in reopens)
 assert log.count('ARCHIVE_ORIGINAL_STREAMS_READY')==2
 elapsed=re.findall(r'^ok\s+js-wf/journal\s+([0-9.]+)s$',log,re.M)
 assert len(elapsed)==1
 reports[mode]={'verdict':'PASS','top_level_groups':2,'native_cases':4,'elapsed_seconds':float(elapsed[0]),'observed_sigkill_exits':4,'exact_logical_authority_recoveries':4,'log_sha256':hashlib.sha256(raw).hexdigest()}
for name in ['initial-normal-failure','traced-normal-failure','initial-placement-readiness-failure']:
 log=gzip.decompress((base/(name+'.log.gz')).read_bytes()).decode()
 assert '--- FAIL: TestProcessGraphCheckpointArchiveKillAndCollection' in log
 assert '--- FAIL: TestProcessGraphCheckpointArchiveKillAndCollection/R3-domain' in log
 assert '--- PASS: TestProcessGraphCheckpointArchiveKillAndCollection/R1-domain' in log
probe=(base/'probe.log.gz')
probe_log=gzip.decompress(probe.read_bytes()).decode()
assert probe_log.count('error=<nil> response=')==6
commands=json.loads((base/'commands.json').read_text())
assert commands['normal']['exit_code']==commands['race']['exit_code']==0
assert '-race' in commands['race']['argv']
review={'verdict':'PASS focused normal and race components','base':before['base'],'selected_inputs':len(before['files']),'inputs_unchanged':True,'changes_from_base':changed,'reports':reports,'preserved_failures':3,'probe':'Six fresh raw API requests succeed across three endpoints during the traced stalled attempt; no server-side root cause proved.','scope':'Acknowledged archive publication followed by observed whole-cluster SIGKILL and original-store recovery. Modeled dispatch/source, fixed fixture clock and synchronous explicit collection. No power-loss, uncertain-publication cut, autonomous worker-kill or complete/native-matrix/scale/release qualification.'}
(base/'review.json').write_text(json.dumps(review,indent=2)+'\n')
print(json.dumps(review,indent=2))
