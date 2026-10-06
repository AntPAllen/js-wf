from pathlib import Path
import json,hashlib,subprocess,shutil,re,sys,importlib.util,datetime,calendar
repo=Path('/home/exedev/js-wf');root=Path('/tmp/js-wf-bulk-latency-full400k-valid-20261006')
out=repo/'docs/scale/full400k-bulk-capacity-preparation-2026-10-06/native-valid-population'
sys.path.insert(0,str(repo/'scripts'));import fixture_archive
spec=importlib.util.spec_from_file_location('closed',repo/'scripts/verify-tier2-closed-originals.py');closed=importlib.util.module_from_spec(spec);spec.loader.exec_module(closed)
read=lambda p:json.loads(p.read_text())
sha=closed.sha
e=read(root/'execution.json');assert e['status'] in ('passed','failed') and 'exit_code' in e
assert (root/'source-after.json').exists() and not Path('/proc',str(e['pid'])).exists()
b=read(root/'binary.json');assert sha(root/'integration.test')==b['sha256']==e['sha256']
assert 'vcs.revision='+e['source'] in e['build_info'] and 'vcs.modified=false' in e['build_info'] and '-race=true' not in e['build_info']
assert '\tdep\tgithub.com/nats-io/nats.go\tv1.54.0' in e['build_info']
before=read(root/'source-before.json');assert before==read(root/'source-after.json') and before['revision']==e['source']
for name,digest in before['files'].items():
 assert sha(root/'source'/name)==digest
 assert hashlib.sha256(subprocess.check_output(['git','cat-file','blob',e['source']+':'+name],cwd=repo)).hexdigest()==digest
commands=read(root/'commands.json');env=commands['env'];assert (env['GOMAXPROCS'],env['GOGC'],env['GOMEMLIMIT'])==('4','500','4GiB')
assert env['WF_MATRIX_BULK_CAPACITY_ROOT']==str(root/'fixture')
assert commands['test'][1]=='-test.run=^TestMatrixBulkLatencyFull400kCapacity$'
admission=read(root/'disk-admission.json');assert admission['free_bytes']>=admission['minimum_free_bytes']==5*1024**3
servers=read(root/'actual-containers/actual-servers.json');assert len(servers)==5
nodes=set();containers=[]
for server in servers:
 assert not Path('/proc',str(server['host_pid'])).exists()
 assert sha(root/'actual-containers'/server['sha256'])==server['sha256']
 assert '\tmod\tgithub.com/nats-io/nats-server/v2\tv2.15.0' in server['actual_proc_build_info']
 mounts=[m['Source'] for m in server['container']['Mounts'] if m['Destination']=='/data'];assert len(mounts)==1
 node=int(Path(mounts[0]).name.split('-')[-1]);assert mounts[0]==str(root/'fixture/cluster'/('node-'+str(node)));nodes.add(node)
 cid=server['container']['Id'];r=subprocess.run(['docker','inspect',cid],capture_output=True,text=True)
 if r.returncode:assert 'no such object' in r.stderr.lower();state='removed'
 else:
  item=json.loads(r.stdout)[0];assert not item['State']['Running'] and not item['State']['Restarting'];state=item['State']
 containers.append(dict(id=cid,recorded_pid=server['host_pid'],current=state))
assert nodes==set(range(5))
limits=[]
for p in Path('/proc').glob('[0-9]*'):
 try:
  exe=(p/'exe').resolve();args=(p/'cmdline').read_bytes().split(b'\0')
  assert not exe.is_relative_to(root) and not any(str(root).encode() in arg for arg in args)
 except PermissionError:limits.append(str(p))
 except (FileNotFoundError,ProcessLookupError):pass
ids=subprocess.check_output(['docker','ps','-q'],text=True).split();running=json.loads(subprocess.check_output(['docker','inspect',*ids],text=True)) if ids else []
for c in running:
 for m in c['Mounts']:
  source=Path(m['Source']).resolve();assert not source.is_relative_to(root) and not root.is_relative_to(source)
fd=closed.verify_no_open_originals(root)
log=(root/'native.log').read_text();passed={name:float(seconds) for name,seconds in re.findall(r'^--- PASS: (\S+) \((\d+\.\d+)s\)$',log,re.M)}
capacity=read(root/'fixture/capacity.json') if (root/'fixture/capacity.json').exists() else None
accepted=e['status']=='passed' and e['exit_code']==0
samples=0;ids_seen=set();samples_sha=None
if accepted:
 assert set(passed)=={'TestMatrixBulkLatencyFull400kCapacity'} and '\nPASS\n' in log and '--- FAIL:' not in log
 assert capacity['accepted'] is True and capacity['population']==400000 and capacity['samples']==800000
 assert capacity['stage_limit_ns']==360*10**9 and capacity['frozen_point_checks']==256 and capacity['all_bulk_samples_equal_independent_full_timestamp_oracle']
 assert capacity['profile']==dict(gomaxprocs=4,gogc='500',gomemlimit='4GiB')
 assert capacity['sdk_lifetime_peak_rss_kib']>0
 for label in ['before','after']:
  v=capacity[label];assert v['error']=='<nil>' and v['elapsed_ns']<v['limit_ns']==20*10**9
  assert v['report']==dict(Invocations=400000,Journals=400000,Entries=4800000,Terminal=400000)
 bulk=capacity['bulk'];assert bulk['error']=='<nil>' and bulk['elapsed_ns']<360*10**9
 stats=bulk['stats'];assert stats['snapshot_fallbacks']==0 and stats['out_of_prefix_child_lookups']==0 and stats['charged_bytes']<=3*1024**3
 expected_counts=dict(WF_INV=400000,WF_JRN=4800000,KV_WF_STATE=400000,WF_SIG=0)
 assert set(stats['records'])<=set(expected_counts)
 assert all(stats['records'].get(name,0)==count for name,count in expected_counts.items())
 for name,count in expected_counts.items():assert stats['source_cuts'][name]['Messages']==count and stats['source_cuts'][name]['Consumers']==0
 def ns(value):
  match=re.fullmatch(r'(\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d)(?:\.(\d{1,9}))?Z',value);assert match
  seconds=calendar.timegm(datetime.datetime.strptime(match[1],'%Y-%m-%dT%H:%M:%S').timetuple())
  return seconds*10**9+int((match[2] or '').ljust(9,'0'))
 with (root/'fixture/samples.jsonl').open() as stream:
  for line in stream:
   pair=json.loads(line);assert len(pair)==2
   first,last=pair;assert first['event']=='start' and last['event']=='terminal'
   identifier=first['id'];number=int(identifier.removeprefix('capacity-'));assert identifier==f'capacity-{number:06d}' and 0<=number<400000 and identifier not in ids_seen;ids_seen.add(identifier)
   assert first['enabled']==last['enabled'] and ns(first['observed'])<=ns(last['observed'])
   for s in pair:
    assert s['type']=='audit' and s['id']==identifier and s.get('server_clock_offset_ns',0)==0 and 'observed_lower' not in s
    assert s['delay_ns']==ns(s['observed'])-ns(s['enabled'])>=0
    assert ns(s['observed'])<=ns(capacity['original_completion_deadline'])
   samples+=2
 assert samples==800000 and len(ids_seen)==400000
 samples_sha=sha(root/'fixture/samples.jsonl')
else:
 assert not capacity or capacity['accepted'] is False
unit=subprocess.check_output(['systemctl','--user','show','js-wf-bulk-latency-full400k-valid-20261006.service','-p','ActiveState','-p','SubState','-p','MainPID','-p','ExecMainStatus','-p','MemoryMax','-p','MemoryPeak','-p','CPUQuotaPerSecUSec'],text=True)
assert 'MainPID=0' in unit
review=dict(execution=e,finite_full400k_bulk_capacity_accepted=accepted,source_inputs_verified=len(before['files']),five_actual_servers_closed=containers,current_global_root_process_scope_clear=True,unobservable_processes=limits,visible_fd_check=fd,native_passes=passed,capacity=capacity,complete_sample_lines_verified=samples//2,complete_samples_verified=samples,samples_sha256=samples_sha,unit=unit,rss_measurement_scope_correction='RUSAGE_SELF highwater observed at test-body completion, including population/oracle/audits, before test cleanup and SDK exit; sampled public process VmHWM also retained. Do not label this observation as exact final whole-SDK-lifetime RSS.',scope='Fresh synthetic full400k/4.8M quiet valid activity latency and measured memory capacity at recorded source; full independent timestamp oracle and256 frozen point witnesses plus original20s full integrity before/after and6m bulk; no real-workflow/fault/default/current-matrix/24h qualification. Complete originals retained; no stores reopened.')
out.mkdir(parents=True,exist_ok=True)
for p in [root/'independent-review.json',out/'independent-review.json']:p.write_text(json.dumps(review,indent=2)+'\n')
shutil.copyfile(__file__,root/'executed-review.py');shutil.copyfile(__file__,out/'executed-review.py')
fixture_archive.capture(root,Path('/tmp/js-wf-bulk-latency-full400k-valid-complete-20261006.tar.gz'),out)
print('COMPLETE_REVIEW_AND_ARCHIVE',accepted,'samples',samples,'body_peak_rss_kib',capacity.get('sdk_lifetime_peak_rss_kib') if capacity else None,flush=True)
