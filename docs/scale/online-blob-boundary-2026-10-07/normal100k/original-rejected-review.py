import sys
sys.dont_write_bytecode=True
from pathlib import Path
import datetime,hashlib,importlib.util,io,json,re,shutil,subprocess,time
repo=Path('/home/exedev/js-wf');root=Path('/tmp/js-wf-online-blob-normal100k-v2-20261007');out=Path('/tmp/js-wf-online-blob-normal100k-independent-20261007');unit=root.name+'.service'
sys.path.insert(0,str(repo/'scripts'));import fixture_archive
from storage_review_common import closure

def verify_model_log(log):
 assert not any(x in log for x in ('--- FAIL:','--- SKIP:','DATA RACE'))
 test='TestSeededOnlineBlobBoundaryReplay'
 assert re.findall(r'^--- PASS: (\w+) \([0-9.]+s\)$',log,re.M)==[test,'TestPinnedRegressionCorpus']
 names=re.findall(r'^\s+--- PASS: TestPinnedRegressionCorpus/(\S+) \(',log,re.M)
 assert sorted(names)==sorted('online-blob-boundary-'+m+'.json' for m in ('paused_upload','quiescent_control','refresh_after_census'))
 assert log.count('TIER1_SEEDS ')==1 and f'TIER1_SEEDS test={test} first=1 last=100000 completed=100000 requested=100000' in log
 values=re.findall(r'online blob boundary: modes=map\[paused_upload:(\d+) quiescent_control:(\d+) refresh_after_census:(\d+)\]',log);assert len(values)==1
 paused,quiet,refresh=map(int,values[0]);assert min(paused,quiet,refresh)>0 and paused+quiet+refresh==100000
 lines=re.findall(r'^TIER1_COVERAGE (.*)$',log,re.M);assert len(lines)==1
 fields=dict(token.split('=',1) for token in lines[0].split())
 expected=dict(model_version=3,seeds_per_workload=100000,generated_schedules=100000,scheduler_choices=100000,transport_events=12*(paused+quiet)+19*refresh,virtual_ms_max=7200000,virtual_buckets_zero=0,under_1s=0,under_1m=0,at_least_1m=100000)
 assert all(int(fields[k])==v for k,v in expected.items())
 return dict(paused_upload=paused,quiescent_control=quiet,refresh_after_census=refresh)

def unit_state():return subprocess.check_output(['systemctl','show',unit,'-p','MainPID','-p','SubState','-p','ExecMainStatus','-p','Restart'],text=True)
initial=unit_state();assert 'SubState=running\n' in initial and 'MainPID=0\n' not in initial
out.mkdir();shutil.copyfile(__file__,out/'executed-review.py')
(out/'initial-live-unit.txt').write_text(initial)
print('WAITING_FOR_ORIGINAL_MODEL_UNIT',unit,flush=True)
while 'MainPID=0\n' not in unit_state():time.sleep(10)
state=unit_state();assert 'ExecMainStatus=0\n' in state and 'Restart=no\n' in state and 'SubState=exited\n' in state
read=lambda p:json.loads(p.read_text());execution=read(root/'execution.json');rev=execution['source'];assert execution['exit_code']==0 and execution['status']=='passed'
before=read(root/'source-before.json');assert before==read(root/'source-after.json') and before['revision']==rev
names=subprocess.check_output(['git','ls-tree','-r','--name-only',rev],cwd=repo,text=True).splitlines();selected=[n for n in names if n.endswith(('.go','.py','.yml')) or n in ('go.mod','go.sum') or n.startswith('sim/testdata/')];assert set(selected)==set(before['files'])
blobs=io.BytesIO(subprocess.check_output(['git','cat-file','--batch'],cwd=repo,input=''.join(rev+':'+name+'\n' for name in selected).encode()))
for name in selected:
 header=blobs.readline().split();assert header[1]==b'blob';data=blobs.read(int(header[2]));assert blobs.read(1)==b'\n'
 assert hashlib.sha256(data).hexdigest()==before['files'][name]==hashlib.sha256((root/'selected-source'/name).read_bytes()).hexdigest()
assert not blobs.read()
sdk=read(root/'actual-sdk.json');binary=read(root/'binary.json');commands=read(root/'commands.json');assert sdk['pid']==execution['actual_sdk_pid'] and not Path('/proc',str(sdk['pid'])).exists()
assert sdk['args']==commands['run'] and sdk['working_directory']==str(repo/'sim') and sdk['admission']['stable_identity_observed_twice']
assert sdk['exe_sha256']==binary['sha256']==hashlib.sha256((root/'sim-normal.test').read_bytes()).hexdigest()
assert '-race=true' not in binary['build_info'] and 'vcs.revision='+rev in binary['build_info'] and 'vcs.modified=false' in binary['build_info']
assert sdk['environment']==dict(GOMAXPROCS='2',GOMEMLIMIT='1GiB',SIM_SEEDS='100000',SIM_COVERAGE_SUMMARY='1',SIM_ONLINE_BLOB_BOUNDARY_ROOT=str(root/'sample-traces'),FAULT_TRACE_OUT=str(root/'failure-trace.json'))
log=(root/'native.log').read_text();counts=verify_model_log(log);row=read(root/'row-review.json');assert row['rejection'] is None and row['qualification']['modes']==counts and row['qualification']['seeds']==100000 and row['qualification']['exact_replays']==100000 and row['qualification']['online_gc_safe'] is False
bad=[log.replace('completed=100000','completed=99999'),log.replace('requested=100000','requested=1000'),log.replace('generated_schedules=100000','generated_schedules=99999'),log.replace('at_least_1m=100000','at_least_1m=99999'),log.replace('virtual_ms_max=7200000','virtual_ms_max=3600000'),log.replace('--- PASS: TestPinnedRegressionCorpus/online-blob-boundary-paused_upload.json','--- SKIP: TestPinnedRegressionCorpus/online-blob-boundary-paused_upload.json'),log+'\nDATA RACE']
for marker in ('TIER1_SEEDS ','TIER1_COVERAGE ','online blob boundary: modes='):
 line=next(line for line in log.splitlines() if marker in line);bad.extend((log.replace(line,''),log.replace(line,line+'\n'+line)))
for altered in bad:
 try:verify_model_log(altered)
 except (AssertionError,ValueError,KeyError):pass
 else:raise AssertionError('actual model-log mutation accepted')
traces=[read(p) for p in (root/'sample-traces').glob('*.json')];assert len(traces)==3 and {t['decisions'][0]['chosen'] for t in traces}==set(counts)
for trace in traces:
 mode=trace['decisions'][0]['chosen'];pin=read(repo/'sim/testdata/regressions'/('online-blob-boundary-'+mode+'.json'));assert trace==pin
meta_root=Path(str(root)+'-proof');meta=read(meta_root/'archive-verification.json');inventory=read(meta_root/'fixture-inventory.json')
assert hashlib.sha256((meta_root/'fixture-inventory.json').read_bytes()).hexdigest()==meta['inventory_sha256']
with root.with_suffix('.tar.gz').open('rb') as stream:declared,actual=fixture_archive.verify_hashed_stream(stream,dict(bytes=meta['archive_bytes'],sha256=meta['archive_sha256']))
assert declared==inventory and fixture_archive.inventory(root)==inventory['files']
report=dict(accepted_focused_normal100k=True,online_gc_safe=False,source=rev,selected_source_files=len(selected),actual_sdk=sdk,seeded_schedules=100000,exact_replays=100000,modes=counts,pins=3,actual_log_mutations_rejected=len(bad),complete_archive=actual,archive_files=len(inventory['files']),fresh_closure=closure(root),terminal_unit=state,finished_utc=datetime.datetime.now(datetime.timezone.utc).isoformat(),scope='One production boundary workload and three exact pins; no full graph, native Raft or online-GC qualification.')
(out/'independent-review.json').write_text(json.dumps(report,indent=2)+'\n');print('FOCUSED_BLOB_NORMAL100K_ACCEPTED',rev,sdk['pid'],json.dumps(counts),len(bad),flush=True)
