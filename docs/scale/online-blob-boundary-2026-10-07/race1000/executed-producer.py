import sys
sys.dont_write_bytecode=True
from pathlib import Path
import datetime,importlib.util,json,os,re,shutil,subprocess,time
repo=Path('/home/exedev/js-wf');root=Path('/tmp/js-wf-online-blob-race1000-20261007')
sys.path.insert(0,str(repo/'scripts'))
import fixture_archive,live_process_admission
spec=importlib.util.spec_from_file_location('shared',repo/'scripts/run-domain-runtime-controls.py');shared=importlib.util.module_from_spec(spec);spec.loader.exec_module(shared)
assert not root.exists();rev=subprocess.check_output(['git','rev-parse','HEAD'],cwd=repo,text=True).strip();before=shared.source_inventory(rev)
root.mkdir();(root/'sample-traces').mkdir();shutil.copyfile(__file__,root/'executed-producer.py')
def save(name,value): (root/name).write_text(json.dumps(value,indent=2)+'\n')
save('source-before.json',before)
for name in before['files']:
 target=root/'selected-source'/name;target.parent.mkdir(parents=True,exist_ok=True);shutil.copyfile(repo/name,target)
test='TestSeededOnlineBlobBoundaryReplay';pins=['online-blob-boundary-'+mode+'.json' for mode in ('paused_upload','refresh_after_census','quiescent_control')]
env=dict(os.environ,GOMAXPROCS='2',GOMEMLIMIT='1GiB',SIM_SEEDS='1000',SIM_COVERAGE_SUMMARY='1',SIM_ONLINE_BLOB_BOUNDARY_ROOT=str(root/'sample-traces'),FAULT_TRACE_OUT=str(root/'failure-trace.json'))
binary=root/'sim-race.test';build=['go','test','-buildvcs=true','-p=1','-race','-c','-o',str(binary),'./sim']
command=[str(binary),'-test.v','-test.run=^(TestSeededOnlineBlobBoundaryReplay|TestPinnedRegressionCorpus)$/^online-blob-boundary-','-test.count=1','-test.timeout=60m']
save('commands.json',dict(build=build,run=command,scope='One workload seeds1..1000 with exact replay and its three shared pins; not full-suite acceptance.'))
with (root/'build.log').open('w') as log:subprocess.run(build,cwd=repo,env=env,stdout=log,stderr=subprocess.STDOUT,check=True)
info=subprocess.check_output(['go','version','-m',str(binary)],text=True);assert '-race=true' in info and 'vcs.revision='+rev in info and 'vcs.modified=false' in info
save('binary.json',dict(sha256=shared.sha(binary),build_info=info));started=time.monotonic()
with (root/'native.log').open('w') as log:
 child=subprocess.Popen(command,cwd=repo/'sim',env=env,stdout=log,stderr=subprocess.STDOUT)
 keys=('GOMAXPROCS','GOMEMLIMIT','SIM_SEEDS','SIM_COVERAGE_SUMMARY','SIM_ONLINE_BLOB_BOUNDARY_ROOT','FAULT_TRACE_OUT')
 actual=live_process_admission.admit(child,command,{key:env[key] for key in keys},repo/'sim',binary,shared.sha(binary));save('actual-sdk.json',actual)
 save('execution.json',dict(source=rev,status='running',actual_sdk_pid=child.pid,started_utc=datetime.datetime.now(datetime.timezone.utc).isoformat(),scope='Focused race1000 boundary workload only; actual counterexamples expected.'))
 print('ACTUAL_BLOB_MODEL_SDK',child.pid,rev,flush=True);code=child.wait()
save('execution.json',dict(source=rev,status='passed' if code==0 else 'failed',exit_code=code,actual_sdk_pid=child.pid,elapsed_seconds=time.monotonic()-started,finished_utc=datetime.datetime.now(datetime.timezone.utc).isoformat()))
after=shared.source_inventory(rev);assert before==after;save('source-after.json',after);save('closure.json',shared.closure(root))
result=None;error=None
try:
 assert code==0,'actual SDK failed';log=(root/'native.log').read_text()
 assert not any(token in log for token in ('DATA RACE','--- FAIL:','--- SKIP:'))
 assert re.findall(r'^--- PASS: (\w+) \([0-9.]+s\)$',log,re.M)==[test,'TestPinnedRegressionCorpus']
 observed=re.findall(r'^\s+--- PASS: TestPinnedRegressionCorpus/(\S+) \(',log,re.M);assert sorted(observed)==sorted(pins)
 expected=f'TIER1_SEEDS test={test} first=1 last=1000 completed=1000 requested=1000'
 assert log.count('TIER1_SEEDS ')==1 and expected in log
 counts=re.findall(r'online blob boundary: modes=map\[paused_upload:(\d+) quiescent_control:(\d+) refresh_after_census:(\d+)\]',log);assert len(counts)==1
 paused,quiet,refresh=map(int,counts[0]);assert min(paused,quiet,refresh)>0 and paused+quiet+refresh==1000
 coverage=re.findall(r'^TIER1_COVERAGE (.*)$',log,re.M);assert len(coverage)==1
 fields=dict(token.split('=',1) for token in coverage[0].split())
 expected_fields=dict(model_version=3,seeds_per_workload=1000,generated_schedules=1000,scheduler_choices=1000,transport_events=12*(paused+quiet)+19*refresh,virtual_ms_max=7200000,virtual_buckets_zero=0,under_1s=0,under_1m=0,at_least_1m=1000)
 assert all(int(fields[key])==value for key,value in expected_fields.items())
 traces=[json.loads(p.read_text()) for p in (root/'sample-traces').glob('*.json')]
 assert len(traces)==3 and {trace['decisions'][0]['chosen'] for trace in traces}=={'paused_upload','quiescent_control','refresh_after_census'}
 result=dict(seeds=1000,exact_replays=1000,modes=dict(paused_upload=paused,quiescent_control=quiet,refresh_after_census=refresh),pins=pins,coverage=fields,online_gc_safe=False,scope='One production boundary workload; original graph and full release matrices remain independently qualified.')
except (AssertionError,ValueError,KeyError,TypeError) as exc:error=str(exc) or type(exc).__name__
save('row-review.json',dict(qualification=result,rejection=error))
meta=fixture_archive.capture(root,root.with_suffix('.tar.gz'),root.with_name(root.name+'-proof'),compresslevel=1)
print('TERMINAL_BLOB_MODEL',json.dumps(dict(qualification=result,rejection=error,archive=meta)),flush=True);assert error is None,error
