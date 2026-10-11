import hashlib,json,os,subprocess,time
from pathlib import Path
out=Path(__file__).resolve().parent
root=Path('/home/exedev/js-wf-replay-terminal-result-20261011')
root.mkdir(exist_ok=True)
wrapper=root/'capture.py'
wrapper.write_text('''import hashlib,json,os,shutil,subprocess,sys,time
from pathlib import Path
root=Path(__file__).resolve().parent
path=Path(sys.argv[1]); data=path.read_bytes(); sha=hashlib.sha256(data).hexdigest()
kept=root/(sha+'.test')
if not kept.exists(): shutil.copy2(path,kept)
start=time.time_ns()
p=subprocess.Popen(sys.argv[1:])
code=p.wait()
(root/(str(p.pid)+'-execution.json')).write_text(json.dumps(dict(argv=sys.argv[1:],pid=p.pid,start_ns=start,end_ns=time.time_ns(),actual_exit=code,sha256=sha,bytes=len(data),retained_binary=str(kept)),indent=2)+'\\n')
sys.exit(code)
''')
inputs=json.loads((out/'source-inputs.json').read_text())
def check():
    assert all(hashlib.sha256(Path(p).read_bytes()).hexdigest()==sha for p,sha in inputs.items())
check()
pattern='^(TestSDKChildOutcomeBeforeConsumption|TestSDKSelectedChildProvenanceBeforeConsumption|TestCheckpointPromiseOutcomeMetadataAdmission|TestReadGraphTerminalMetadataAdmission|TestLegacyHeldTerminalMetadataAdmission|TestLegacyHeldTerminalResultBeforeAck|TestGraphReplaySnapshotTerminalMetadataAdmission|TestReplayTerminalEnvelopeBeforeHandler|TestReplayTerminalEnvelopeTypedBoundary|TestReplayTerminalResultBeforeHandler|TestReplayTerminalResultBeforePlugin|TestGraphChildRetentionRequiresExactParentOwnership)$'
jobs=[('normal-admission',[],['./wf','./worker','./internal/checkpoint','./cmd/wf'],pattern,'3m'),('race-admission',['-race'],['./wf','./worker','./internal/checkpoint','./cmd/wf'],pattern,'3m'),('race-sdk',['-race'],['./wf'],'','8m'),('race-native-plugin',['-race'],['./cmd/wf'],'^(TestReplayContinuationPlugin|TestReplayTerminalResultBeforePlugin|TestReplayRecordOrderBeforePlugin|TestReplayDeclaredGraphBeforePlugin)$','6m')]
env=dict(os.environ,GOMAXPROCS='2',GOMEMLIMIT='512MiB')
for name,flags,packages,run,timeout in jobs:
    check()
    command=['go','test',*flags,'-p=1','-exec','python3 '+str(wrapper),*packages]
    if run:command+=['-run',run]
    command+=['-count=1','-v','-timeout='+timeout]
    start=time.time_ns()
    with (out/(name+'.log')).open('w') as log:
        code=subprocess.run(command,env=env,stdout=log,stderr=subprocess.STDOUT).returncode
    check()
    executions={p.name:json.loads(p.read_text()) for p in root.glob('*-execution.json')}
    (out/(name+'-command.json')).write_text(json.dumps(dict(command=command,actual_exit=code,start_ns=start,end_ns=time.time_ns(),go_inputs_unchanged=True,executions=executions),indent=2)+'\n')
    print(name,'actual_exit',code,flush=True)
    if code:raise SystemExit(code)
