import hashlib,json,subprocess,time,sys
from pathlib import Path
out=Path(__file__).resolve().parent
mode=sys.argv[1]
assert mode in ('normal','race')
if mode=='native':
 command=['go','test','-race','./integration','-run','^(TestTerminalWakeupsDrainWhileHealthyLeaseHeld|TestLargeTerminalResultSpillsAndAwaitVerifies)$','-count=1','-v','-timeout=3m']
 names=('integration',)
else:
 command=['go','test']+(['-race'] if mode=='race' else [])+['./wf','./worker','-run','^(TestReadGraphTerminalMetadataAdmission|TestLegacyHeldTerminalMetadataAdmission|TestLegacyHeldTerminalResultBeforeAck|TestGraphReplaySnapshotTerminalMetadataAdmission|TestReplayTerminalEnvelopeBeforeHandler|TestReplayTerminalEnvelopeTypedBoundary|TestGraphChildRetentionRequiresExactParentOwnership)$','-count=1','-v','-timeout=3m']
 names=('wf','worker')
start=time.time_ns();captured={}
with (out/(mode+'.log')).open('w') as log:
 p=subprocess.Popen(command,stdout=log,stderr=subprocess.STDOUT)
 while p.poll() is None:
  for proc in Path('/proc').iterdir():
   if not proc.name.isdigit():continue
   try:
    cmd=(proc/'cmdline').read_bytes().replace(b'\0',b' ').decode()
    if not cmd.startswith('/tmp/go-build'):continue
    parent=int((proc/'status').read_text().split('PPid:')[1].splitlines()[0].strip())
    if parent!=p.pid:continue
    for name in names:
     if name in captured or '/'+name+'.test ' not in cmd:continue
     exe=proc/'exe';captured[name]={'pid':int(proc.name),'parent_pid':parent,'sha256':hashlib.sha256(exe.read_bytes()).hexdigest(),'command':cmd,'build':subprocess.check_output(['go','version','-m',str(exe)],text=True),'captured_ns':time.time_ns()}
     (out/(mode+'-executables.json')).write_text(json.dumps(captured,indent=2)+'\n')
   except (OSError,subprocess.CalledProcessError):pass
  time.sleep(.005)
receipt={'command':command,'driver_pid':p.pid,'actual_exit_code':p.returncode,'start_ns':start,'end_ns':time.time_ns(),'executables_captured':sorted(captured)}
(out/(mode+'-command.json')).write_text(json.dumps(receipt,indent=2)+'\n');print(json.dumps(receipt));raise SystemExit(p.returncode)
