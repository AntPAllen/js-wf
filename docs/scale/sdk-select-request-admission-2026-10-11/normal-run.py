import hashlib,json,subprocess,time
from pathlib import Path
out=Path(__file__).resolve().parent
command=['go', 'test', './wf', './internal/stepwire', '-run', '^(TestSelectManyRequestMetadataAdmission|TestSelectManySuspendsReplaysChoiceAndRetainsLosers|TestSelectManyUnknownCompletionReplaysRecordedWinner|TestReplayNullStepMetadataBeforeHandler|TestNullRuntimeMetadataAndOpaqueResults|TestRequestVariantsRemainReadable|TestCompletionEnvelopeKeepsUserResultOpaque|TestRequestEnvelopeKeepsHashedUserInputOpaque)$', '-count=1', '-v', '-timeout=4m']
start=time.time_ns();captured={}
with (out/'normal.log').open('w') as log:
 p=subprocess.Popen(command,stdout=log,stderr=subprocess.STDOUT)
 while p.poll() is None:
  for proc in Path('/proc').iterdir():
   if not proc.name.isdigit():continue
   try:
    cmd=(proc/'cmdline').read_bytes().replace(b'\0',b' ').decode()
    if not cmd.startswith('/tmp/go-build'):continue
    for name in ('wf','stepwire'):
     if name in captured or '/'+name+'.test ' not in cmd:continue
     exe=proc/'exe';captured[name]={'pid':int(proc.name),'sha256':hashlib.sha256(exe.read_bytes()).hexdigest(),'command':cmd,'build':subprocess.check_output(['go','version','-m',str(exe)],text=True),'captured_ns':time.time_ns()}
     (out/'normal-executables.json').write_text(json.dumps(captured,indent=2)+'\n')
   except (OSError,subprocess.CalledProcessError):pass
  time.sleep(.001)
receipt={'command':command,'driver_pid':p.pid,'actual_exit_code':p.returncode,'start_ns':start,'end_ns':time.time_ns(),'executables_captured':sorted(captured)}
(out/'normal-command.json').write_text(json.dumps(receipt,indent=2)+'\n');print(json.dumps(receipt));raise SystemExit(p.returncode)
