import hashlib, json, subprocess, time
from pathlib import Path
out=Path(__file__).resolve().parent
command=['go','test','-race','./integrity','-run','^(TestRawGraphStartFieldPresence|TestRawGraphCursorFieldPresence|TestRawGraphCursorState|TestRawGraphCursorVersionCapabilities|TestRawGraphCursorEnvelope|TestRawGraphStartEnvelope|TestRawGraphRetiredProjectionEnvelope|TestRawGraphRetiredSourceProjectionScope|TestRawGraphStartSourceHeaders|TestNativeRawGraphCheckpointAudit)$','-count=1','-v','-timeout=4m']
start=time.time_ns()
captured=False
with (out/'race.log').open('w') as log:
 p=subprocess.Popen(command,stdout=log,stderr=subprocess.STDOUT)
 while p.poll() is None:
  if not captured:
   for d in Path('/proc').iterdir():
    if not d.name.isdigit(): continue
    try:
     cmd=(d/'cmdline').read_bytes().replace(b'\0',b' ').decode()
     if cmd.startswith('/tmp/go-build') and '/integrity.test ' in cmd and 'TestRawGraphStartFieldPresence' in cmd:
      exe=d/'exe'
      receipt={'pid':int(d.name),'sha256':hashlib.sha256(exe.read_bytes()).hexdigest(),'command':cmd,'build':subprocess.check_output(['go','version','-m',str(exe)],text=True),'captured_ns':time.time_ns()}
      (out/'executable.json').write_text(json.dumps(receipt,indent=2)+'\n')
      captured=True
      break
    except (OSError,subprocess.CalledProcessError): pass
  time.sleep(.1)
receipt={'command':command,'driver_pid':p.pid,'actual_exit_code':p.returncode,'start_ns':start,'end_ns':time.time_ns(),'executable_captured':captured}
(out/'command.json').write_text(json.dumps(receipt,indent=2)+'\n')
print(json.dumps(receipt))
raise SystemExit(p.returncode)
