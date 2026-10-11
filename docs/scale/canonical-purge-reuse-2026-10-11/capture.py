import hashlib,json,os,shutil,subprocess,sys,time
from pathlib import Path
root=Path(__file__).resolve().parent
path=Path(sys.argv[1]); data=path.read_bytes(); sha=hashlib.sha256(data).hexdigest()
kept=root/(sha+'.test')
if not kept.exists(): shutil.copy2(path,kept)
start=time.time_ns()
p=subprocess.Popen(sys.argv[1:])
code=p.wait()
(root/(str(p.pid)+'-execution.json')).write_text(json.dumps(dict(argv=sys.argv[1:],pid=p.pid,start_ns=start,end_ns=time.time_ns(),actual_exit=code,sha256=sha,bytes=len(data),retained_binary=str(kept)),indent=2)+'\n')
sys.exit(code)
