from pathlib import Path
import datetime,json,time,hashlib
root=Path('/tmp/js-wf-bulk-latency-cohort-whole-state-20261006')
e=json.loads((root/'execution.json').read_text());pid=e['pid'];exe=Path('/proc')/str(pid)/'exe'
with exe.open('rb') as stream:assert hashlib.file_digest(stream,'sha256').hexdigest()==e['sha256']
rows=[]
while Path('/proc',str(pid),'status').exists():
 try:
  status=Path('/proc',str(pid),'status').read_text()
 except FileNotFoundError:break
 values={line.split(':',1)[0]:line.split(':',1)[1].strip() for line in status.splitlines() if line.startswith(('VmRSS:','VmHWM:','Threads:'))}
 rows.append(dict(time_utc=datetime.datetime.now(datetime.timezone.utc).isoformat(),values=values))
 time.sleep(1)
Path('/tmp/js-wf-bulk-whole-state-memory-20261006.json').write_text(json.dumps(dict(pid=pid,source=e['source'],actual_sdk_sha256=e['sha256'],samples=rows,scope='Late-start one-second public proc observations of same actual SDK; excludes startup, servers and post-exit HWM. Not full400k RSS or application charge qualification.'),indent=2)+'\n')
