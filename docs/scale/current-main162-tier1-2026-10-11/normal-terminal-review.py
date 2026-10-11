import gzip,hashlib,json,subprocess
from pathlib import Path
base=Path(__file__).resolve().parent
state=json.loads((base/'normal-state.json').read_text())
assert state['phase']=='closed' and state['exit']==0
assert state['source']=='f24e6c012b87bb7c9246f4d2f00d4d8d94dba6fa'
assert not Path('/proc/'+str(state['pid'])).exists() and not Path('/proc/'+str(state['child_pid'])).exists()
subprocess.check_call(['python3',str(base/'qualification-review.py'),'normal'])
files=json.loads((base/'normal-terminal-files.json').read_text());root=Path(state['root'])/'normal1000'
assert set(files)=={str(p.relative_to(root)) for p in root.rglob('*') if p.is_file() and p.name!='sim.test'}
for name,record in files.items():
 packed=(base/'normal-terminal'/(name+'.gz')).read_bytes()
 assert hashlib.sha256(packed).hexdigest()==record['compressed_sha256']
 data=gzip.decompress(packed)
 assert len(data)==record['bytes'] and hashlib.sha256(data).hexdigest()==record['sha256']
 assert data==(root/name).read_bytes()
result=dict(accepted=True,compressed_files=len(files),original_bytes=sum(row['bytes'] for row in files.values()),source=state['source'],actual_exit=0)
(base/'normal-terminal-review.json').write_text(json.dumps(result,indent=2)+'\n');print(json.dumps(result))
