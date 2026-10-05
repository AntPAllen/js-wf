import socket,json,datetime
from pathlib import Path
root=Path('/tmp/js-wf-direct-r1-r5-process-owner-loss-20261005')
records=json.loads((root/'actual-containers/actual-servers.json').read_text())
results=[]
for r in records:
 c=r['container'];ports=c['NetworkSettings']['Ports'].get('4222/tcp')
 if not ports:continue
 for stream in ('WF_INV','WF_JRN'):
  observed={'container':c['Name'],'stream':stream,'at':datetime.datetime.now(datetime.timezone.utc).isoformat()}
  try:
   with socket.create_connection(('127.0.0.1',int(ports[0]['HostPort'])),timeout=2) as s:
    s.settimeout(2);f=s.makefile('rb');observed['greeting']=f.readline().decode().strip()
    s.sendall(b'CONNECT {"verbose":false,"pedantic":false}\r\nSUB _INBOX.cleanup_observation 1\r\n'+f'PUB $JS.API.STREAM.INFO.{stream} _INBOX.cleanup_observation 2\r\n{{}}\r\n'.encode())
    while True:
     line=f.readline()
     if line==b'PING\r\n':s.sendall(b'PONG\r\n');continue
     if line.startswith(b'MSG '):
      size=int(line.split()[-1]);data=f.read(size);f.read(2)
      observed['response']=json.loads(data);break
     if not line:raise RuntimeError('connection closed')
  except Exception as e:observed['error']=str(e)
  results.append(observed)
(root/'independent-api-observation.json').write_text(json.dumps(results,indent=2)+'\n')
(root/'executed-api-observer.py').write_text(Path(__file__).read_text())
for r in results:print(r['container'],r['stream'],r.get('error'),r.get('response',{}).get('state',{}).get('consumer_count'))
