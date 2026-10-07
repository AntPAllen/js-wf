import sys,os,json,subprocess,socket,signal,time,hashlib,datetime,shutil
from pathlib import Path
sys.dont_write_bytecode=True
repo=Path('/home/exedev/js-wf');sys.path.insert(0,str(repo/'scripts'))
import fixture_archive,live_process_admission
root=Path('/tmp/js-wf-operator-connection-baseline-20261007');root.mkdir();source=root/'source'
revision='3d04417'
subprocess.run(['git','worktree','add','--detach',str(source),revision],cwd=repo,check=True)
revision=subprocess.check_output(['git','rev-parse','HEAD'],cwd=source,text=True).strip()
assert subprocess.check_output(['git','status','--porcelain'],cwd=source)==b''
binary=root/'wf';env=dict(os.environ,GOMAXPROCS='2',GOMEMLIMIT='1GiB',GOWORK='off',GOFLAGS='')
sha=lambda p:hashlib.sha256(p.read_bytes()).hexdigest()
build=['go','build','-race','-buildvcs=true','-o',str(binary),'./cmd/wf']
with (root/'build.log').open('w') as out:subprocess.run(build,cwd=source,env=env,stdout=out,stderr=subprocess.STDOUT,check=True)
info=subprocess.check_output(['go','version','-m',str(binary)],text=True)
assert 'vcs.revision='+revision in info and 'vcs.modified=false' in info and '-race=true' in info
records=[];report={'source':revision,'build':build,'build_info':info,'binary_sha256':sha(binary),'records':records}
write=lambda:(root/'baseline.json').write_text(json.dumps(report,indent=2)+'\n')
write()
for command in ('project','tombstone-loop'):
 for stage in ('INFO','PONG'):
  for sig in (signal.SIGTERM,signal.SIGINT):
   listener=socket.socket();listener.bind(('127.0.0.1',0));listener.listen();listener.settimeout(3)
   args=[str(binary),'-url','nats://127.0.0.1:'+str(listener.getsockname()[1]),command]
   child=subprocess.Popen(args,cwd=source,env=env,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
   conn=None
   try:
    conn,_=listener.accept();conn.settimeout(3);reader=conn.makefile('rb');wire=b'';server=b''
    if stage=='PONG':
     server=b'INFO {"server_id":"CONNECTION_FIXTURE","version":"2.15.0","proto":1,"max_payload":1048576}\r\n';conn.sendall(server)
     a,b=reader.readline(),reader.readline();assert a.startswith(b'CONNECT ') and b==b'PING\r\n';wire=a+b
    actual=live_process_admission.admit(child,args,{k:env[k] for k in ('GOMAXPROCS','GOMEMLIMIT','GOWORK','GOFLAGS')},source,binary,sha(binary))
    sent=datetime.datetime.now(datetime.timezone.utc);child.send_signal(sig)
    out,err=child.communicate(timeout=3);joined=datetime.datetime.now(datetime.timezone.utc)
    tail=reader.read();assert not tail
    record={'command':command,'stage':stage,'signal':signal.Signals(sig).name,'actual':actual,'sent_at':sent.isoformat(),'joined_at':joined.isoformat(),'exit_code':child.returncode,'client_wire':wire.decode(),'server_wire':server.decode(),'stdout':out.decode(),'stderr':err.decode()}
    records.append(record);write();assert child.returncode==-sig,record
    print('BASELINE_SIGNAL_TERMINATION',command,stage,sig,child.pid,child.returncode,flush=True)
   finally:
    if child.poll() is None:child.kill();child.communicate()
    if conn is not None:conn.close()
    listener.close()
assert len(records)==8 and subprocess.check_output(['git','status','--porcelain'],cwd=source)==b''
report['expected_baseline_failure_proven']=True;write()
shutil.copyfile(__file__,root/'executed-producer.py')
fixture_archive.capture(root,root.with_suffix('.tar.gz'),root.with_name(root.name+'-proof'),compresslevel=1)
print('BASELINE_ALL_EIGHT_CONFIRMED',flush=True)
