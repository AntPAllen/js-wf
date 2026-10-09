from pathlib import Path
import subprocess,os,json,hashlib
root=Path(__file__).resolve().parent
repo=Path('/home/exedev/js-wf')
env=dict(os.environ,GOMAXPROCS='2',GOMEMLIMIT='512MiB')
records=[]
def call(args,log,overrides={}):
 with (root/log).open('w') as out:r=subprocess.run(args,cwd=repo,env=dict(env,**overrides),stdout=out,stderr=subprocess.STDOUT)
 records.append(dict(command=args,cwd=str(repo),exit_code=r.returncode,log=log))
 (root/'pin-command-results.json').write_text(json.dumps(records,indent=2)+'\n')
 r.check_returncode()
binary=root/'race.test'
call(['go','test','-p=1','-race','-c','-o',str(binary),'./sim'],'race-compile.log')
(root/'race-binary.json').write_text(json.dumps(dict(sha256=hashlib.sha256(binary.read_bytes()).hexdigest(),build_info=subprocess.check_output(['go','version','-m',str(binary)],text=True)),indent=2)+'\n')
for p in sorted((root/'pins').glob('*.json')):
 call([str(binary),'-test.run=^TestReplayFaultTrace$','-test.count=1','-test.timeout=5m','-test.v'],f'pin-race-{p.stem}.log',{'FAULT_TRACE':str(p)})
print('all five directed pin race replays passed')
