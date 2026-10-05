#!/usr/bin/env python3
from pathlib import Path
import subprocess,json,hashlib,os,shutil,time,datetime,argparse
parser=argparse.ArgumentParser(description="Retain the complete six-boundary 500-child combined parent SIGKILL and library journal restart race run.")
parser.add_argument('--root',type=Path,required=True,help='Fresh absolute evidence directory outside the checkout')
parser.add_argument('--seed',type=int,default=1)
parser.add_argument('--physical-drain',action='store_true',help='require production worker drain and all-three-peer zero queue/64 durable witness inside original case limit')
parser.add_argument('--diagnostic-trace',action='store_true',help='opt-in worker timings and stack before the existing cut deadline')
parser.add_argument('--case',choices=[p+'/'+c for p in ('create','results') for c in ('first','interior','last')],help='focused diagnostic only; cannot qualify the full matrix')
args=parser.parse_args()
repo=Path(subprocess.check_output(['git','rev-parse','--show-toplevel'],text=True).strip()).resolve()
root=args.root
if not root.is_absolute() or root.resolve().is_relative_to(repo) or not -(2**63)<=args.seed<2**63:
 parser.error('require an absolute evidence root outside the checkout and an int64 seed')
root=root.resolve();root.mkdir(mode=0o700);(root/'originals').mkdir(mode=0o700)
def sha(p):
 with p.open('rb') as f:return hashlib.file_digest(f,'sha256').hexdigest()
revision=subprocess.check_output(['git','rev-parse','HEAD'],cwd=repo,text=True).strip();assert not subprocess.check_output(['git','status','--porcelain'],cwd=repo)
producer_bytes=Path(__file__).read_bytes();producer_path=str(Path(__file__).resolve().relative_to(repo))
assert producer_bytes==subprocess.check_output(['git','show',revision+':'+producer_path],cwd=repo)
(root/'producer-source.json').write_text(json.dumps({'revision':revision,'path':producer_path,'sha256':hashlib.sha256(producer_bytes).hexdigest()},indent=2)+'\n')
names=subprocess.check_output(['git','ls-files','*.go','go.mod','go.sum'],cwd=repo,text=True).splitlines();before={n:sha(repo/n) for n in names}
for n,d in before.items():
 assert hashlib.sha256(subprocess.check_output(['git','show',revision+':'+n],cwd=repo)).hexdigest()==d
 p=root/'source'/n;p.parent.mkdir(parents=True,exist_ok=True);shutil.copy2(repo/n,p)
(root/'source-before.json').write_text(json.dumps({'revision':revision,'files':before},indent=2)+'\n')
fmt='{{.Dir}}|{{join .GoFiles " "}}|{{join .CgoFiles " "}}|{{join .TestGoFiles " "}}|{{join .XTestGoFiles " "}}'
deps=subprocess.check_output(['go','list','-deps','-test','-f',fmt,'./integration'],cwd=repo,text=True);(root/'dependencies.txt').write_text(deps)
goroot=Path(subprocess.check_output(['go','env','GOROOT'],text=True).strip());modules=Path(subprocess.check_output(['go','env','GOMODCACHE'],text=True).strip());inputs={};captured={}
for line in deps.splitlines():
 directory,*groups=line.split('|')
 for n in ' '.join(groups).split():
  p=(Path(directory)/n).resolve()
  if not p.is_file() or p.is_relative_to(repo):continue
  if str(p) in inputs:continue
  digest=sha(p);inputs[str(p)]=digest
  rel=Path('modules')/p.relative_to(modules) if p.is_relative_to(modules) else Path('toolchain')/p.relative_to(goroot) if p.is_relative_to(goroot) else Path('other')/str(p).lstrip('/')
  dest=root/'selected-external-source'/rel;dest.parent.mkdir(parents=True,exist_ok=True);shutil.copy2(p,dest);assert sha(dest)==digest;captured[str(p)]=str(dest.relative_to(root))
(root/'external-source-before.json').write_text(json.dumps(inputs,indent=2)+'\n');(root/'external-captured-paths.json').write_text(json.dumps(captured,indent=2)+'\n')
env={k:v for k,v in os.environ.items() if not k.startswith('WF_')};env.update(GOMAXPROCS='2',GOMEMLIMIT='2GiB',WF_FANOUT_COMBINED_ROOT=str(root/'originals'),FAULT_SEED=str(args.seed))
if args.physical_drain:env['WF_FANOUT_PHYSICAL_DRAIN']='1'
if args.diagnostic_trace:env['WF_FANOUT_DIAGNOSTIC_TRACE']='1'
selected_case=args.case
selection='^TestFiveHundredChildFanoutCombinedParentAndJournalBoundaryMatrix$'
if args.case:selection+='/'+ '/'.join('^'+part+'$' for part in args.case.split('/'))
build=['go','test','-race','-p=1','-buildvcs=true','-c','-o',str(root/'integration.test'),'./integration']
with (root/'build.log').open('w') as log:subprocess.run(build,cwd=repo,env=env,stdout=log,stderr=subprocess.STDOUT,check=True)
info=subprocess.check_output(['go','version','-m',str(root/'integration.test')],text=True);assert 'vcs.modified=false' in info and f'vcs.revision={revision}' in info and '-race=true' in info
(root/'binary.json').write_text(json.dumps({'sha256':sha(root/'integration.test'),'build_info':info},indent=2)+'\n')
args=[str(root/'integration.test'),'-test.run='+selection,'-test.count=1','-test.v','-test.timeout=35m']
(root/'commands.json').write_text(json.dumps({'build':build,'test':args,'selected_case':selected_case,'full_six_boundary_selection':selected_case is None,'environment':{k:v for k,v in env.items() if k.startswith('WF_') or k in ['GOMEMLIMIT','GOMAXPROCS','GOCACHE','FAULT_SEED']}},indent=2)+'\n')
with (root/'native.log').open('w') as log:
 p=subprocess.Popen(args,cwd=repo,env=env,stdout=log,stderr=subprocess.STDOUT);exe=Path(f'/proc/{p.pid}/exe');actual={'pid':p.pid,'sha256':sha(exe),'exe':os.readlink(exe),'build_info':subprocess.check_output(['go','version','-m',str(exe)],text=True),'source':revision,'selected_case':selected_case,'full_six_boundary_selection':selected_case is None,'status':'running','started_utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'native_R3_servers_embedded_in_actual_sdk':True,'separate_server_process_hashes':False};assert actual['sha256']==sha(root/'integration.test');(root/'execution.json').write_text(json.dumps(actual,indent=2)+'\n');print('NATIVE_STARTED',p.pid,revision,flush=True)
 code=p.wait();actual.update(status='passed' if code==0 else 'failed',exit_code=code,finished_utc=datetime.datetime.now(datetime.timezone.utc).isoformat());(root/'execution.json').write_text(json.dumps(actual,indent=2)+'\n')
after={n:sha(repo/n) for n in names};assert before==after;(root/'source-after.json').write_text(json.dumps({'revision':revision,'files':after},indent=2)+'\n')
external_after={n:sha(Path(n)) for n in inputs};assert inputs==external_after;(root/'external-source-after.json').write_text(json.dumps(external_after,indent=2)+'\n');assert Path(__file__).read_bytes()==producer_bytes;shutil.copy2(__file__,root/'executed-producer.py');print('NATIVE_FINISHED',code,flush=True)

raise SystemExit(code)
