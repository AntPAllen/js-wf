import pathlib,subprocess,os,hashlib,json,time,sys
root=pathlib.Path(sys.argv[1]); root.mkdir(exist_ok=True)
repo=pathlib.Path('/home/exedev/js-wf')
files=subprocess.check_output(['git','ls-files','--cached','--others','--exclude-standard','-z'],cwd=repo).decode().split('\0')
files=sorted(set(p for p in files if p and not p.startswith('docs/scale/') and (repo/p).is_file())|{'integration/result_absence_test.go'})
def snapshot():
 return {p:hashlib.sha256((repo/p).read_bytes()).hexdigest() for p in files}
before=snapshot();(root/'source-before.json').write_text(json.dumps(before,indent=2))
subprocess.run(['tar','-czf',str(root/'source.tar.gz'),*files],cwd=repo,check=True)
for args,name in [(['git','status','--porcelain=v1'],'git-status.txt'),(['git','diff','--binary'],'git-diff.patch'),(['git','rev-parse','HEAD'],'revision.txt'),(['go','version','-m',str(root/'integration.test')],'sdk-build.txt')]:
 (root/name).write_bytes(subprocess.check_output(args,cwd=repo))
env=os.environ.copy();env.update(WF_RESULT_ABSENCE_ROOT=str(root),GOMAXPROCS='2',GOMEMLIMIT='2GiB')
with (root/'native.log').open('wb') as log:
 p=subprocess.Popen([str(root/'integration.test'),'-test.run=^TestResultReadVerifiesWeakAbsence$','-test.v','-test.timeout=90s'],cwd=repo,env=env,stdout=log,stderr=subprocess.STDOUT)
 proc=pathlib.Path('/proc')/str(p.pid)
 (root/'sdk-live.json').write_text(json.dumps({'pid':p.pid,'exe':os.readlink(proc/'exe'),'sha256':hashlib.sha256((proc/'exe').read_bytes()).hexdigest(),'cmdline':(proc/'cmdline').read_bytes().decode().split('\0'),'environment':(proc/'environ').read_bytes().decode().split('\0')},indent=2))
 code=p.wait()
after=snapshot();(root/'source-after.json').write_text(json.dumps(after,indent=2))
(root/'execution.json').write_text(json.dumps({'exit_code':code,'source_unchanged':before==after},indent=2))
print(json.dumps({'root':str(root),'exit_code':code,'source_unchanged':before==after}))
print((root/'native.log').read_text())
