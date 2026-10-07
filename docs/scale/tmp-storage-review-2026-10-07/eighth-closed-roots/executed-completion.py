from pathlib import Path
import subprocess,sys,json,shutil,time,os
sys.dont_write_bytecode=True
repo=Path('/home/exedev/js-wf');base=repo/'docs/scale/tmp-storage-review-2026-10-07';out=base/'eighth-closed-roots'
def run(args,**kw):return subprocess.run(args,cwd=repo,check=True,**kw)
def commit(message):
 run(['git','add',str(base.relative_to(repo))]);run(['git','commit','-m',message]);run(['git','push','origin','main'])
while Path('/proc/2371560').exists():time.sleep(10)
assert 'STORAGE_PIPELINE_DONE' in Path('/tmp/storage-eighth-pipeline.log').read_text() or (out/'report-copy-correction.json').exists()
assert len(list(out.glob('*/offload.json')))==285
assert not Path('/proc/2320334').exists()
log=Path('/tmp/storage-eighth-capture.log').read_text()
assert 'Traceback' not in log
names=json.loads(Path('/tmp/storage-eighth-names.json').read_text())
finished={line.split()[1] for line in log.splitlines() if line.startswith(('CAPTURED ','SKIPPED_SYMLINK ','SKIPPED_ACTIVE '))}
assert finished==set(names),(len(finished),len(names),set(names)-finished)
assert not subprocess.check_output(['git','status','--porcelain'],cwd=repo).strip()
run(['python3','/tmp/storage-staging-eighth.py'],env=dict(os.environ,AWS_ACCESS_KEY_ID='x',AWS_SECRET_ACCESS_KEY='x'))
staging=base/'eighth-verified-staging-removal';shutil.copytree('/tmp/storage-eighth-staging-proof',staging)
shutil.copyfile('/tmp/storage-eighth-staging-plan.json',staging/'selected-archives.json')
shutil.copyfile('/tmp/storage-pipeline-eighth-resumed.py',out/'executed-resumed-pipeline.py')
for kind in ('capture','upload','remove','pipeline'):
 shutil.copyfile('/tmp/storage-'+kind+'-eighth.py',out/('executed-'+kind+'.py'))
run(['python3','/tmp/storage-finish-eighth.py'])
shutil.copyfile(__file__,out/'executed-completion.py')
commit('Summarize further verified temporary fixture and archive cleanup')
run(['python3','/tmp/storage-remove-metadata-eighth.py'])
commit('Remove pushed duplicate cleanup metadata staging')
assert not subprocess.check_output(['git','status','--porcelain'],cwd=repo).strip()
assert subprocess.check_output(['git','rev-parse','HEAD'],cwd=repo,text=True).strip()==subprocess.check_output(['git','ls-remote','origin','refs/heads/main'],cwd=repo,text=True).split()[0]
print('ALL_CLEANUP_FINISHED',flush=True)
