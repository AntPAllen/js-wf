from pathlib import Path
import subprocess,json,shutil,time,os
repo=Path('/home/exedev/js-wf');source=Path('/tmp/storage-sixth-proofs');target=repo/'docs/scale/tmp-storage-review-2026-10-06/sixth-closed-roots'
run=lambda args,**kw:subprocess.run(args,cwd=repo,check=True,**kw)
def commit(message):
 run(['git','add',str(target.relative_to(repo)),'docs/scale/tmp-storage-review-2026-10-06/sixth-verified-staging-removal'])
 run(['git','commit','-m',message]);run(['git','push','origin','main'])
# Source capture must have completed before any repository mutation.
while Path('/proc/1664461').exists() or Path('/proc/1685193').exists():time.sleep(10)
assert not Path('/proc/1664461').exists()
assert not subprocess.check_output(['git','status','--porcelain'],cwd=repo).strip()
assert subprocess.check_output(['git','branch','--show-current'],cwd=repo,text=True).strip()=='main'
assert subprocess.check_output(['git','rev-parse','HEAD'],cwd=repo,text=True).strip()==subprocess.check_output(['git','ls-remote','origin','refs/heads/main'],cwd=repo,text=True).split()[0]
target.mkdir(parents=True,exist_ok=True)
shutil.copyfile('/tmp/storage-capture-sixth.py',target/'executed-capture.py')
shutil.copyfile('/tmp/storage-sixth-candidates.json',target/'initial-candidates.json')
shutil.copyfile('/tmp/storage-sixth-du-errors.txt',target/'inventory-inspection-limits.txt')
shutil.copytree('/tmp/storage-sixth-staging-proof',target.parent/'sixth-verified-staging-removal',dirs_exist_ok=True)
batch=len(list(target.glob('*/capture.json')))//20
while True:
 ready=[p for p in sorted(source.iterdir()) if p.is_dir() and (p/'capture.json').is_file() and not (target/p.name).exists()]
 live=Path('/proc/1696966').exists()
 if len(ready)<20 and live:time.sleep(10);continue
 if not ready:break
 batch+=1
 for p in ready[:20]:shutil.copytree(p,target/p.name)
 commit('Preserve closed temporary fixture inventories, batch '+str(batch))
 run(['python3','/tmp/storage-upload-sixth.py'])
 commit('Record verified S3 copies for temporary fixtures, batch '+str(batch))
 run(['python3','/tmp/storage-remove-sixth.py'],env=dict(os.environ,AWS_ACCESS_KEY_ID='x',AWS_SECRET_ACCESS_KEY='x'))
 commit('Remove verified local temporary fixture copies, batch '+str(batch))
 print('BATCH_FINISHED',batch,flush=True)
shutil.copyfile('/tmp/storage-capture-sixth.log',target/'capture.log')
shutil.copyfile(__file__,target/'executed-pipeline.py')
print('STORAGE_PIPELINE_DONE',batch,flush=True)
