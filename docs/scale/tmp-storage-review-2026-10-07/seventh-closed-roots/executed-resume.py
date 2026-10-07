from pathlib import Path
import subprocess,time,os,signal,shutil
repo=Path('/home/exedev/js-wf');out=repo/'docs/scale/tmp-storage-review-2026-10-07/seventh-closed-roots'
while Path('/proc/2071144').exists() and Path('/proc/2071144/stat').read_text().split()[2]!='Z':time.sleep(5)
os.kill(2043333,signal.SIGTERM)
os.kill(2043333,signal.SIGCONT)
for source,target in [('storage-capture-seventh-initial.py','executed-capture-initial.py'),('storage-remove-seventh-initial.py','executed-offload-initial.py'),('storage-seventh-capture-initial.log','initial-capture-failure.log'),('storage-pipeline-seventh-initial.py','executed-pipeline-initial.py')]:shutil.copyfile(Path('/tmp')/source,out/target)
(out/'closure-retry-context.txt').write_text('The initial archive collector stopped when a live-soak container disappeared between docker ps and docker inspect. The resumed closure function retries up to five fresh complete Docker lists, aborting if none stabilize. An already captured archive without a terminal capture record is fully reverified against its manifest and current original files before a fresh successful closure record is written. No historical test verdict or original byte was repaired. The cleanup orchestrator was paused while its current removal child finished, then resumed from committed per-root receipts and removal records.\n')
subprocess.run(['git','add',str(out.relative_to(repo))],cwd=repo,check=True)
subprocess.run(['git','commit','-m','Record verified cleanup progress and Docker closure retry'],cwd=repo,check=True)
subprocess.run(['git','push','origin','main'],cwd=repo,check=True)
p=Path('/tmp/storage-remove-seventh.py');s=p.read_text();old=" running=json.loads(subprocess.check_output(['docker','inspect',*ids],text=True)) if ids else []";new=Path('/tmp/storage-seventh-docker-closure-replacement.txt').read_text();assert old in s;s=s.replace(old,new).replace('running_docker_ids=ids)', 'running_docker_ids=ids,docker_inspection_retries=docker_retries)');s=s.replace("or v['bytes']>=1048576)", "or v['bytes']>=1048576 or (k.endswith('.log') and v['bytes']>=65536))");p.write_text(s)
subprocess.run(['python3','/tmp/storage-pipeline-seventh-resumed.py'],cwd=repo,check=True)
