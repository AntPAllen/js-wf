from pathlib import Path
import hashlib,json,subprocess,shutil,sys
root=Path(sys.argv[1])
repo=Path('/home/exedev/js-wf')
model=Path('/tmp/js-wf-tier3-model-79915ca')
source='79915ca41a5c5a23b9997eea5f3f66d82530ee30'
r=json.loads((root/'row-review.json').read_text())
first,last=r['first_seed'],r['last_seed']
assert r['shard_qualified'] and r['source']==source and r['row']=='block_disk' and r['seeds']==last-first+1
helper=root/'history-review.go'
review_source=subprocess.check_output(['git','rev-parse','HEAD'],cwd=repo,text=True).strip()
helper_bytes=subprocess.check_output(['git','show',review_source+':scripts/tier2-history-review.go.txt'],cwd=repo)
assert helper_bytes==(repo/'scripts/tier2-history-review.go.txt').read_bytes()
helper.write_bytes(helper_bytes)
helper_git_sha256=hashlib.sha256(helper_bytes).hexdigest()
deps=subprocess.check_output(['go','list','-deps','-f','{{.Dir}}|{{join .GoFiles " "}}|{{join .CgoFiles " "}}',str(helper)],cwd=model,text=True)
names={'go.mod','go.sum'}
for line in deps.splitlines():
    directory,go,cgo=line.split('|');directory=Path(directory)
    if directory.is_relative_to(model):
        names.update(str((directory/n).relative_to(model)) for n in (go+' '+cgo).split())
def sha(p):
    with p.open('rb') as f:return hashlib.file_digest(f,'sha256').hexdigest()
hashes={n:sha(model/n) for n in sorted(names)}
for n,d in hashes.items():
    assert hashlib.sha256(subprocess.check_output(['git','show',source+':'+n],cwd=repo)).hexdigest()==d
    p=root/'model-source'/n;p.parent.mkdir(parents=True,exist_ok=True);shutil.copyfile(model/n,p)
(root/'model-dependencies.json').write_text(json.dumps(dict(source=source,root=str(model),files=hashes),indent=2)+'\n')
binary=root/'history-review'
subprocess.run(['go','build','-p=1','-o',str(binary),str(helper)],cwd=model,check=True)
(root/'model-binary.json').write_text(json.dumps(dict(sha256=sha(binary),go_build_info=subprocess.check_output(['go','version','-m',str(binary)],text=True),source=source),indent=2)+'\n')
reports=[]
for seed in range(first,last+1):
    history=root/f'raw/tier3-matrix-range/seed-{seed}/tier3-mixed-journal/history.jsonl'
    operations=sum(bool(l.strip()) for l in history.read_text().splitlines())
    p=subprocess.run([str(binary),str(history)],capture_output=True,text=True,timeout=120)
    (root/f'model-seed-{seed}.stdout').write_text(p.stdout);(root/f'model-seed-{seed}.stderr').write_text(p.stderr)
    assert p.returncode==0 and p.stdout.splitlines()==[f'whole {n} operations={operations} verdict=Ok error=<nil>' for n in ['starts','signals','results']] and not p.stderr
    reports.append(dict(seed=seed,operations=operations,all_three_exact_ok=True))
for n,d in hashes.items():assert sha(model/n)==sha(root/'model-source'/n)==d
for p in (repo/'scripts').glob('*.py'):
    dest=root/'reviewer/scripts'/p.name;dest.parent.mkdir(parents=True,exist_ok=True);shutil.copyfile(p,dest)
shutil.copyfile(__file__,root/'executed-model-review.py')
(root/'model-review.json').write_text(json.dumps(dict(source=source,all_three_models_exact_ok=True,actual_dependencies=len(hashes),operations=sum(x['operations'] for x in reports),reports=reports,review_helper_git_source=review_source,review_helper_git_sha256=helper_git_sha256,actual_workload_sdk_retained=False,physical_stores_retained=False,final_integrity_and_drain_scope='named-test assertions'),indent=2)+'\n')
print('MODEL COMPLETE',len(hashes),sum(x['operations'] for x in reports),flush=True)
