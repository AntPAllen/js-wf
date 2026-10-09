"""Verify current component evidence; never infer full-suite acceptance."""
import hashlib,json,re,subprocess
from pathlib import Path
base=Path(__file__).resolve().parent
repo=base.parents[2]
observed=json.loads((base/'acknowledged-before.json').read_text())
for path,digest in observed['files'].items():
 assert hashlib.sha256((repo/path).read_bytes()).hexdigest()==digest,path
packages=['js-wf/client','js-wf/cmd/wf','js-wf/cmd/wf-worker']
required=['TestGraphAwaitContentionFreshReadAndGenerationFence','TestGraphAwaitOwnReadDeadline','TestNativeGraphCursorOperatorHistory','TestWorkerRunnerCanonicalGraphRepair','TestWorkerRunnerCanonicalCheckpointGraphRepair']
logs={}
for kind in ['normal','race']:
 exit_code=json.loads((base/('acknowledged-'+kind+'.exit.json')).read_text())['exit_code']
 assert exit_code == (0 if kind=='normal' else 1)
 path=base/('acknowledged-'+kind+'.log');s=path.read_text()
 assert 'WARNING: DATA RACE' not in s and not re.search(r'^panic:',s,re.M),path
 if kind=='normal': assert not re.search(r'^\s*--- FAIL:|^FAIL(?:\s|$)',s,re.M),path
 else: assert re.search(r'^FAIL\s+js-wf/cmd/wf-worker\s+[\d.]+s$',s,re.M),path
 for package in (packages if kind=='normal' else packages[:2]): assert re.search(r'^ok\s+'+re.escape(package)+r'\s+[\d.]+s$',s,re.M),(path,package)
 for test in (required if kind=='normal' else required[:3]): assert re.search(r'^--- PASS: '+test+r' \(',s,re.M),(path,test)
 for version in [4,5,6]:
  for rep in [1,3]:
   test=f'TestNativeGraphCursorOperatorHistory/v{version}/R{rep}-domain'
   assert re.search(r'^\s*--- '+('PASS' if kind=='normal' or not test.startswith('TestWorker') or rep=='R1' else 'FAIL')+r': '+re.escape(test)+r' \(',s,re.M),(path,test)
 for version in [5,6]:
  for rep in ['R1','R3Domain']:
   test=f'TestWorkerRunnerCanonicalCheckpointGraphRepair/v{version}/{rep}'
   assert re.search(r'^\s*--- '+('PASS' if kind=='normal' or not test.startswith('TestWorker') or rep=='R1' else 'FAIL')+r': '+re.escape(test)+r' \(',s,re.M),(path,test)
 for rep in ['R1','R3Domain']:
  test=f'TestWorkerRunnerCanonicalGraphRepair/{rep}'
  assert re.search(r'^\s*--- '+('PASS' if kind=='normal' or not test.startswith('TestWorker') or rep=='R1' else 'FAIL')+r': '+re.escape(test)+r' \(',s,re.M),(path,test)
 for version in [4,6]:
  for mode in ['observe','pinned','release','generation','purge','cancel','unknown']:
   test=f'TestGraphAwaitContentionFreshReadAndGenerationFence/v{version}/{mode}'
   assert re.search(r'^\s*--- '+('PASS' if kind=='normal' or not test.startswith('TestWorker') or rep=='R1' else 'FAIL')+r': '+re.escape(test)+r' \(',s,re.M),(path,test)
 for mode in ['deadline','deadline-corrupt']:
  test=f'TestGraphAwaitOwnReadDeadline/{mode}'
  assert re.search(r'^\s*--- '+('PASS' if kind=='normal' or not test.startswith('TestWorker') or rep=='R1' else 'FAIL')+r': '+re.escape(test)+r' \(',s,re.M),(path,test)
 logs[kind]={'exit_code':exit_code,'sha256':hashlib.sha256(path.read_bytes()).hexdigest(),'package_times':re.findall(r'^ok\s+(\S+)\s+([\d.]+s)$',s,re.M)}
review={'verdict':'FAIL focused native worker race qualification; normal and client/operator race passed','repository_inputs_unchanged':len(observed['files']),'logs':logs,'directed_cases_per_seed':14,'directed_seeds':16,'directed_bodies':224,'exact_replays':224,'native_operator_cases':6,'native_worker_cases':6,'scope':'All native worker R3 race cases miss the unchanged two-minute deadline. Their cause remains unconfirmed; no complete current qualification or deployment admission. Explicit operator/ordinary-worker cursor selection, owned archive history and Await contention/deadline decisions only. Shared Tier-1 integration, full current qualification, continuation CLI execution/public admission, import/deployment migration, original native matrices and every broader release gate remain open.'}
(base/'review.json').write_text(json.dumps(review,indent=2)+'\n')
print(json.dumps(review))
