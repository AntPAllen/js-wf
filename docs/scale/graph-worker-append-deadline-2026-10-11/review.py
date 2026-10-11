import hashlib,json,re,subprocess
from pathlib import Path
import yaml
out=Path(__file__).resolve().parent
inputs=json.loads((out/'source-inputs.json').read_text())
required={str(p) for p in Path('.').rglob('*') if p.is_file() and (p.suffix=='.go' or str(p) in ('go.mod','go.sum','.github/workflows/graph-publication.yml') or str(p).startswith('sim/testdata/'))}
assert set(inputs)==required
for name,sha in inputs.items():assert hashlib.sha256(Path(name).read_bytes()).hexdigest()==sha,name
seeded=subprocess.check_output(['go','run','./scripts/tier1-seed-inventory'])
assert seeded==(out/'seeded-inventory.txt').read_bytes() and len(seeded.splitlines())==163
assert b'TestSeededGraphWorkerAppendDeadlineReplay\n' in seeded
pins=list(Path('sim/testdata/regressions').glob('graph_worker_append_deadline_*.json'))
assert len(pins)==24 and len(list(Path('sim/testdata/regressions').glob('*.json')))==946
workflow=yaml.safe_load(Path('.github/workflows/graph-publication.yml').read_text())
run=next(step['run'] for step in workflow['jobs']['seeded-worker-append-deadline']['steps'] if 'run' in step)
guard=run.split("python3 - <<'PY'\n",1)[1].rsplit('\nPY',1)[0]
seconds={}
for mode in ('normal','race'):
 receipt=json.loads((out/(mode+'-command.json')).read_text());exe=json.loads((out/(mode+'-executable.json')).read_text())
 assert receipt['actual_exit']==0 and receipt['executable_captured']
 assert exe['parent_pid']==receipt['driver_pid'] and receipt['start_ns']<=exe['captured_ns']<=receipt['end_ns']
 assert ('-race=true' in exe['build'])==(mode=='race')
 assert '/sim.test ' in exe['command']
 assert receipt['environment']['SIM_SEEDS']=='1000' and receipt['environment']['GOMAXPROCS']=='2' and receipt['environment']['GOMEMLIMIT']=='512MiB'
 log=(out/(mode+'.log')).read_text();assert not re.search(r'^\s*--- (FAIL|SKIP):',log,re.M)
 exec(guard.replace("pathlib.Path('worker-append-deadline.log').read_text()",'log'),{'log':log})
 assert 'WORKER_APPEND_DEADLINE modes=' in log
 seconds[mode]=float(re.search(r'ok\s+js-wf/sim\s+([\d.]+)s',log).group(1))
source=Path('worker/worker.go').read_text()
assert source.encode()==subprocess.check_output(['git','show','2035d52:worker/worker.go'])
old='if !errors.Is(err, graphpublication.ErrConflict) || errors.Is(err, journal.ErrUnknown) || errors.Is(err, graphpublication.ErrRevoked) || appendCtx.Err() != nil {'
new='if (!errors.Is(err, graphpublication.ErrConflict) && !errors.Is(err, journal.ErrUnknown)) || errors.Is(err, graphpublication.ErrRevoked) || appendCtx.Err() != nil {'
assert source.count(old)==1 and (out/'retry-unknown-worker.go.txt').read_text()==source.replace(old,new)
assert json.loads((out/'retry-unknown-overlay.json').read_text())=={'Replace':{str(Path('worker/worker.go').resolve()):str(out/'retry-unknown-worker.go.txt')}}
for name,want in (('retry-unknown',1),('retry-unknown-replay',1),('retry-unknown-minimize',0),('retry-unknown-minimized-replay',1)):
 receipt=json.loads((out/(name+'-command.json')).read_text());assert receipt['actual_exit']==want
 log=(out/(name+'.log')).read_text()
 assert '--- SKIP:' not in log
 if want:assert 'uncertain append retried after 1 renewals' in log and '--- FAIL:' in log
 else:assert '--- PASS: TestMinimizeFaultTrace ' in log and 'minimized ' in log
for name in ('retry-unknown-failure.json','retry-unknown-minimized.json'):
 assert json.loads((out/name).read_text())['workload']=='graph_worker_append_deadline'
s3=json.loads((out/'wrong-partition-s3.json').read_text())
assert s3['upload_exit']==s3['fresh_remote_read_exit']==0 and s3['remote_hash_verified']
assert s3['uncompressed']==json.loads((out/'wrong-partition-trace.json').read_text())
result=dict(accepted_focused_family=True,source_inputs_unchanged=len(inputs),current_seeded_families=163,current_pins=946,new_pins=24,completed_seed_bodies_each_mode=1000,exact_current_ci_guard=True,actual_executables=2,mutation_killed=True,failure_replay_and_minimization=True,production_worker_unchanged=True,wrong_partition_failure_s3_preserved=True,native_cause_confirmed=False,full_current_qualification=False,test_seconds=seconds)
(out/'review.json').write_text(json.dumps(result,indent=2)+'\n');print(json.dumps(result))
