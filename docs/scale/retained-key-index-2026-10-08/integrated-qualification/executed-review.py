import json,hashlib,subprocess,shutil
from pathlib import Path
repo=Path('/home/exedev/js-wf');base=repo/'docs/scale/retained-key-index-2026-10-08/integrated-qualification'
before=json.loads((base/'source-before.json').read_text());after=json.loads((base/'source-after.json').read_text());result=json.loads((base/'results.json').read_text())
assert before['revision']==after['revision']==result['source'] and before['files']==after['files'] and before['selected_inputs_match_git'] and after['unchanged']
for name,digest in before['files'].items():
 assert hashlib.sha256((repo/name).read_bytes()).hexdigest()==digest
 assert hashlib.sha256(subprocess.check_output(['git','cat-file','blob',result['source']+':'+name],cwd=repo)).hexdigest()==digest
checks=[]
expected={'TestPersistentIndexMatchesMapAndCapturedPopulations','TestIndexMaximumDepthAndPacketBound','TestIndexCorruptionUncertaintyAndCancellation','TestNativeOwnedIndexReopenedSnapshotAndPhysicalDrain','TestOwnedIndexSnapshotsForksRetirementAndDrain','TestEmptyOwnedIndexRequiresLivePin'}
assert [run['mode'] for run in result['runs']]==['normal','race']
for run in result['runs']:
 assert run['exit_code']==0 and run['environment']==dict(GOMAXPROCS='2',GOMEMLIMIT='512MiB') and '-count=1' in run['command'] and '-timeout=5m' in run['command'] and ('-race' in run['command'])==(run['mode']=='race')
 rows=[json.loads(x) for x in (base/(run['mode']+'.jsonl')).read_text().splitlines()];assert not any(r.get('Action') in ('skip','fail','build-fail') for r in rows)
 passed={r['Test'] for r in rows if r.get('Action')=='pass' and r.get('Test')};assert {name for name in passed if '/' not in name}==expected
 for seed in range(1,33):assert 'TestPersistentIndexMatchesMapAndCapturedPopulations/seed'+str(seed) in passed
 for replicas in (1,3):assert 'TestNativeOwnedIndexReopenedSnapshotAndPhysicalDrain/R'+str(replicas) in passed
 output=''.join(r.get('Output','') for r in rows);assert 'DATA RACE' not in output and output.count('zero objects and physical chunks')==2
 assert (base/(run['mode']+'.stderr')).read_bytes()==b'' and len(run['package_passes'])==1
 checks.append(dict(mode=run['mode'],package=run['package_passes'][0],top_groups=6,wall_seconds=run['wall_seconds']))
report=dict(source=result['source'],selected_inputs=len(before['files']),committed_inputs_unchanged=True,all_commands_pass=True,checks=checks,map_property_seeds=32,updates_per_seed=512,maximum_copied_nodes=257,maximum_packet_bytes=20576,native_replicas=[1,3],scope='Bounded persistent same-forest key index codec and live-pin reader adapter only. Native reopens retained8-key/input snapshot after16 appends and retirement, rejects released pin and checks complete physical subject census has zero chunks. Explicit fixture clock advances and collection are not production GC or process/server fault qualification. Signal reservation/publication/binding/ordered drain, native concurrency and scale, shared seeded workload/corpus integration, import and all original acceptance gates remain open.')
(base/'review.json').write_text(json.dumps(report,indent=2)+'\n');shutil.copyfile(__file__,base/'executed-review.py');print(json.dumps(checks))
