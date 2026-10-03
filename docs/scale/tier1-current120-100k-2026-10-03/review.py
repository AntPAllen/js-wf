"""Verify the current120 campaign using exact Git sources and prior compiled inventory."""
import hashlib,json,subprocess,sys,tarfile,importlib.util
from pathlib import Path
root=Path(sys.argv[1]); evidence=root/'tier1-extended-suite-evidence'
revision='ad37bfc492ec8726232f8c9e7850983fe82cd6e7'; prior_revision='3a5452e14924e413c10ed1504c7da2a55549b196'
assert (evidence/'tier1-source.txt').read_text().strip()==revision
assert not subprocess.check_output(['git','diff','--name-only',prior_revision,revision,'--','*.go','go.mod','go.sum'])
prior=Path('/tmp/js-wf-tier1-120-1k-default-3a5452e-20261002')
for old,new in [('inventory.txt','tier1-inventory.txt'),('seeded-inventory.txt','tier1-seeded-inventory.txt'),('regression-inventory.txt','tier1-regression-inventory.txt')]:
    assert (prior/old).read_bytes()==(evidence/new).read_bytes()
    (root/('independent-'+old)).write_bytes((prior/old).read_bytes())
source={}
for name in subprocess.check_output(['git','ls-tree','-r','--name-only',revision],text=True).splitlines():
    if name.endswith('.go') or name in ('go.mod','go.sum','scripts/check-tier1-suite.py','.github/workflows/tier1-extended.yml') or name.startswith('sim/testdata/regressions/') and name.endswith('.json'):
        b=subprocess.check_output(['git','show',revision+':'+name]); source[name]=hashlib.sha256(b).hexdigest()
        path=root/'source'/name; path.parent.mkdir(parents=True,exist_ok=True); path.write_bytes(b)
(root/'source-hashes.json').write_text(json.dumps(source,indent=2)+'\n')
assert sorted(n for n in source if n.startswith('sim/testdata/regressions/'))==(evidence/'tier1-regression-inventory.txt').read_text().splitlines()
subprocess.run([sys.executable,str(root/'source/scripts/check-tier1-suite.py'),'--events',str(evidence/'tier1-events.jsonl'),'--inventory',str(root/'independent-inventory.txt'),'--regressions',str(root/'independent-regression-inventory.txt'),'--seeded-inventory',str(root/'independent-seeded-inventory.txt'),'--source',str(evidence/'tier1-source.txt'),'--seeds','100000','--output',str(root/'independent-result.json')],check=True)
assert (root/'independent-result.json').read_bytes()==(evidence/'tier1-result.json').read_bytes()
r=json.loads((root/'independent-result.json').read_text()); assert r['per_workload_seed_proof']['workloads']==120 and r['pinned_regressions_pass']==266
terminal=json.loads((root/'terminal.json').read_text()); assert terminal['status']=='completed' and terminal['conclusion']=='success' and terminal['headSha']==revision
assert all(j['status']=='completed' and j['conclusion']=='success' for j in terminal['jobs'])
events=[json.loads(l) for l in (evidence/'tier1-events.jsonl').read_text().splitlines()]
seconds=next(e['Elapsed'] for e in events if e['Action']=='pass' and not e.get('Test'))
review=dict(source=revision,actual_package_seconds=seconds,exact_git_source_files=len(source),compiled_and_AST_inventory_reused_from=prior_revision,all_go_and_module_sources_byte_identical_to_prior=True,regenerated_report_byte_identical=True,workloads=120,completed_bodies=12000000,pins=266,aggregate_counts=r['aggregate_counts'],scope='Accepted current120 Tier1 100k gate; no whole-package race, real-server or full release claim.')
(root/'independent-review.json').write_text(json.dumps(review,indent=2)+'\n'); print(json.dumps(review,indent=2))
