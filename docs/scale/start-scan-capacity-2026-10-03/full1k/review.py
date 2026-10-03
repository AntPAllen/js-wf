#!/usr/bin/env python3
import argparse,hashlib,json,os,subprocess,tempfile,types
from pathlib import Path
p=argparse.ArgumentParser();p.add_argument('root',type=Path);p.add_argument('--repo',type=Path,default=Path('/home/exedev/js-wf'));p.add_argument('--output',type=Path);a=p.parse_args();root=a.root.resolve();repo=a.repo.resolve()
source=(root/'tier1-source.txt').read_text().strip()
def gitbytes(name):return subprocess.check_output(['git','show',source+':'+name],cwd=repo)
names=subprocess.check_output(['git','ls-tree','-r','--name-only',source],cwd=repo,text=True).splitlines()
selected=[n for n in names if n.endswith(('.go','.py','.yml')) or n in ('go.mod','go.sum') or n.startswith('sim/testdata/')]
expected={n:hashlib.sha256(gitbytes(n)).hexdigest() for n in selected}
before=json.loads((root/'source-before.json').read_text());after=json.loads((root/'source-after.json').read_text())
assert before==after==dict(revision=source,files=expected)
assert (root/'tier1-regression-inventory.txt').read_text().splitlines()==[n for n in names if n.startswith('sim/testdata/regressions/') and n.endswith('.json')]
provenance=json.loads((root/'binary.json').read_text());binary=root/'sim.test'
assert hashlib.sha256(binary.read_bytes()).hexdigest()==provenance['binary_sha256']
info=subprocess.check_output(['go','version','-m',str(binary)],text=True)
assert info.splitlines()[1:]==provenance['build_info'].splitlines()[1:]
assert ('-race=true' in info)==provenance['race_instrumented']
env=dict(os.environ,SIM_COVERAGE_SUMMARY='0',GOMAXPROCS='2',GOMEMLIMIT='512MiB')
listing=subprocess.check_output([str(binary),'-test.list=^Test'],cwd=repo,env=env,text=True)
assert listing==(root/'tier1-list.log').read_text()
assert (root/'tier1-inventory.txt').read_text()=='\n'.join(l for l in listing.splitlines() if l.startswith('Test'))+'\n'
with tempfile.TemporaryDirectory() as directory:
 temp=Path(directory);(temp/'sim').mkdir()
 for name in names:
  if name.startswith('sim/') and name.endswith('_test.go'):(temp/name).write_bytes(gitbytes(name))
 (temp/'inventory.go').write_bytes(gitbytes('scripts/tier1-seed-inventory/main.go'))
 seeded=subprocess.check_output(['go','run',str(temp/'inventory.go')],cwd=temp,env=dict(env,GO111MODULE='off'),text=True)
 assert seeded==(root/'tier1-seeded-inventory.txt').read_text()
module=types.ModuleType('recorded_guard');exec(compile(gitbytes('scripts/check-tier1-suite.py'),'recorded_guard','exec'),module.__dict__)
report=module.check([json.loads(line) for line in (root/'tier1-events.jsonl').read_text().splitlines()],(root/'tier1-inventory.txt').read_text(),int(provenance['environment']['SIM_SEEDS']),source,(root/'tier1-regression-inventory.txt').read_text(),seeded)
report['events_sha256']=hashlib.sha256((root/'tier1-events.jsonl').read_bytes()).hexdigest()
assert report==json.loads((root/'tier1-result.json').read_text())
commands=json.loads((root/'commands.json').read_text());contexts=json.loads((root/'execution-contexts.json').read_text())
assert [c['command'] for c in contexts]==commands
build=commands[0];assert build[:3]==['go','test','-p=1'] and ('-race' in build)==provenance['race_instrumented']
original_binary=build[build.index('-o')+1]
execution=[c for c in contexts if 'test2json' in c['command']];assert len(execution)==1
assert execution[0]['command'][execution[0]['command'].index('js-wf/sim')+1]==original_binary
assert Path(execution[0]['working_directory']).name=='sim'
result=dict(accepted=True,source=source,source_files_verified=len(expected),retained_binary_sha256=provenance['binary_sha256'],race_instrumented=provenance['race_instrumented'],suite_report_regenerates=True,compiled_inventory_matches_binary=True,seed_inventory_matches_exact_source=True,scope='Complete requested seed suite from retained binary; real-cluster matrices and24h qualification remain separate.',suite=report)
output=json.dumps(result,indent=2)+'\n'
if a.output:a.output.write_text(output)
else:print(output,end='')
