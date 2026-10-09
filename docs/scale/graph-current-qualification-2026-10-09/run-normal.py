"""Run and verify the full normal Tier1 suite from an isolated frozen checkout."""
import hashlib, json, os, pathlib, subprocess, time
source = pathlib.Path('/home/exedev/js-wf-compaction-qualification')
out = pathlib.Path('/home/exedev/js-wf-tier1-full149-normal1000-corrected-20261009')
out.mkdir(exist_ok=False)
revision = subprocess.check_output(['git','rev-parse','HEAD'],cwd=source,text=True).strip()
expected = 'fca8264d2229144e9b3e6a70df747d1307666fe6'
assert revision == expected
commands = []
env = dict(os.environ, SIM_SEEDS='1000', SIM_COVERAGE_SUMMARY='1', SIM_PROGRESS='1')
for key in ('FAULT_TRACE','SIM_GRAPH_COMPACTION_ROOT'):
    env.pop(key, None)
def write(name, value):
    (out/name).write_text(json.dumps(value, indent=2)+'\n')
def snapshot():
    assert not subprocess.check_output(['git','status','--porcelain'],cwd=source), 'Frozen source is dirty'
    selected = {}
    for row in subprocess.check_output(['git','ls-files','-s','-z'],cwd=source).split(b'\0'):
        if not row: continue
        metadata, name = row.split(b'\t',1)
        path = name.decode()
        if path.startswith('docs/') or path.startswith('.claude-artifacts/'): continue
        mode, blob, stage = metadata.decode().split()
        assert stage == '0'
        data = (source/path).read_bytes()
        actual_blob = hashlib.sha1(b'blob '+str(len(data)).encode()+b'\0'+data).hexdigest()
        assert actual_blob == blob, path
        selected[path] = hashlib.sha256(data).hexdigest()
    return {'revision':revision, 'files':selected}
def run(args, output, cwd=source):
    commands.append({"command":args,"working_directory":str(cwd)})
    write('commands.json', commands)
    print('RUN', args, flush=True)
    with (out/output).open('w') as stdout, (out/(output+'.stderr')).open('w') as stderr:
        code = subprocess.call(args,cwd=cwd,env=env,stdout=stdout,stderr=stderr)
    write(output+'.exit.json', {'exit_code':code})
    assert code == 0, (args, code)
write('source-before.json', snapshot())
(out/'tier1-source.txt').write_text(revision+'\n')
binary = str(out/'sim.test')
run(['go','test','-p=1','-c','-o',binary,'./sim'],'compile.log')
run([binary,'-test.list=^Test'],'tier1-list.log')
names = [x for x in (out/'tier1-list.log').read_text().splitlines() if x.startswith('Test')]
assert names and len(names)==len(set(names))
(out/'tier1-inventory.txt').write_text('\n'.join(names)+'\n')
run(['go','run','-p=1','./scripts/tier1-seed-inventory'],'tier1-seeded-inventory.txt')
pins = sorted(p.relative_to(source).as_posix() for p in (source/'sim/testdata/regressions').glob('*.json'))
(out/'tier1-regression-inventory.txt').write_text('\n'.join(pins)+'\n')
binary_hash = hashlib.sha256(pathlib.Path(binary).read_bytes()).hexdigest()
write('binary.json', {'binary_sha256':binary_hash, 'race_instrumented':False, 'source':revision, 'build_info':subprocess.check_output(['go','version','-m',binary],text=True), 'environment':{key:env[key] for key in ('SIM_SEEDS','SIM_COVERAGE_SUMMARY','SIM_PROGRESS')}})
start=time.time()
run(['/usr/bin/time','-o',str(out/'tier1-time.txt'),'-f','elapsed=%e user=%U system=%S','go','tool','test2json','-t','-p','js-wf/sim',binary,'-test.v=test2json','-test.count=1','-test.timeout=300m'],'tier1-events.jsonl', cwd=source/'sim')
write('source-after.json', snapshot())
assert json.loads((out/'source-before.json').read_text()) == json.loads((out/'source-after.json').read_text())
assert hashlib.sha256(pathlib.Path(binary).read_bytes()).hexdigest() == binary_hash
args=['python3','scripts/check-tier1-suite.py']
for flag, filename in [('events','tier1-events.jsonl'),('inventory','tier1-inventory.txt'),('regressions','tier1-regression-inventory.txt'),('source','tier1-source.txt'),('seeded-inventory','tier1-seeded-inventory.txt')]:
    args += ['--'+flag,str(out/filename)]
args += ['--seeds','1000','--output',str(out/'tier1-result.json')]
run(args,'verification.log')
result=json.loads((out/'tier1-result.json').read_text())
proof=result['per_workload_seed_proof']
assert proof['workloads']==149 and proof['completed_bodies']==149000 and result['pinned_regressions_pass']==762
write('review.json', {'source':revision,'verdict':'PASS','elapsed_seconds':time.time()-start,'source_and_binary_unchanged':True,'selected_inputs':len(snapshot()['files']),'scope':'Complete normal default Tier1 suite; race, extended campaigns and original wider requirements remain separate.','result':result})
print('VERIFIED PASS', flush=True)
