"""Independently verify complete source, binary and eight real package completions."""
import hashlib
import io
import json
import os
from pathlib import Path
import subprocess
import sys

base = Path(__file__).resolve().parent
mode = sys.argv[1]
assert mode in ('normal', 'race')
s = json.loads((base / (mode+'-state.json')).read_text())
assert s['mode'] == mode and s['phase'] == 'closed' and s['exit'] == 0 and s['finished']
checkout, root = Path(s['checkout']), Path(s['root'])
stage = root / (mode+'1000')
props = subprocess.check_output(['systemctl', '--user', 'show', 'js-wf-current-tier1-'+mode+'-20261010-qualified.service',
                                '-p', 'LoadState', '-p', 'MainPID', '-p', 'ExecMainStatus', '-p', 'InvocationID',
                                '-p', 'ExecMainExitTimestamp', '-p', 'CPUQuotaPerSecUSec', '-p', 'RemainAfterExit'], text=True)
assert 'LoadState=loaded\n' in props and 'MainPID=0\n' in props and 'ExecMainStatus=0\n' in props
assert 'InvocationID='+s['invocation']+'\n' in props
service = dict(line.split('=', 1) for line in props.splitlines())
assert service['ExecMainExitTimestamp'] and service['RemainAfterExit'] == 'yes'
launch = json.loads((base / (mode+'-launch.json')).read_text())
assert launch['source'] == s['source'] and launch['invocation'] == s['invocation']
assert launch['supervisor_pid'] == s['pid'] and launch['child_pid'] == s['child_pid']
assert launch['command'] == s['command']
assert service['CPUQuotaPerSecUSec'] == launch['service']['CPUQuotaPerSecUSec']
assert hashlib.sha256((base / 'run.py').read_bytes()).hexdigest() == launch['supervisor_sha256']
(stage / 'supervisor-exit.txt').write_text(props)
assert s['command'] == ['python3', 'scripts/check-tier1-race.py', '--root', str(stage), '--seeds', '1000', '--shards', '8'] + (['--no-race'] if mode == 'normal' else [])
assert subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=checkout, text=True).strip() == s['source']
assert not subprocess.check_output(['git', 'status', '--porcelain'], cwd=checkout)
before, after = [json.loads((stage / n).read_text()) for n in ('source-before.json', 'source-after.json')]
assert before == after and before['revision'] == s['source']
names = subprocess.check_output(['git', 'ls-tree', '-r', '--name-only', s['source']], cwd=checkout, text=True).splitlines()
required = [n for n in names if n.endswith(('.go', '.py', '.yml')) or n in ('go.mod', 'go.sum') or n.startswith('sim/testdata/')]
assert set(required) == set(before['files'])
stream = io.BytesIO(subprocess.check_output(['git', 'cat-file', '--batch'], cwd=checkout,
    input=''.join(s['source']+':'+n+'\n' for n in required).encode()))
for n in required:
    header = stream.readline().split()
    assert header[1] == b'blob'
    data = stream.read(int(header[2]))
    assert stream.read(1) == b'\n'
    assert hashlib.sha256(data).hexdigest() == before['files'][n] == hashlib.sha256((checkout / n).read_bytes()).hexdigest()
binary = stage / 'sim.test'
receipt = json.loads((stage / 'binary.json').read_text())
assert hashlib.sha256(binary.read_bytes()).hexdigest() == receipt['binary_sha256']
assert ('-race=true' in subprocess.check_output(['go', 'version', '-m', str(binary)], text=True)) == (mode == 'race')
assert receipt['environment'] == dict(GOMEMLIMIT='512MiB', GOMAXPROCS='2', SIM_SEEDS='1000', SIM_COVERAGE_SUMMARY='1', SIM_PROGRESS='1')
listing = subprocess.check_output([str(binary), '-test.list=^Test'], cwd=checkout,
                                 env=dict(os.environ, SIM_COVERAGE_SUMMARY='0'), text=True)
compiled = '\n'.join(l for l in listing.splitlines() if l.startswith('Test'))+'\n'
assert compiled == (stage / 'tier1-inventory.txt').read_text()
seeded = subprocess.check_output(['go', 'run', '-p=1', './scripts/tier1-seed-inventory'], cwd=checkout, text=True)
assert seeded == (stage / 'tier1-seeded-inventory.txt').read_text() and len(seeded.splitlines()) == 158
traces = [n for n in names if n.startswith('sim/testdata/regressions/') and n.endswith('.json')]
assert len(traces) == 853 and '\n'.join(traces)+'\n' == (stage / 'tier1-regression-inventory.txt').read_text()
sys.path.insert(0, str(checkout / 'scripts'))
from tier1_partitions import partition_tests, selector
groups = partition_tests(compiled.splitlines(), seeded.splitlines(), 8)
manifest = json.loads((stage / 'parts.json').read_text())
assert manifest == [dict(inventory=str(stage / f'part-{i:02d}' / 'inventory.txt'), events=str(stage / f'part-{i:02d}' / 'events.jsonl')) for i in range(8)]
commands = json.loads((stage / 'command-results.json').read_text())
assert len(commands) == 11 and all(c['exit_code'] == 0 for c in commands)
assert commands[0]['command'] == ['go', 'test', '-p=1'] + (['-race'] if mode == 'race' else []) + ['-c', '-o', str(binary), './sim']
assert commands[1]['command'] == ['go', 'run', '-p=1', './scripts/tier1-seed-inventory']
for i, group in enumerate(groups):
    part = stage / f'part-{i:02d}'
    assert (part / 'inventory.txt').read_text() == '\n'.join(group)+'\n'
    assert commands[i+2]['working_directory'] == str(checkout / 'sim')
    assert commands[i+2]['command'] == ['/usr/bin/time', '-o', str(part / 'time.txt'), '-f', 'elapsed=%e user=%U system=%S',
        'go', 'tool', 'test2json', '-t', '-p', 'js-wf/sim', str(binary), '-test.v=test2json', '-test.run='+selector(group), '-test.count=1', '-test.timeout=300m']
checker = ['python3', 'scripts/check-tier1-suite.py', '--parts', str(stage / 'parts.json'), '--inventory', str(stage / 'tier1-inventory.txt'),
           '--regressions', str(stage / 'tier1-regression-inventory.txt'), '--source', str(stage / 'tier1-source.txt'),
           '--seeded-inventory', str(stage / 'tier1-seeded-inventory.txt'), '--seeds', '1000', '--output']
assert commands[-1]['command'] == checker+[str(stage / 'tier1-result.json')]
assert commands[-1]['working_directory'] == str(checkout)
subprocess.check_call(checker+[str(stage / 'independent-review-result.json')], cwd=checkout)
report = json.loads((stage / 'tier1-result.json').read_text())
assert report == json.loads((stage / 'independent-review-result.json').read_text())
assert report['source'] == s['source'] and report['package_processes'] == 8 and report['disjoint_compiled_inventory_union']
assert report['pinned_regressions_pass'] == 853 and report['per_workload_seed_proof']['workloads'] == 158 and report['per_workload_seed_proof']['completed_bodies'] == 158000
result = dict(accepted=True, mode=mode, source=s['source'], git_verified_inputs=len(required), binary_sha256=receipt['binary_sha256'],
              actual_supervisor_exit=0, invocation=s['invocation'], service_cpu_quota=service['CPUQuotaPerSecUSec'], actual_package_processes=8, top_level_pass=report['top_level_pass'], saved_traces=853,
              seed_families=158, completed_seed_bodies=158000,
              scope='Complete latest158 '+mode+'1000 only, sequential disjoint processes/same binary and GOMAXPROCS2/GOMEMLIMIT512MiB, launch-bound service CPU quota, per-process300m watchdog. Other mode, extended, original native fault/scale/soak/retention/import/admission/rollout gates remain separate.')
(base / (mode+'-review.json')).write_text(json.dumps(result, indent=2)+'\n')
print(json.dumps(result, indent=2))
