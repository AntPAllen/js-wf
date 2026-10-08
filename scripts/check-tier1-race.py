#!/usr/bin/env python3
"""Qualify the complete Tier1 suite from one retained binary (race by default)."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess

REPO = Path(__file__).resolve().parents[1]
p = argparse.ArgumentParser(description=__doc__)
p.add_argument('--root', type=Path, required=True)
p.add_argument('--seeds', type=int, choices=(1000,10000,100000), default=1000)
p.add_argument('--no-race', action='store_true', help='retain a normal binary for the extended seed suite')
a = p.parse_args()
root = a.root.resolve()
assert not root.exists() and not root.is_relative_to(REPO)
assert not subprocess.check_output(['git', 'status', '--porcelain'], cwd=REPO)
revision = subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=REPO, text=True).strip()
names = subprocess.check_output(['git', 'ls-files'], cwd=REPO, text=True).splitlines()
def inventory():
    return {n: hashlib.sha256((REPO/n).read_bytes()).hexdigest() for n in names
            if n.endswith(('.go', '.py', '.yml')) or n in ('go.mod', 'go.sum') or n.startswith('sim/testdata/')}
root.mkdir(parents=True)
before = inventory()
(root/'source-before.json').write_text(json.dumps(dict(revision=revision, files=before), indent=2)+'\n')
(root/'tier1-source.txt').write_text(revision+'\n')
(root/'tier1-regression-inventory.txt').write_text('\n'.join(n for n in names if n.startswith('sim/testdata/regressions/') and n.endswith('.json'))+'\n')
env = dict(os.environ, GOMEMLIMIT='512MiB', GOMAXPROCS='2', SIM_SEEDS=str(a.seeds), SIM_COVERAGE_SUMMARY='1', SIM_PROGRESS='1', FAULT_TRACE_OUT=str(root/'failure-trace.json'))
commands = []
execution_contexts = []
def call(command, **kwargs):
    cwd = kwargs.pop('cwd', REPO)
    commands.append(command)
    execution_contexts.append(dict(command=command, working_directory=str(cwd)))
    (root/'commands.json').write_text(json.dumps(commands, indent=2)+'\n')
    (root/'execution-contexts.json').write_text(json.dumps(execution_contexts, indent=2)+'\n')
    return subprocess.run(command, cwd=cwd, env=env, check=True, **kwargs)
try:
    binary = root/'sim.test'
    call(['go', 'test', '-p=1', *([] if a.no_race else ['-race']), '-c', '-o', str(binary), './sim'])
    listing = subprocess.check_output([str(binary), '-test.list=^Test'], cwd=REPO, env=dict(env, SIM_COVERAGE_SUMMARY='0'), text=True)
    (root/'tier1-list.log').write_text(listing)
    (root/'tier1-inventory.txt').write_text('\n'.join(l for l in listing.splitlines() if l.startswith('Test'))+'\n')
    with (root/'tier1-seeded-inventory.txt').open('w') as out:
        call(['go', 'run', '-p=1', './scripts/tier1-seed-inventory'], stdout=out)
    provenance = dict(binary_sha256=hashlib.sha256(binary.read_bytes()).hexdigest(), race_instrumented=not a.no_race,
                     go_version=subprocess.check_output(['go', 'version'], text=True).strip(),
                     build_info=subprocess.check_output(['go', 'version', '-m', str(binary)], text=True),
                     environment={k: env[k] for k in ('GOMEMLIMIT', 'GOMAXPROCS', 'SIM_SEEDS', 'SIM_COVERAGE_SUMMARY', 'SIM_PROGRESS')})
    assert ('-race=true' in provenance['build_info']) == provenance['race_instrumented'], 'binary race provenance mismatch'
    (root/'binary.json').write_text(json.dumps(provenance, indent=2)+'\n')
    with (root/'tier1-events.jsonl').open('w') as out, (root/'stderr.log').open('w') as err:
        call(['/usr/bin/time', '-o', str(root/'tier1-time.txt'), '-f', 'elapsed=%e user=%U system=%S',
              'go', 'tool', 'test2json', '-t', '-p', 'js-wf/sim', str(binary),
              '-test.v=test2json', '-test.count=1', '-test.timeout='+('300m' if a.no_race else '60m')], cwd=REPO/'sim', stdout=out, stderr=err)
    call(['python3', 'scripts/check-tier1-suite.py', '--events', str(root/'tier1-events.jsonl'),
          '--inventory', str(root/'tier1-inventory.txt'), '--regressions', str(root/'tier1-regression-inventory.txt'),
          '--source', str(root/'tier1-source.txt'), '--seeded-inventory', str(root/'tier1-seeded-inventory.txt'),
          '--seeds', str(a.seeds), '--output', str(root/'tier1-result.json')])
    assert hashlib.sha256(binary.read_bytes()).hexdigest()==provenance['binary_sha256']
finally:
    after=inventory()
    (root/'source-after.json').write_text(json.dumps(dict(revision=revision, files=after), indent=2)+'\n')
    assert after==before
