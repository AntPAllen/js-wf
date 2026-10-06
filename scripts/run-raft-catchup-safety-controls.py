#!/usr/bin/env python3
"""Run every pinned TestNRG case using the already source-bound race binaries."""
import argparse
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import re
import shutil
import subprocess

import fixture_archive

REPO = Path(__file__).resolve().parents[1]
PARENT = Path('docs/scale/lease-partition-component-2026-10-06/raft-callback-regression')
COMPONENT = Path('docs/scale/lease-partition-component-2026-10-06/candidate-component')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, required=True)
    parser.add_argument('--parent-root', type=Path, required=True)
    args = parser.parse_args()
    root, parent = args.root.absolute(), args.parent_root.absolute()
    if root.exists() or root.is_relative_to(REPO) or parent.is_symlink() or not parent.is_dir():
        parser.error('require fresh output outside checkout and a regular preserved parent directory')
    revision = subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=REPO, text=True).strip()
    assert revision == subprocess.check_output(['git', 'ls-remote', 'origin', 'refs/heads/main'], cwd=REPO, text=True).split()[0]
    spec = importlib.util.spec_from_file_location('shared', REPO/'scripts/run-domain-runtime-controls.py')
    shared = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(shared)
    before = shared.source_inventory(revision)
    root.mkdir()
    save = lambda name, data: (root/name).write_text(json.dumps(data, indent=2)+'\n')
    save('source-before.json', before)
    for name in before['files']:
        target = root/'selected-source'/name
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(REPO/name, target)
    records = {}
    retained = root/'parent-proof'; retained.mkdir()
    for name in ['archive-verification.json', 'fixture-inventory.json', 's3-readback.json', 'independent-review.json',
                 'upstream-binary.json', 'candidate-binary.json', 'dependencies-before.json', 'dependencies-after.json',
                 'nats-source-before.json', 'nats-source-after.json', 'module-inputs.json', 'source-before.json', 'source-after.json',
                 'candidate-overlay.json', 'upstream-overlay.json', 'generated-testmain-limits.json']:
        data = subprocess.check_output(['git', 'cat-file', 'blob', revision+':'+str(PARENT/name)], cwd=REPO)
        assert data == (REPO/PARENT/name).read_bytes()
        (retained/name).write_bytes(data); records[name] = hashlib.sha256(data).hexdigest()
    read = lambda name: json.loads((retained/name).read_text())
    metadata, manifest, receipt = (read(n) for n in ['archive-verification.json', 'fixture-inventory.json', 's3-readback.json'])
    assert metadata['schema'] == 'js-wf-full-fixture-archive-v1'
    assert metadata['inventory_sha256'] == records['fixture-inventory.json']
    assert receipt['archive']['full_readback'] == {'bytes': metadata['archive_bytes'], 'sha256': metadata['archive_sha256']}
    assert fixture_archive.inventory(parent) == manifest['files']
    dependencies = read('dependencies-before.json'); assert dependencies == read('dependencies-after.json')
    for path, row in dependencies.items():
        assert shared.sha(path) == row['sha256'] == shared.sha(parent/row['captured'])
        target = root/'selected-parent-dependencies'/str(path).lstrip('/')
        target.parent.mkdir(parents=True, exist_ok=True); shutil.copyfile(parent/row['captured'], target)
    copied = root/'fixture-source'; shutil.copytree(parent/'nats-source', copied)
    module_before = fixture_archive.inventory(copied)
    assert module_before == read('nats-source-before.json') == read('nats-source-after.json')
    save('fixture-source-before.json', module_before)
    component_raft = REPO/COMPONENT/'candidate-raft.go.txt'
    component_data = subprocess.check_output(['git', 'cat-file', 'blob', revision+':'+str(COMPONENT/'candidate-raft.go.txt')], cwd=REPO)
    assert component_data == component_raft.read_bytes() == (parent/'candidate-raft.go').read_bytes()
    save('parent-reference.json', {'canonical': str(PARENT), 'records': records, 'source': read('independent-review.json')['source'],
         'archive_url': receipt['archive']['url'], 'archive_sha256': metadata['archive_sha256'],
         'component_source': json.loads((REPO/COMPONENT/'candidate-build.json').read_text())['source'],
         'component_candidate_raft_sha256': shared.sha(component_raft), 'same_candidate_guard_source_as_real_component': True})
    binaries = {}
    for profile in ['upstream', 'candidate']:
        binary_info = read(profile+'-binary.json')
        assert '-race' in binary_info['build'] and binary_info['executable_sha256'] == shared.sha(parent/(profile+'.test'))
        target = root/(profile+'.test'); shutil.copy2(parent/(profile+'.test'), target); target.chmod(0o700)
        assert shared.sha(target) == binary_info['executable_sha256']
        binaries[profile] = target
    expected = None
    listings = {}
    for profile, binary in binaries.items():
        command = [str(binary), '-test.list=^TestNRG']
        listing = subprocess.check_output(command, cwd=copied/'server', text=True).splitlines()
        assert len(listing) == len(set(listing)) == 170 and all(re.fullmatch(r'TestNRG\w+', n) for n in listing)
        if expected is None: expected = listing
        else: assert listing == expected
        listings[profile] = {'command': command, 'tests': listing, 'executable_sha256': shared.sha(binary)}
    save('selected-tests.json', listings)
    source_names = re.findall(r'^func (TestNRG\w+)\(', (copied/'server/raft_test.go').read_text(), re.M)
    assert set(source_names) == set(expected) and len(source_names) == len(expected)
    env = {k:v for k,v in os.environ.items() if not k.startswith(('WF_', 'MATRIX_', 'TIER3_MATRIX_'))}
    temporary = root/'test-tmp'; temporary.mkdir()
    env.update(GOMAXPROCS='1', GOMEMLIMIT='2GiB', TMPDIR=str(temporary), GOWORK='off', GOFLAGS='')
    results = {}
    for profile, binary in binaries.items():
        command = [str(binary), '-test.run=^TestNRG', '-test.v', '-test.count=1', '-test.timeout=20m']
        with (root/(profile+'.log')).open('w') as output:
            child = subprocess.Popen(command, cwd=copied/'server', env=env, stdout=output, stderr=subprocess.STDOUT)
            proc = Path('/proc', str(child.pid))
            actual = {'pid':child.pid, 'stat':(proc/'stat').read_text(), 'argv':[os.fsdecode(v) for v in (proc/'cmdline').read_bytes().split(b'\0') if v],
                      'actual_executable_sha256':shared.sha(proc/'exe'), 'cwd':os.readlink(proc/'cwd')}
            save(profile+'-live-execution.json', {'actual':actual, 'command':command, 'environment':{k:env[k] for k in ['GOMAXPROCS','GOMEMLIMIT','TMPDIR','GOWORK','GOFLAGS']}})
            code = child.wait()
        assert shared.sha(binary) == actual['actual_executable_sha256'] == read(profile+'-binary.json')['executable_sha256']
        log = (root/(profile+'.log')).read_text()
        started = re.findall(r'^=== RUN   (TestNRG\w+)$', log, re.M)
        passed = re.findall(r'^--- PASS: (TestNRG\w+) \(', log, re.M)
        failed = re.findall(r'^--- FAIL: (TestNRG\w+) \(', log, re.M)
        skipped = re.findall(r'^--- SKIP: (TestNRG\w+) \(', log, re.M)
        qualified = code == 0 and started == expected and len(passed) == len(expected) and set(passed) == set(expected) and not failed and not skipped and 'WARNING: DATA RACE' not in log
        results[profile] = {'exit_code':code, 'expected':len(expected), 'started':started, 'passed':passed, 'failed':failed, 'skipped':skipped,
                            'race_report':'WARNING: DATA RACE' in log, 'qualified':qualified}
        save(profile+'-execution.json', {'actual':actual, 'command':command, 'exit_code':code, 'result':results[profile]})
        print('FULL_NRG_FINISHED', profile, code, len(passed), 'qualified='+str(qualified), flush=True)
    assert fixture_archive.inventory(copied) == module_before
    save('fixture-source-after.json', module_before)
    for path,row in dependencies.items():
        assert shared.sha(path) == row['sha256'] == shared.sha(parent/row['captured']) == shared.sha(root/'selected-parent-dependencies'/str(path).lstrip('/'))
    assert fixture_archive.inventory(parent) == manifest['files']
    after = shared.source_inventory(revision); assert after == before; save('source-after.json', after)
    save('closure.json', shared.closure(root))
    save('result.json', {'orchestrator_source':revision, 'compiled_server_source':read('independent-review.json')['source'], 'results':results,
         'scope':'Every170 pinned TestNRG top-level case, original source-bound race binaries, count1/20m each, fresh fixture-source and temp dirs, 1CPU/2GiB. Broader Raft controls only; no full NATS test suite, production adoption, workflow matrix or Tier1 qualification.'})
    shutil.copyfile(__file__, root/'executed-producer.py')
    proof = fixture_archive.capture(root, root.with_suffix('.tar.gz'), root.with_name(root.name+'-proof'), compresslevel=1)
    print(json.dumps({'proof':proof,'all_pass':all(r['qualified'] for r in results.values())}), flush=True)
    raise SystemExit(0 if all(r['qualified'] for r in results.values()) else 1)


if __name__ == '__main__':
    main()
