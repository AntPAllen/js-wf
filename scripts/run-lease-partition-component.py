#!/usr/bin/env python3
"""Run a bare R3 lease KV partition diagnostic on fresh stores."""
import argparse
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import shutil
import subprocess
import time

import fixture_archive
from matrix_process_observer import observe_servers

REPO = Path(__file__).resolve().parents[1]


def module(name, filename):
    spec = importlib.util.spec_from_file_location(name, REPO/'scripts'/filename)
    result = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(result)
    return result


def prepare_candidate(root, revision, shared, save, env, variant='strict'):
    """Build one explicitly bound overlay on a fresh unchanged upstream copy."""
    source = Path(subprocess.check_output(['go', 'list', '-m', '-f', '{{.Dir}}',
                                         'github.com/nats-io/nats-server/v2'], cwd=REPO, text=True).strip())
    copied = root/'candidate-nats-source'
    shutil.copytree(source, copied)
    source_before = fixture_archive.inventory(copied)
    save('candidate-nats-source-before.json', source_before)
    patcher = REPO/'scripts/raft-obsolete-catchup-candidate.py'
    assert patcher.read_bytes() == subprocess.check_output(
        ['git', 'cat-file', 'blob', revision+':scripts/raft-obsolete-catchup-candidate.py'], cwd=REPO)
    shutil.copyfile(patcher, root/'candidate-patcher.py')
    candidate = root/'candidate-raft.go'
    subprocess.run(['python3', str(patcher), '--original', str(copied/'server/raft.go'),
                    '--output', str(candidate), '--variant', variant], cwd=REPO, check=True)
    if variant == 'contiguous':
        canonical = Path('docs/scale/lease-partition-component-2026-10-06/raft-contiguous-controls')
        records = {}
        for name in ['contiguous-raft.go.txt', 'independent-review.json',
                     'archive-verification.json', 'fixture-inventory.json', 's3-readback.json']:
            data = subprocess.check_output(['git', 'cat-file', 'blob', revision+':'+str(canonical/name)], cwd=REPO)
            assert data == (REPO/canonical/name).read_bytes()
            records[name] = hashlib.sha256(data).hexdigest()
            (root/('contiguous-parent-'+name)).write_bytes(data)
        assert candidate.read_bytes() == (REPO/canonical/'contiguous-raft.go.txt').read_bytes()
        metadata = json.loads((REPO/canonical/'archive-verification.json').read_text())
        receipt = json.loads((REPO/canonical/'s3-readback.json').read_text())
        assert receipt['archive']['full_readback'] == {
            'bytes': metadata['archive_bytes'], 'sha256': metadata['archive_sha256']}
        review = json.loads((REPO/canonical/'independent-review.json').read_text())
        assert review['direct_cases'] == 12 and review['unchanged_upstream_controls'] == 11
        assert review['results']['contiguous-regression']['exit_code'] == 0
        assert review['results']['contiguous-controls']['exit_code'] == 0
        save('contiguous-parent-reference.json', {
            'canonical': str(canonical), 'records': records,
            'source': review['source'], 'archive_url': receipt['archive']['url'],
            'archive_sha256': metadata['archive_sha256'],
            'candidate_raft_sha256': shared.sha(candidate)})
    mod = root/'candidate.mod'
    mod.write_text((REPO/'go.mod').read_text()+'\nreplace github.com/nats-io/nats-server/v2 => '+str(copied)+'\n')
    shutil.copyfile(REPO/'go.sum', root/'candidate.sum')
    save('candidate-overlay.json', {'Replace': {str(copied/'server/raft.go'): str(candidate)}})
    flags = ['-mod=readonly', '-modfile='+str(mod), '-overlay='+str(root/'candidate-overlay.json')]
    command = ['go', 'list', *flags, '-deps', '-f',
               '{{.Dir}}|{{join .GoFiles " "}}|{{join .CgoFiles " "}}',
               'github.com/nats-io/nats-server/v2']
    deps = subprocess.check_output(command, cwd=REPO, env=env, text=True)
    (root/'candidate-dependencies.txt').write_text(deps)
    external = {}
    for line in deps.splitlines():
        directory, *groups = line.split('|')
        for name in ' '.join(groups).split():
            path = (Path(directory)/name).resolve()
            if path == copied/'server/raft.go':
                path = candidate
            target = root/'candidate-selected-dependencies'/str(path).lstrip('/')
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(path, target)
            external[str(path)] = {'sha256': shared.sha(path), 'captured': str(target.relative_to(root))}
    save('candidate-dependencies-before.json', external)
    inputs = {'mod_sha256': shared.sha(mod), 'sum_sha256': shared.sha(root/'candidate.sum')}
    save('candidate-module-inputs.json', inputs)
    binary = root/'candidate-server'
    build = ['go', 'build', *flags, '-p=1', '-buildvcs=true', '-o', str(binary),
             'github.com/nats-io/nats-server/v2']
    with (root/'candidate-build.log').open('w') as output:
        subprocess.run(build, cwd=REPO, env=env, check=True, stdout=output, stderr=subprocess.STDOUT)
    save('candidate-build.json', {'source': revision, 'list': command, 'build': build,
         'executable_sha256': shared.sha(binary), 'patcher_sha256': shared.sha(patcher),
         'original_raft_sha256': shared.sha(copied/'server/raft.go'),
         'candidate_raft_sha256': shared.sha(candidate), 'guard_variant': variant,
         'build_info': subprocess.check_output(['go', 'version', '-m', str(binary)], text=True)})
    return binary, copied, source_before, external, inputs


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, required=True)
    parser.add_argument('--key-profile',choices=['fixed','fresh'],default='fixed')
    parser.add_argument('--expiry-profile',choices=['production','disabled'],default='production')
    parser.add_argument('--raft-debug',action='store_true')
    parser.add_argument('--marker-profile',choices=['production','disabled-markers'],default='production')
    parser.add_argument('--server-profile', choices=['upstream', 'obsolete-catchup-candidate',
                                                   'obsolete-catchup-contiguous-candidate'], default='upstream')
    args = parser.parse_args()
    root = args.root.absolute()
    assert not root.exists() and not root.is_relative_to(REPO)
    shared = module('shared', 'run-domain-runtime-controls.py')
    revision = subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=REPO, text=True).strip()
    assert revision == subprocess.check_output(['git','ls-remote','origin','refs/heads/main'],cwd=REPO,text=True).split()[0]
    before = shared.source_inventory(revision)
    env = dict(os.environ,GOMAXPROCS='2',GOMEMLIMIT='2GiB',GOWORK='off',GOFLAGS='')
    root.mkdir()
    save = lambda name, value: (root/name).write_text(json.dumps(value,indent=2)+'\n')
    save('source-before.json',before)
    for name in before['files']:
        target = root/'selected-source'/name
        target.parent.mkdir(parents=True,exist_ok=True)
        shutil.copyfile(REPO/name,target)
    variant = 'contiguous' if args.server_profile == 'obsolete-catchup-contiguous-candidate' else 'strict'
    candidate = prepare_candidate(root, revision, shared, save, env, variant) if args.server_profile != 'upstream' else None
    template = REPO/'scripts/lease-partition-component.go.txt'
    assert template.read_bytes()==subprocess.check_output(['git','cat-file','blob',revision+':scripts/lease-partition-component.go.txt'],cwd=REPO)
    helper = root/'helper.go';shutil.copyfile(template,helper)
    deps=subprocess.check_output(['go','list','-deps','-f','{{.Dir}}|{{join .GoFiles " "}}|{{join .CgoFiles " "}}',str(helper)],cwd=REPO,text=True)
    (root/'dependencies.txt').write_text(deps)
    external={}
    for line in deps.splitlines():
        directory,*groups=line.split('|')
        for name in ' '.join(groups).split():
            path=(Path(directory)/name).resolve()
            digest=shared.sha(path)
            if path==helper:
                assert digest==shared.sha(template)
            elif path.is_relative_to(REPO):
                assert before['files'][str(path.relative_to(REPO))]==digest
            else:
                target=root/'selected-external-source'/str(path).lstrip('/')
                target.parent.mkdir(parents=True,exist_ok=True);shutil.copyfile(path,target)
                external[str(path)]=dict(sha256=digest,captured=str(target.relative_to(root)))
    save('external-source-before.json',external)
    binary = root/'diagnostic'
    build = ['go','build','-p=1','-buildvcs=true','-o',str(binary),str(helper)]
    with (root/'build.log').open('w') as output:
        subprocess.run(build,cwd=REPO,env=env,stdout=output,stderr=subprocess.STDOUT,check=True)
    command = [str(binary),str(root),args.key_profile,args.expiry_profile,'debug' if args.raft_debug else 'normal',args.marker_profile]
    if candidate is not None:
        command.append(str(candidate[0]))
    save('commands.json',dict(build=build,run=command,source=revision,helper_sha256=shared.sha(helper),server_profile=args.server_profile))
    records=[];seen=set()
    with (root/'native.log').open('w') as output:
        child=subprocess.Popen(command,cwd=REPO,env=env,stdout=output,stderr=subprocess.STDOUT)
        proc=Path('/proc',str(child.pid))
        save('actual-helper.json',dict(pid=child.pid,stat=(proc/'stat').read_text(),argv=[os.fsdecode(a) for a in (proc/'cmdline').read_bytes().split(b'\0') if a],exe_sha256=shared.sha(proc/'exe'),build_info=subprocess.check_output(['go','version','-m',str(binary)],text=True)))
        while child.poll() is None:
            observe_servers(child.pid,root,seen,records)
            save('observed-servers.json',records)
            try: child.wait(timeout=.25)
            except subprocess.TimeoutExpired: pass
        code=child.wait()
    assert len(records)==3 and len({r['actual_executable_sha256'] for r in records})==1
    assert all('v2.15.0' in r['build_info'] for r in records)
    if candidate is not None:
        candidate_binary, copied, source_before, dependencies, inputs = candidate
        assert all(r['actual_executable_sha256'] == shared.sha(candidate_binary) for r in records)
        assert fixture_archive.inventory(copied) == source_before
        assert inputs == {'mod_sha256': shared.sha(root/'candidate.mod'), 'sum_sha256': shared.sha(root/'candidate.sum')}
        assert all(shared.sha(path) == row['sha256'] == shared.sha(root/row['captured']) for path, row in dependencies.items())
        save('candidate-nats-source-after.json', source_before)
        save('candidate-dependencies-after.json', dependencies)
    assert all(shared.sha(path)==record['sha256']==shared.sha(root/record['captured']) for path,record in external.items())
    save('external-source-after.json',external)
    after=shared.source_inventory(revision);assert after==before
    save('source-after.json',after)
    save('closure.json',shared.closure(root))
    save('execution.json',dict(source=revision,exit_code=code,server_profile=args.server_profile,scope='Bare R3 lease KV component diagnostic on fresh stores. No SDK workflows, native matrix or causal Tier1 reproduction qualification.'))
    shutil.copyfile(__file__,root/'executed-producer.py')
    proof=fixture_archive.capture(root,root.with_suffix('.tar.gz'),root.with_name(root.name+'-proof'),compresslevel=1)
    print(json.dumps(dict(exit_code=code,proof=proof)),flush=True)
    raise SystemExit(code)


if __name__=='__main__':
    main()
