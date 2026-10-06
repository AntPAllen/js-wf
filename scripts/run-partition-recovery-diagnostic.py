#!/usr/bin/env python3
"""Restart freshly verified copies of a failed partition seed, without writers."""
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


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, required=True)
    parser.add_argument('--failure-proof', type=Path, required=True)
    args = parser.parse_args()
    root = args.root.absolute()
    assert not root.exists() and not root.is_relative_to(REPO)
    assert not args.failure_proof.is_absolute() and '..' not in args.failure_proof.parts
    shared = module('shared', 'run-domain-runtime-controls.py')
    s3 = module('s3', 'offload-proof-to-s3.py')
    revision = subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=REPO, text=True).strip()
    assert revision == subprocess.check_output(['git','ls-remote','origin','refs/heads/main'],cwd=REPO,text=True).split()[0]
    before = shared.source_inventory(revision)
    blob = lambda name: subprocess.check_output(['git','cat-file','blob',revision+':'+str(args.failure_proof/name)],cwd=REPO)
    metadata = json.loads(blob('archive-verification.json'))
    inventory_bytes = blob('fixture-inventory.json')
    assert hashlib.sha256(inventory_bytes).hexdigest() == metadata['inventory_sha256']
    inventory = json.loads(inventory_bytes)
    receipt = json.loads(blob('s3-readback.json'))
    execution = json.loads(blob('execution.json'))
    assert execution['status']=='failed' and execution['exit_code']==1 and execution['row']=='partition'
    assert execution['test']=='TestMixedMatrixServerPartitionEveryThirtySeconds'
    root.mkdir()
    save = lambda name, value: (root/name).write_text(json.dumps(value,indent=2)+'\n')
    save('source-before.json',before)
    for name in before['files']:
        target = root/'selected-source'/name
        target.parent.mkdir(parents=True,exist_ok=True)
        shutil.copyfile(REPO/name,target)
    archive = root/'downloaded.tar.gz'
    with archive.open('xb') as output:
        subprocess.run(['curl','--config','-','--aws-sigv4','aws:amz:us-east-1:s3','--silent','--show-error','--fail','--connect-timeout','30','--max-time','1800',receipt['archive']['url']],input=s3.credentials(),stdout=output,check=True)
    expected = dict(bytes=metadata['archive_bytes'],sha256=metadata['archive_sha256'])
    restore = fixture_archive.restore(archive,expected,inventory,root/'restored')
    save('restore.json',dict(report=restore,canonical_proof=str(args.failure_proof),remote_url=receipt['archive']['url'],expected_archive=expected))
    # Newly downloaded staging is a verified duplicate of the canonical S3 object.
    archive.unlink()
    original = root/'restored'/'originals'/execution['test']
    copied = root/'copied-cluster'
    initial = fixture_archive.inventory(original)
    shutil.copytree(original,copied)
    assert fixture_archive.inventory(copied)==initial
    save('copy-before.json',dict(files=initial,all_copy_bytes_modes_mtimes_equal=True))
    captured = list((root/'restored'/'server-executables').glob('*.bin'))
    assert len(captured)==1 and shared.sha(captured[0])==captured[0].stem
    server = root/'exact-nats-server'
    shutil.copyfile(captured[0],server);server.chmod(0o755)
    server_sha = shared.sha(server)
    observed_original = json.loads((root/'restored'/'observed-servers.json').read_text())
    assert len(observed_original)==3 and all(r['actual_executable_sha256']==server_sha for r in observed_original)
    template = REPO/'scripts/partition-recovery-diagnostic.go.txt'
    assert template.read_bytes()==subprocess.check_output(['git','cat-file','blob',revision+':scripts/partition-recovery-diagnostic.go.txt'],cwd=REPO)
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
    env = dict(os.environ,GOMAXPROCS='2',GOMEMLIMIT='2GiB',GOWORK='off',GOFLAGS='')
    build = ['go','build','-p=1','-buildvcs=true','-o',str(binary),str(helper)]
    with (root/'build.log').open('w') as output:
        subprocess.run(build,cwd=REPO,env=env,stdout=output,stderr=subprocess.STDOUT,check=True)
    command = [str(binary),str(copied),str(server),str(root/'diagnostics')]
    save('commands.json',dict(build=build,run=command,source=revision,helper_sha256=shared.sha(helper),exact_nats_sha256=server_sha))
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
    assert fixture_archive.inventory(original)==initial
    assert fixture_archive.inventory(root/'restored')==inventory['files']
    assert all(r['actual_executable_sha256']==server_sha for r in records) and len(records)==3
    assert all(shared.sha(path)==record['sha256']==shared.sha(root/record['captured']) for path,record in external.items())
    save('external-source-after.json',external)
    after=shared.source_inventory(revision);assert after==before
    save('source-after.json',after)
    save('closure.json',shared.closure(root))
    save('execution.json',dict(source=revision,exit_code=code,original_source=execution['source'],original_native_failure_unchanged=True,restored_baseline_unchanged=True,scope='Quiescent fresh-copy restart diagnostic only. Original partition200 remains failed; no workflow writers, fault replay, recovery-gate relaxation or full-row acceptance.'))
    shutil.copyfile(__file__,root/'executed-producer.py')
    proof=fixture_archive.capture(root,root.with_suffix('.tar.gz'),root.with_name(root.name+'-proof'),compresslevel=1)
    print(json.dumps(dict(exit_code=code,proof=proof)),flush=True)
    raise SystemExit(code)


if __name__=='__main__':
    main()
