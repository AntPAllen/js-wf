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


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, required=True)
    parser.add_argument('--key-profile',choices=['fixed','fresh'],default='fixed')
    parser.add_argument('--expiry-profile',choices=['production','disabled'],default='production')
    parser.add_argument('--raft-debug',action='store_true')
    args = parser.parse_args()
    root = args.root.absolute()
    assert not root.exists() and not root.is_relative_to(REPO)
    shared = module('shared', 'run-domain-runtime-controls.py')
    revision = subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=REPO, text=True).strip()
    assert revision == subprocess.check_output(['git','ls-remote','origin','refs/heads/main'],cwd=REPO,text=True).split()[0]
    before = shared.source_inventory(revision)
    root.mkdir()
    save = lambda name, value: (root/name).write_text(json.dumps(value,indent=2)+'\n')
    save('source-before.json',before)
    for name in before['files']:
        target = root/'selected-source'/name
        target.parent.mkdir(parents=True,exist_ok=True)
        shutil.copyfile(REPO/name,target)
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
    env = dict(os.environ,GOMAXPROCS='2',GOMEMLIMIT='2GiB',GOWORK='off',GOFLAGS='')
    build = ['go','build','-p=1','-buildvcs=true','-o',str(binary),str(helper)]
    with (root/'build.log').open('w') as output:
        subprocess.run(build,cwd=REPO,env=env,stdout=output,stderr=subprocess.STDOUT,check=True)
    command = [str(binary),str(root),args.key_profile,args.expiry_profile,'debug' if args.raft_debug else 'normal']
    save('commands.json',dict(build=build,run=command,source=revision,helper_sha256=shared.sha(helper)))
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
    assert all(shared.sha(path)==record['sha256']==shared.sha(root/record['captured']) for path,record in external.items())
    save('external-source-after.json',external)
    after=shared.source_inventory(revision);assert after==before
    save('source-after.json',after)
    save('closure.json',shared.closure(root))
    save('execution.json',dict(source=revision,exit_code=code,scope='Bare R3 lease KV component diagnostic on fresh stores. No SDK workflows, native matrix or causal Tier1 reproduction qualification.'))
    shutil.copyfile(__file__,root/'executed-producer.py')
    proof=fixture_archive.capture(root,root.with_suffix('.tar.gz'),root.with_name(root.name+'-proof'),compresslevel=1)
    print(json.dumps(dict(exit_code=code,proof=proof)),flush=True)
    raise SystemExit(code)


if __name__=='__main__':
    main()
