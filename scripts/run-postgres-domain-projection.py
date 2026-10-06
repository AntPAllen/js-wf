#!/usr/bin/env python3
"""Run the original full PostgreSQL projection fault/rebuild case in a real domain."""
import argparse
from datetime import datetime, timezone
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import shutil
import subprocess
import time

import fixture_archive

REPO = Path(__file__).resolve().parents[1]
TEST = 'TestPostgresProjectionCrashAndSessionLossFiftyThousandInvocationsInJetStreamDomain'


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, required=True)
    args = parser.parse_args()
    root = args.root.absolute()
    assert not root.exists() and not root.is_relative_to(REPO)
    assert shutil.disk_usage(root.parent).free >= 5*(1 << 30), '5GiB disk admission required'
    spec = importlib.util.spec_from_file_location('shared', REPO/'scripts/run-domain-runtime-controls.py')
    shared = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(shared)
    revision = subprocess.check_output(['git','rev-parse','HEAD'], cwd=REPO, text=True).strip()
    before = shared.source_inventory(revision)
    root.mkdir(); (root/'originals').mkdir()
    save = lambda name, value: (root/name).write_text(json.dumps(value, indent=2)+'\n')
    save('source-before.json', before)
    for name in before['files']:
        target = root/'selected-source'/name
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(REPO/name, target)
    # Preserve the selected actual external Go inputs, as in the original full case.
    fmt = '{{.Dir}}|{{join .GoFiles " "}}|{{join .CgoFiles " "}}|{{join .TestGoFiles " "}}|{{join .XTestGoFiles " "}}'
    deps = subprocess.check_output(['go','list','-deps','-test','-f',fmt,'./integration'], cwd=REPO, text=True)
    (root/'dependencies.txt').write_text(deps)
    goroot = Path(subprocess.check_output(['go','env','GOROOT'],text=True).strip())
    modules = Path(subprocess.check_output(['go','env','GOMODCACHE'],text=True).strip())
    inputs, captured = {}, {}
    for line in deps.splitlines():
        directory, *groups = line.split('|')
        for name in ' '.join(groups).split():
            path = (Path(directory)/name).resolve()
            if not path.is_file() or path.is_relative_to(REPO) or str(path) in inputs:
                continue
            inputs[str(path)] = shared.sha(path)
            relative = Path('modules')/path.relative_to(modules) if path.is_relative_to(modules) else Path('toolchain')/path.relative_to(goroot) if path.is_relative_to(goroot) else Path('other')/str(path).lstrip('/')
            target = root/'selected-external-source'/relative
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(path, target)
            assert shared.sha(target)==inputs[str(path)]
            captured[str(path)] = str(target.relative_to(root))
    save('external-source-before.json', inputs); save('external-captured-paths.json', captured)
    container = root.name+'-postgres'; volume = container+'-data'
    image = json.loads(subprocess.check_output(['docker','image','inspect','postgres:16-alpine']))[0]
    save('postgres-image.json', image)
    # A fresh owned volume is never shared with another proof or old historical media.
    assert volume not in subprocess.check_output(['docker','volume','ls','--format','{{.Name}}'],text=True).splitlines()
    subprocess.run(['docker','volume','create',volume],check=True,stdout=subprocess.DEVNULL)
    launch = ['docker','run','-d','--name',container,'--memory=1g','-p','127.0.0.1::5432',
              '-e','POSTGRES_USER=workflow','-e','POSTGRES_DB=workflow','-e','POSTGRES_HOST_AUTH_METHOD=trust',
              '-v',volume+':/var/lib/postgresql/data',image['Id']]
    save('postgres-command.json', launch)
    subprocess.run(launch,check=True,stdout=subprocess.DEVNULL)
    code = None
    try:
        ready = time.monotonic()+60
        while subprocess.run(['docker','exec',container,'pg_isready','-h','127.0.0.1','-U','workflow','-d','workflow'],
                             stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL).returncode:
            if time.monotonic()>=ready:
                raise RuntimeError('PostgreSQL admission failed')
            time.sleep(.25)
        observed = json.loads(subprocess.check_output(['docker','inspect',container]))[0]
        pid = observed['State']['Pid']; actual_exe = f'/proc/{pid}/exe'
        version = subprocess.check_output(['docker','exec',container,'postgres','--version'],text=True).strip()
        with (root/'actual-postgres').open('wb') as stream:
            subprocess.run(['sudo','-n','cat',actual_exe],stdout=stream,check=True)
        digest = subprocess.check_output(['sudo','-n','sha256sum',actual_exe],text=True).split()[0]
        assert digest==shared.sha(root/'actual-postgres')
        save('actual-postgres.json', dict(container=observed,actual_host_pid=pid,actual_executable_sha256=digest,version=version))
        port = observed['NetworkSettings']['Ports']['5432/tcp'][0]['HostPort']
        dsn = f'postgres://workflow@127.0.0.1:{port}/workflow?sslmode=disable'
        env = {k:v for k,v in os.environ.items() if not k.startswith('WF_')}
        env.update(GOMAXPROCS='2',GOMEMLIMIT='2GiB',GOWORK='off',GOFLAGS='',
                   WF_PROJECTION_POSTGRES_FAULT_ROOT=str(root/'originals'),WF_TEST_POSTGRES_DSN=dsn)
        binary = root/'integration.test'
        build = ['go','test','-p=1','-buildvcs=true','-c','-o',str(binary),'./integration']
        command = [str(binary),'-test.run=^'+TEST+'$','-test.count=1','-test.v','-test.timeout=22m']
        save('commands.json',dict(build=build,test=command,working_directory=str(REPO),
                                 environment={k:env[k] for k in ('GOMAXPROCS','GOMEMLIMIT','GOWORK','GOFLAGS','WF_PROJECTION_POSTGRES_FAULT_ROOT','WF_TEST_POSTGRES_DSN')}))
        with (root/'build.log').open('w') as log:
            subprocess.run(build,cwd=REPO,env=env,stdout=log,stderr=subprocess.STDOUT,check=True)
        info = subprocess.check_output(['go','version','-m',str(binary)],text=True)
        assert 'vcs.modified=false' in info and 'vcs.revision='+revision in info and '-race=true' not in info
        save('binary.json',dict(sha256=shared.sha(binary),build_info=info))
        started = time.monotonic()
        with (root/'native.log').open('w') as log:
            child = subprocess.Popen(command,cwd=REPO,env=env,stdout=log,stderr=subprocess.STDOUT)
            proc = Path('/proc',str(child.pid))
            actual_env = dict(v.split(b'=',1) for v in (proc/'environ').read_bytes().split(b'\0') if b'=' in v)
            actual = dict(pid=child.pid,stat=(proc/'stat').read_text(),exe=str((proc/'exe').resolve()),
                          exe_sha256=shared.sha(proc/'exe'),working_directory=str((proc/'cwd').resolve()),
                          args=[os.fsdecode(v) for v in (proc/'cmdline').read_bytes().split(b'\0') if v],
                          environment={k:os.fsdecode(actual_env[k.encode()]) for k in ('GOMAXPROCS','GOMEMLIMIT','GOWORK','GOFLAGS','WF_PROJECTION_POSTGRES_FAULT_ROOT','WF_TEST_POSTGRES_DSN')},
                          actual_WF_PROJECTION_COUNT_present=b'WF_PROJECTION_COUNT' in actual_env,
                          native_R3_servers_embedded_in_actual_sdk=True)
            assert actual['args']==command and actual['exe_sha256']==shared.sha(binary) and not actual['actual_WF_PROJECTION_COUNT_present']
            save('actual-sdk.json',actual)
            save('execution.json',dict(source=revision,status='running',started_utc=datetime.now(timezone.utc).isoformat()))
            print('ACTUAL_FULL_DOMAIN_PROJECTION_SDK',child.pid,revision,flush=True)
            code = child.wait()
        save('execution.json',dict(source=revision,status='passed' if code==0 else 'failed',exit_code=code,
                                   elapsed_seconds=time.monotonic()-started,finished_utc=datetime.now(timezone.utc).isoformat()))
    finally:
        with (root/'postgres-before-stop.log').open('wb') as log:
            subprocess.run(['docker','logs',container],stdout=log,stderr=subprocess.STDOUT,check=True)
        subprocess.run(['docker','stop',container],check=True,stdout=subprocess.DEVNULL)
        stopped = json.loads(subprocess.check_output(['docker','inspect',container]))[0]
        assert not stopped['State']['Running'] and stopped['State']['Pid']==0
        save('postgres-stopped.json',stopped)
        subprocess.run(['docker','cp',container+':/var/lib/postgresql/data',str(root/'postgres-stopped-data')],check=True)
        subprocess.run(['sudo','-n','chown','-R',f'{os.getuid()}:{os.getgid()}',str(root/'postgres-stopped-data')],check=True)
        mount = json.loads(subprocess.check_output(['docker','volume','inspect',volume]))[0]['Mountpoint']
        script = "import pathlib,sys,hashlib,json; r=pathlib.Path(sys.argv[1]); print(json.dumps({str(p.relative_to(r)):hashlib.file_digest(p.open('rb'),'sha256').hexdigest() for p in r.rglob('*') if p.is_file()}))"
        hashes = json.loads(subprocess.check_output(['sudo','-n','python3','-c',script,mount]))
        for name,digest in hashes.items():
            assert shared.sha(root/'postgres-stopped-data'/name)==digest
        save('postgres-media-copy-verification.json',dict(volume=volume,files=hashes,all_closed_sql_media_bytes_match=True))
    after = shared.source_inventory(revision); save('source-after.json',after); assert before==after
    external_after = {name:shared.sha(Path(name)) for name in inputs}
    save('external-source-after.json',external_after); assert inputs==external_after
    save('closure.json',shared.closure(root))
    proof = fixture_archive.capture(root,root.with_suffix('.tar.gz'),root.with_name(root.name+'-proof'),compresslevel=1)
    print(json.dumps(dict(native_exit_code=code,proof=proof,qualification='Pending independent full domain/projection/rebuild review')),flush=True)
    raise SystemExit(code)


if __name__ == '__main__':
    main()
