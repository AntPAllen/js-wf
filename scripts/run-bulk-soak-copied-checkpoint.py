#!/usr/bin/env python3
"""Diagnose the full failed checkpoint8520 on fresh verified stores, unchanged20s."""
import argparse
from datetime import datetime, timezone
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import time

import fixture_archive

REPO=Path(__file__).resolve().parents[1]
DONOR=Path('/tmp/js-wf-bulk-journal-24h-joined-20261006')
CANONICAL='docs/scale/bulk-journal-24h-2026-10-06/terminal'
TEST='TestRetainedAuditBulkSoakCheckpoint8520VerifiedCopy'


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root',type=Path,required=True)
    parser.add_argument('--cpu-profile',action='store_true',help='Opt-in full SDK checker CPU/memory diagnosis; original audit budget unchanged.')
    parser.add_argument('--parallel-decode',action='store_true',help='Explicit experimental bounded ordered decoding; original full cohort and audit budget unchanged.')
    parser.add_argument('--cursor-owner-restart',action='store_true',help='Cold full-cohort R1 cursor-owner SIGKILL/same-store restart, requiring --parallel-decode; no healthy warmup.')
    parser.add_argument('--archive',type=Path,default=Path('/tmp/js-wf-bulk-journal-24h-joined-complete-20261006.tar.gz'),help='Downloaded complete original S3 archive; every byte/member is verified before opening stores.')
    parser.add_argument('--state-watch-connection-loss',action='store_true',help='Controlled real SDK watch connection close before exposure; requires --cursor-owner-restart.')
    parser.add_argument('--state-watch-peer-outage',action='store_true',help='Direct watch peer SIGKILL with three-second offline interval; requires --cursor-owner-restart.')
    a=parser.parse_args();root=a.root.absolute()
    assert not a.state_watch_peer_outage or (a.cursor_owner_restart and not a.state_watch_connection_loss)
    assert not a.state_watch_connection_loss or a.cursor_owner_restart
    assert not a.cursor_owner_restart or (a.parallel_decode and not a.cpu_profile),'cursor fault requires explicit parallel decode and no CPU instrumentation'
    package='./integrity' if a.cursor_owner_restart else './integration'
    test='TestRetainedAuditBulkSoakCheckpoint8520CursorOwnerRestartVerifiedCopy' if a.cursor_owner_restart else TEST
    assert not root.exists() and not root.is_relative_to(REPO) and not root.is_relative_to(DONOR)
    assert shutil.disk_usage(root.parent).free>=20*(1<<30),'20GiB restore/collection reserve required'
    spec=importlib.util.spec_from_file_location('shared',REPO/'scripts/run-domain-runtime-controls.py')
    shared=importlib.util.module_from_spec(spec);spec.loader.exec_module(shared)
    revision=subprocess.check_output(['git','rev-parse','HEAD'],cwd=REPO,text=True).strip()
    before=shared.source_inventory(revision)
    def committed(name):
        data=subprocess.check_output(['git','cat-file','blob',revision+':'+CANONICAL+'/'+name],cwd=REPO)
        assert data==(REPO/CANONICAL/name).read_bytes()
        return json.loads(data)
    meta=committed('archive-verification.json');inventory=committed('fixture-inventory.json');receipt=committed('s3-readback.json')
    assert receipt['archive']['full_readback']==dict(bytes=meta['archive_bytes'],sha256=meta['archive_sha256'])
    assert hashlib.sha256((REPO/CANONICAL/'fixture-inventory.json').read_bytes()).hexdigest()==meta['inventory_sha256']
    assert fixture_archive.inventory(DONOR)==inventory['files'],'original complete closed census changed'
    original_closure=shared.closure(DONOR)
    execution=json.loads((DONOR/'execution.json').read_text());assert execution['status']=='failed' and execution['test_exit_code']==1 and execution['source']=='757454ab9681f044af469a9462ecb7dfc63233ad'
    root.mkdir();(root/'originals').mkdir()
    save=lambda name,value:(root/name).write_text(json.dumps(value,indent=2)+'\n')
    save('source-before.json',before)
    save('donor-reference.json',dict(root=str(DONOR),canonical=CANONICAL,original_execution=execution,archive=meta,receipt=receipt,visible_closure=original_closure))
    for name in before['files']:
        p=root/'selected-source'/name;p.parent.mkdir(parents=True,exist_ok=True);shutil.copyfile(REPO/name,p)
    restored=root/'restored-original'
    report=fixture_archive.restore(a.archive,dict(bytes=meta['archive_bytes'],sha256=meta['archive_sha256']),inventory,restored)
    save('restore-verification.json',dict(destination=str(restored),**report))
    prefix='fixture/cluster/'
    stores={name:row for name,row in inventory['files'].items() if any(name.startswith(prefix+f'node-{n}/') for n in range(5))}
    assert stores and len({name.split('/')[2] for name in stores})==5
    observations=[json.loads(line) for line in (restored/'watch-servers.jsonl').read_text().splitlines()]
    identities={x['inspect']['Args'][x['inspect']['Args'].index('-n')+1].rsplit('-n',1)[0] for x in observations}
    assert len(identities)==1
    identity=identities.pop();original_server_hashes={x['sha256'] for x in observations};assert len(original_server_hashes)==1
    save('copied-store-admission.json',dict(identity=identity,stores_prefix=prefix,files=stores,original_server_sha256=original_server_hashes.pop(),cutoff=238560,original_native_failure_unchanged=True))
    # Capture selected external compiler inputs for both the test SDK and Docker server.
    fmt='{{.Dir}}|{{join .GoFiles " "}}|{{join .CgoFiles " "}}|{{join .TestGoFiles " "}}|{{join .XTestGoFiles " "}}'
    deps=subprocess.check_output(['go','list','-deps','-test','-f',fmt,package,'github.com/nats-io/nats-server/v2'],cwd=REPO,text=True)
    (root/'dependencies.txt').write_text(deps)
    goroot=Path(subprocess.check_output(['go','env','GOROOT'],text=True).strip());modules=Path(subprocess.check_output(['go','env','GOMODCACHE'],text=True).strip())
    inputs={};captured={}
    for line in deps.splitlines():
        directory,*groups=line.split('|')
        for name in ' '.join(groups).split():
            path=(Path(directory)/name).resolve()
            if not path.is_file() or path.is_relative_to(REPO) or str(path) in inputs:continue
            inputs[str(path)]=shared.sha(path)
            relative=Path('modules')/path.relative_to(modules) if path.is_relative_to(modules) else Path('toolchain')/path.relative_to(goroot) if path.is_relative_to(goroot) else Path('other')/str(path).lstrip('/')
            target=root/'selected-external-source'/relative;target.parent.mkdir(parents=True,exist_ok=True);shutil.copyfile(path,target)
            assert shared.sha(target)==inputs[str(path)];captured[str(path)]=str(target.relative_to(root))
    save('external-source-before.json',inputs);save('external-captured-paths.json',captured)
    env={k:v for k,v in os.environ.items() if not k.startswith('WF_')}
    env.update(GOMAXPROCS='4',GOGC='500',GOMEMLIMIT='4GiB',GOWORK='off',GOFLAGS='',WF_AUDIT_BULK_SOAK_STORES=str(restored/'fixture/cluster'),WF_AUDIT_BULK_SOAK_ROOT=str(root/'originals'),WF_AUDIT_BULK_SOAK_IDENTITY=identity,WF_TIER3_EXPLICIT_ROUTE_SEEDS='1',WF_TIER3_SYNC_INTERVAL='2m')
    if a.cpu_profile:env['WF_AUDIT_BULK_SOAK_CPU_PROFILE']='1'
    if a.parallel_decode:env['WF_AUDIT_BULK_SOAK_PARALLEL_DECODE']='1'
    if a.cursor_owner_restart:env['WF_AUDIT_BULK_SOAK_CURSOR_OWNER_RESTART']='1'
    if a.state_watch_connection_loss:env['WF_AUDIT_BULK_SOAK_STATE_CONNECTION_LOSS']='1'
    if a.state_watch_peer_outage:env['WF_AUDIT_BULK_SOAK_STATE_PEER_OUTAGE']='1'
    profile={k:v for k,v in env.items() if k.startswith('WF_') or k in ('GOMAXPROCS','GOGC','GOMEMLIMIT','GOWORK','GOFLAGS')}
    binary=root/('integrity.test' if a.cursor_owner_restart else 'integration.test');build=['go','test','-p=1','-buildvcs=true','-c','-o',str(binary),package]
    command=[str(binary),'-test.run=^'+test+'$','-test.count=1','-test.v','-test.timeout=6m']
    save('commands.json',dict(build=build,test=command,environment=profile,working_directory=str(REPO)))
    with (root/'build.log').open('wb') as log:subprocess.run(build,cwd=REPO,env=env,stdout=log,stderr=subprocess.STDOUT,check=True)
    info=subprocess.check_output(['go','version','-m',str(binary)],text=True)
    assert 'vcs.revision='+revision in info and 'vcs.modified=false' in info and '-race=true' not in info
    save('binary.json',dict(sha256=shared.sha(binary),build_info=info))
    actual_servers=[];seen=set();(root/'actual-containers').mkdir();started=time.monotonic()
    with (root/'native.log').open('wb') as log:
        child=subprocess.Popen(command,cwd=REPO,env=env,stdout=log,stderr=subprocess.STDOUT)
        proc=Path('/proc',str(child.pid));actual_env=dict(x.split(b'=',1) for x in (proc/'environ').read_bytes().split(b'\0') if b'=' in x)
        actual=dict(pid=child.pid,stat=(proc/'stat').read_text(),args=[os.fsdecode(x) for x in (proc/'cmdline').read_bytes().split(b'\0') if x],exe=str((proc/'exe').resolve()),exe_sha256=shared.sha(proc/'exe'),working_directory=str((proc/'cwd').resolve()),environment={k:os.fsdecode(actual_env[k.encode()]) for k in profile})
        assert actual['args']==command and actual['environment']==profile and actual['exe_sha256']==shared.sha(binary)
        save('actual-sdk.json',actual);save('execution.json',dict(source=revision,status='running',started_utc=datetime.now(timezone.utc).isoformat()))
        print('ACTUAL_COPIED_CHECKPOINT_SDK',child.pid,revision,flush=True)
        while child.poll() is None:
            ids=subprocess.check_output(['docker','ps','-q','--filter',f'name=js-wf-route-{child.pid}-'],text=True).split()
            for cid in ids:
                try:
                    observed=json.loads(subprocess.check_output(['docker','inspect',cid],text=True,stderr=subprocess.DEVNULL))[0];pid=observed['State']['Pid']
                    if not pid or (cid,pid) in seen:continue
                    p=Path('/proc',str(pid));digest=shared.sha(p/'exe');target=root/'actual-containers'/digest
                    if not target.exists():shutil.copyfile(p/'exe',target)
                    assert shared.sha(target)==digest
                    actual_servers.append(dict(container=observed,pid=pid,sha256=digest,stat=(p/'stat').read_text(),args=[os.fsdecode(x) for x in (p/'cmdline').read_bytes().split(b'\0') if x],build_info=subprocess.check_output(['go','version','-m',str(p/'exe')],text=True),observed_utc=datetime.now(timezone.utc).isoformat()))
                    seen.add((cid,pid));save('actual-servers.json',actual_servers)
                except (FileNotFoundError,ProcessLookupError,subprocess.CalledProcessError):continue
            time.sleep(.5)
        code=child.wait()
    save('execution.json',dict(source=revision,status='passed' if code==0 else 'failed',exit_code=code,elapsed_seconds=time.monotonic()-started,finished_utc=datetime.now(timezone.utc).isoformat()))
    after=shared.source_inventory(revision);save('source-after.json',after);assert before==after
    external_after={name:shared.sha(Path(name)) for name in inputs};save('external-source-after.json',external_after);assert inputs==external_after
    assert fixture_archive.inventory(DONOR)==inventory['files']
    save('original-after-verification.json',dict(all_original_bytes_modes_mtimes_unchanged=True,original_files=len(inventory['files']),visible_closure=shared.closure(DONOR)))
    save('closure.json',shared.closure(root));shutil.copyfile(__file__,root/'executed-producer.py')
    proof=fixture_archive.capture(root,root.with_suffix('.tar.gz'),root.with_name(root.name+'-proof'),compresslevel=1)
    print(json.dumps(dict(native_exit_code=code,proof=proof,qualification='Pending independent copied full-cohort review; original24h failure unchanged')),flush=True)
    raise SystemExit(code)


if __name__=='__main__':main()
