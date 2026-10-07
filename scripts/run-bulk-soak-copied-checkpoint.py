#!/usr/bin/env python3
"""Diagnose a complete original failed checkpoint on fresh verified stores, unchanged20s."""
import argparse
from datetime import datetime, timezone
import hashlib
import importlib.util
import json
import os
import re
from pathlib import Path
import shutil
import subprocess
import sys
import time

import fixture_archive
import live_process_admission

REPO=Path(__file__).resolve().parents[1]
DONOR=Path('/tmp/js-wf-bulk-journal-24h-joined-20261006')
CANONICAL='docs/scale/bulk-journal-24h-2026-10-06/terminal'
TEST='TestRetainedAuditBulkSoakCheckpoint8520VerifiedCopy'


def close_failed_owned_containers(root,sdk_pid,exit_code):
    # Go's SDK timeout bypasses testing cleanups. Never archive a live Docker
    # mount merely because host /proc descriptors show its /data namespace.
    ids=subprocess.check_output(['docker','ps','-aq','--filter',f'name=js-wf-route-{sdk_pid}-'],text=True).split()
    if not ids:return dict(native_exit_code=exit_code,remaining_owned_containers=0,containers=[])
    assert exit_code!=0,'a passing native test must close its own containers'
    out=root/'failed-container-cleanup';out.mkdir()
    report=dict(native_exit_code=exit_code,containers=[])
    for cid in ids:
        info=json.loads(subprocess.check_output(['docker','inspect',cid],text=True))[0]
        assert re.fullmatch(rf'js-wf-route-{sdk_pid}-[0-9]+-n[0-4]',info['Name'].lstrip('/'))
        mounts=[m for m in info['Mounts'] if m['Destination']=='/data'];assert len(mounts)==1
        source=Path(mounts[0]['Source'])
        assert source.parent==root/'restored-original/fixture/cluster' and source.name in {f'node-{n}' for n in range(5)}
        (out/(cid+'-logs.txt')).write_bytes(subprocess.check_output(['docker','logs',cid],stderr=subprocess.STDOUT))
        if info['State']['Running']:
            subprocess.run(['docker','stop','--time','10',cid],check=True,stdout=subprocess.DEVNULL)
        final_read=subprocess.run(['docker','inspect',cid],stdout=subprocess.PIPE,stderr=subprocess.PIPE)
        if final_read.returncode==0:
            final=json.loads(final_read.stdout)[0];assert not final['State']['Running']
            removed=subprocess.run(['docker','rm',cid],stdout=subprocess.PIPE,stderr=subprocess.PIPE)
            if removed.returncode:
                assert info['HostConfig']['AutoRemove']
                assert not subprocess.check_output(['docker','ps','-aq','--filter','id='+cid]).strip()
        else:
            assert info['HostConfig']['AutoRemove'] and b'no such object' in final_read.stderr.lower()
            final=dict(auto_removed_after_stop=True,inspect_exit=final_read.returncode,stderr=final_read.stderr.decode())
        assert not subprocess.check_output(['docker','ps','-aq','--filter','id='+cid]).strip()
        report['containers'].append(dict(before=info,after_stop=final,removed=True))
        (out/'cleanup.json').write_text(json.dumps(report,indent=2)+'\n')
    assert not subprocess.check_output(['docker','ps','-aq','--filter',f'name=js-wf-route-{sdk_pid}-']).strip()
    report['remaining_owned_containers']=0
    return report

def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root',type=Path,required=True)
    parser.add_argument('--checkpoint',type=int,choices=(8520,4160),default=8520,help='Exact original failure profile; latest4160 requires the cursor-owner fault.')
    parser.add_argument('--cpu-profile',action='store_true',help='Opt-in full SDK checker CPU/memory diagnosis; original audit budget unchanged.')
    parser.add_argument('--parallel-decode',action='store_true',help='Explicit experimental bounded ordered decoding; original full cohort and audit budget unchanged.')
    parser.add_argument('--cursor-owner-restart',action='store_true',help='Cold full-cohort R1 cursor-owner SIGKILL/same-store restart, requiring --parallel-decode; no healthy warmup.')
    parser.add_argument('--archive',type=Path,default=Path('/tmp/js-wf-bulk-journal-24h-joined-complete-20261006.tar.gz'),help='Downloaded complete original S3 archive; every byte/member is verified before opening stores.')
    parser.add_argument('--state-watch-connection-loss',action='store_true',help='Controlled real SDK watch connection close before exposure; requires --cursor-owner-restart.')
    parser.add_argument('--state-watch-peer-outage',action='store_true',help='Direct watch peer SIGKILL with three-second offline interval; requires --cursor-owner-restart.')
    parser.add_argument('--state-watch-creation-stall',action='store_true',help='Real R5 WatchAll creation held before publication; latest4160 only, no other watch fault.')
    a=parser.parse_args();root=a.root.absolute()
    global DONOR,CANONICAL
    cutoff=238560; original_source='757454ab9681f044af469a9462ecb7dfc63233ad'
    if a.checkpoint==4160:
        assert a.cursor_owner_restart, 'checkpoint4160 requires the full fault diagnostic'
        DONOR=Path('/tmp/js-wf-parallel-recovery-journal-24h-20261007')
        CANONICAL='docs/scale/parallel-recovery-journal-24h-2026-10-07/terminal-failure'
        cutoff=116480; original_source='bc9f92bdfd1f01ad78d4acd24e6576d46c82a078'

    assert not a.state_watch_peer_outage or (a.cursor_owner_restart and not a.state_watch_connection_loss)
    assert not a.state_watch_connection_loss or a.cursor_owner_restart
    assert not a.state_watch_creation_stall or (a.checkpoint==4160 and a.cursor_owner_restart and not a.state_watch_peer_outage and not a.state_watch_connection_loss)
    assert not a.cursor_owner_restart or (a.parallel_decode and not a.cpu_profile),'cursor fault requires explicit parallel decode and no CPU instrumentation'
    package='./integrity' if a.cursor_owner_restart else './integration'
    test=f'TestRetainedAuditBulkSoakCheckpoint{a.checkpoint}CursorOwnerRestartVerifiedCopy' if a.cursor_owner_restart else TEST
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
    donor_present=DONOR.exists()
    if donor_present:
        assert fixture_archive.inventory(DONOR)==inventory['files'],'original complete closed census changed'
        original_closure=shared.closure(DONOR)
    else:
        original_closure=dict(local_original_retired=True,scope='Complete verified archive is the donor; no original local stores opened.')
    execution=committed('execution.json')
    assert execution['status']=='failed' and execution['test_exit_code']==1 and execution['source']==original_source
    if donor_present: assert execution==json.loads((DONOR/'execution.json').read_text())
    root.mkdir();(root/'originals').mkdir()
    save=lambda name,value:(root/name).write_text(json.dumps(value,indent=2)+'\n')
    save('source-before.json',before)
    save('donor-reference.json',dict(root=str(DONOR),local_original_present=donor_present,checkpoint=a.checkpoint,canonical=CANONICAL,original_execution=execution,archive=meta,receipt=receipt,visible_closure=original_closure))
    for name in before['files']:
        p=root/'selected-source'/name;p.parent.mkdir(parents=True,exist_ok=True);shutil.copyfile(REPO/name,p)
    restored=root/'restored-original'
    report=fixture_archive.restore(a.archive,dict(bytes=meta['archive_bytes'],sha256=meta['archive_sha256']),inventory,restored)
    save('restore-verification.json',dict(destination=str(restored),**report))
    assert json.loads((restored/'execution.json').read_text())==execution
    prefix='fixture/cluster/'
    stores={name:row for name,row in inventory['files'].items() if any(name.startswith(prefix+f'node-{n}/') for n in range(5))}
    assert stores and len({name.split('/')[2] for name in stores})==5
    observations=[json.loads(line) for line in (restored/'watch-servers.jsonl').read_text().splitlines()]
    identities={x['inspect']['Args'][x['inspect']['Args'].index('-n')+1].rsplit('-n',1)[0] for x in observations}
    assert len(identities)==1
    identity=identities.pop();original_server_hashes={x['sha256'] for x in observations};assert len(original_server_hashes)==1
    save('copied-store-admission.json',dict(identity=identity,stores_prefix=prefix,files=stores,original_server_sha256=original_server_hashes.pop(),checkpoint=a.checkpoint,cutoff=cutoff,original_native_failure_unchanged=True))
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
    if a.state_watch_creation_stall:env['WF_AUDIT_BULK_SOAK_STATE_CREATION_STALL']='1'
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
        actual=live_process_admission.admit(child,command,profile,REPO,binary,shared.sha(binary))
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
    save('container-cleanup-after-sdk.json',close_failed_owned_containers(root,child.pid,code))
    after=shared.source_inventory(revision);save('source-after.json',after);assert before==after
    external_after={name:shared.sha(Path(name)) for name in inputs};save('external-source-after.json',external_after);assert inputs==external_after
    if donor_present: assert fixture_archive.inventory(DONOR)==inventory['files']
    save('original-after-verification.json',dict(all_original_bytes_modes_mtimes_unchanged=True,local_original_present=donor_present,original_files=len(inventory['files']),visible_closure=shared.closure(DONOR) if donor_present else original_closure,scope='Verified complete archive preserved; only fresh restored stores were opened.'))
    save('closure.json',shared.closure(root));shutil.copyfile(__file__,root/'executed-producer.py')
    proof=fixture_archive.capture(root,root.with_suffix('.tar.gz'),root.with_name(root.name+'-proof'),compresslevel=1)
    print(json.dumps(dict(native_exit_code=code,proof=proof,qualification='Pending independent copied full-cohort review; original24h failure unchanged')),flush=True)
    raise SystemExit(code)


if __name__=='__main__':main()
