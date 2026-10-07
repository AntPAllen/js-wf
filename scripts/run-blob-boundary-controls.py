#!/usr/bin/env python3
"""Retain the real JetStream counterexample to unsafe concurrent quiescent GC."""
import argparse
from datetime import datetime,timezone
import importlib.util
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import time
import fixture_archive
import live_process_admission

REPO=Path(__file__).resolve().parents[1]
TEST='TestBlobSweepConcurrentRefreshContract'

def verify_log(log):
    assert log.rstrip().endswith('PASS') and not any(token in log for token in ('DATA RACE','--- FAIL:','--- SKIP:'))
    assert re.findall(r'^--- PASS: (\w+) \([0-9.]+s\)$',log,re.M)==[TEST]
    assert sorted(re.findall(r'^\s+--- PASS: '+TEST+r'/(quiescent|refresh_after_census) \(',log,re.M))==['quiescent','refresh_after_census']
    rows=re.findall(r'native blob boundary: mode=(quiescent|refresh_after_census) old_nuid=(\w+) fresh_nuid=(\w+) acked_ref=true deleted=(\d+) dangling=(true|false) server_id=(\w+)',log)
    assert len(rows)==2 and {mode for mode,*_ in rows}=={'quiescent','refresh_after_census'}
    assert all(old!=new and (deleted,dangling)==(('0','false') if mode=='quiescent' else ('1','true')) for mode,old,new,deleted,dangling,_ in rows)
    assert len({row[-1] for row in rows})==2
    return dict(counterexample_confirmed=True,quiescent_control_retained=True,scope='R1 native contract for violating quiescent collector precondition, not safe online GC or broad release qualification.')

def main():
    parser=argparse.ArgumentParser();parser.add_argument('--root',type=Path,required=True);args=parser.parse_args()
    root=args.root.absolute();assert not root.exists() and not root.is_relative_to(REPO)
    spec=importlib.util.spec_from_file_location('shared',REPO/'scripts/run-domain-runtime-controls.py');shared=importlib.util.module_from_spec(spec);spec.loader.exec_module(shared)
    rev=subprocess.check_output(['git','rev-parse','HEAD'],cwd=REPO,text=True).strip();before=shared.source_inventory(rev)
    root.mkdir();(root/'stores').mkdir();save=lambda name,value:(root/name).write_text(json.dumps(value,indent=2)+'\n')
    save('source-before.json',before)
    for name in before['files']:
        target=root/'selected-source'/name;target.parent.mkdir(parents=True,exist_ok=True);shutil.copyfile(REPO/name,target)
    env=dict(os.environ,GOMAXPROCS='2',GOMEMLIMIT='1GiB',WF_BLOB_BOUNDARY_ROOT=str(root/'stores/scenario'))
    binary=root/'retention-race.test';build=['go','test','-buildvcs=true','-race','-c','-o',str(binary),'./retention']
    command=[str(binary),'-test.v','-test.run=^'+TEST+'$','-test.count=1','-test.timeout=3m'];save('commands.json',dict(build=build,run=command))
    with (root/'build.log').open('w') as log:subprocess.run(build,cwd=REPO,env=env,stdout=log,stderr=subprocess.STDOUT,check=True)
    info=subprocess.check_output(['go','version','-m',str(binary)],text=True);assert '-race=true' in info and 'vcs.revision='+rev in info and 'vcs.modified=false' in info
    save('binary.json',dict(sha256=shared.sha(binary),build_info=info));started=time.monotonic()
    with (root/'native.log').open('w') as log:
        child=subprocess.Popen(command,cwd=REPO,env=env,stdout=log,stderr=subprocess.STDOUT)
        actual=live_process_admission.admit(child,command,{key:env[key] for key in ('GOMAXPROCS','GOMEMLIMIT','WF_BLOB_BOUNDARY_ROOT')},REPO,binary,shared.sha(binary));save('actual-sdk.json',actual)
        print('ACTUAL_BLOB_BOUNDARY_SDK',child.pid,flush=True);code=child.wait()
    save('execution.json',dict(source=rev,exit_code=code,elapsed_seconds=time.monotonic()-started,finished_utc=datetime.now(timezone.utc).isoformat()))
    after=shared.source_inventory(rev);assert before==after;save('source-after.json',after);save('closure.json',shared.closure(root))
    result=None;error=None
    try:
        assert code==0,'native contract failed';result=verify_log((root/'native.log').read_text())
        for mode in ('quiescent','refresh_after_census'):
            proof=json.loads((root/'stores/scenario'/mode/'boundary-proof.json').read_text())
            assert proof['mode']==mode and proof['old_nuid']!=proof['fresh_nuid'] and proof['object']==proof['acknowledged_reference'] and proof['invocation_sequence']>0
            assert proof['dangling']==(mode=='refresh_after_census') and proof['sweep']['deleted']==(1 if mode=='refresh_after_census' else 0)
    except (AssertionError,ValueError,KeyError,TypeError) as exc:error=str(exc) or type(exc).__name__
    save('row-review.json',dict(qualification=result,rejection=error))
    proof=fixture_archive.capture(root,root.with_suffix('.tar.gz'),root.with_name(root.name+'-proof'),compresslevel=1)
    print(json.dumps(dict(qualification=result,rejection=error,proof=proof)),flush=True);assert error is None,error

if __name__=='__main__':main()
