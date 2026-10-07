#!/usr/bin/env python3
"""Run default/domain worker controls under race and preserve closed originals."""
import argparse
from datetime import datetime, timezone
import importlib.util
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import time

import fixture_archive
import worker_leaf_wire

REPO = Path(__file__).resolve().parents[1]
TESTS = ['TestWorkerRunnerCompletesWorkflowAndServesMetrics', 'TestWorkerRunnerCompletesWorkflowAndServesMetricsInJetStreamDomain', 'TestWorkerRunnerStartsAfterServerRestart', 'TestWorkerRunnerStartsAfterServerRestartInJetStreamDomain']


def verify_log(log):
    assert log.rstrip().endswith('PASS')
    assert not any(s in log for s in ('DATA RACE', '--- FAIL:', '--- SKIP:'))
    assert sorted(re.findall(r'^--- PASS: (\w+) \([0-9.]+s\)$', log, re.M)) == sorted(TESTS)
    peers = re.findall(r'worker domain admitted node=(\d) domain=WFWORKER server_id=(\w+)', log)
    assert len(peers)==12 and all(sum(n==str(i) for n, _ in peers)==4 for i in range(3))
    assert len({i for _, i in peers})==12
    stopped = re.findall(r'worker domain startup stopped node=(\d) domain=WFWORKER server_id=(\w+)', log)
    restarted = re.findall(r'worker domain startup restarted node=(\d) domain=WFWORKER server_id=(\w+)', log)
    assert sorted(n for n, _ in stopped)==['0','1','2'] and sorted(n for n, _ in restarted)==['0','1','2']
    assert len({i for _, i in stopped})==3 and len({i for _, i in restarted})==3
    assert not ({i for _, i in stopped} & {i for _, i in restarted})
    assert {i for _, i in restarted}.issubset({i for _, i in peers})
    assert max(log.index('worker domain startup stopped node='+n) for n,_ in stopped) < min(log.index('worker domain startup restarted node='+n) for n,_ in restarted)
    routes = re.findall(r'worker real domain=WFWORKER domain_api_requests=(\d+) wrong_prefix_requests=(\d+)', log)
    assert len(routes)==4 and all(int(n)>0 and wrong=='0' for n, wrong in routes)
    for name in ('TestWorkerRunnerCompletesWorkflowAndServesMetrics', 'TestWorkerRunnerCompletesWorkflowAndServesMetricsInJetStreamDomain'):
        modes = re.findall(r'^\s+--- PASS: '+name+r'/(static|kv|auto) \([0-9.]+s\)$', log, re.M)
        assert sorted(modes)==['auto','kv','static']
    return dict(tests=TESTS, real_domain_peer_admissions=12, domain_runs=4,
                traced_domain_api_requests=[int(n) for n, _ in routes], wrong_traced_api_prefix_requests=0,
                scope='Real embedded NATS domain CLI workflow/metrics/assignment/retention and startup after all-server library restart; no native SIGKILL, leaf, full matrix or release qualification.')


STANDALONE_TESTS=['TestWorkerStandaloneCommands','TestWorkerStandaloneCommandsInJetStreamDomain','TestWorkerStandaloneStartsAfterServerRestart','TestWorkerStandaloneStartsAfterServerRestartInJetStreamDomain']


def verify_standalone_log(log):
    assert log.rstrip().endswith('PASS') and not any(x in log for x in ('DATA RACE','--- FAIL:','--- SKIP:'))
    assert sorted(re.findall(r'^--- PASS: (\w+) \([0-9.]+s\)$',log,re.M))==sorted(STANDALONE_TESTS)
    for name in STANDALONE_TESTS[:2]:
        assert sorted(re.findall(r'^\s+--- PASS: '+name+r'/(static|kv|auto) \(',log,re.M))==['auto','kv','static']
    peers=re.findall(r'worker domain admitted node=(\d) domain=WFWORKER server_id=(\w+)',log)
    assert len(peers)==12 and len({i for _,i in peers})==12 and all(sum(n==str(i) for n,_ in peers)==4 for i in range(3))
    old=re.findall(r'worker domain startup stopped node=(\d) domain=WFWORKER server_id=(\w+)',log)
    new=re.findall(r'worker domain startup restarted node=(\d) domain=WFWORKER server_id=(\w+)',log)
    assert sorted(n for n,_ in old)==sorted(n for n,_ in new)==['0','1','2']
    assert len({i for _,i in old})==len({i for _,i in new})==3 and not ({i for _,i in old}&{i for _,i in new})
    assert {i for _,i in new}.issubset({i for _,i in peers})
    assert max(log.index('worker domain startup stopped node='+n) for n,_ in old)<min(log.index('worker domain startup restarted node='+n) for n,_ in new)
    processes=re.findall(r'worker standalone process domain="(WFWORKER)?" pid=(\d+) signal=SIGTERM exit=0 exe_sha256=([0-9a-f]{64})',log)
    assert len(processes)==len({p for _,p,_ in processes})==8 and len({h for _,_,h in processes})==1
    assert sum(d=='WFWORKER' for d,_,_ in processes)==4
    assert len(re.findall(r'worker standalone process ',log))==8
    return dict(tests=STANDALONE_TESTS,actual_standalone_processes=8,real_domain_peer_admissions=12,scope='Actual built worker default/domain static/KV/auto workflow metrics retention and startup after real embedded all-server library restart; eight real SIGTERM joins. No outgoing child wire trace, native SIGKILL, leaf, fullmatrix or release qualification.')


def verify_leaf_log(log):
    assert log.rstrip().endswith('PASS') and not any(x in log for x in ('DATA RACE','--- FAIL:','--- SKIP:'))
    assert re.findall(r'^--- PASS: (\w+) \([0-9.]+s\)$',log,re.M)==[worker_leaf_wire.TEST]
    assert sorted(re.findall(r'^\s+--- PASS: '+worker_leaf_wire.TEST+r'/(static|kv|auto) \(',log,re.M))==['auto','kv','static']
    processes=re.findall(r'worker standalone process domain="WFWORKER" pid=(\d+) signal=SIGTERM exit=0 exe_sha256=([0-9a-f]{64})',log)
    assert len(processes)==len({pid for pid,_ in processes})==3 and len({sha for _,sha in processes})==1
    wires=re.findall(r'worker leaf wire: test='+worker_leaf_wire.TEST+r'/(static|kv|auto) local=WFEDGE remote=WFWORKER leaf_pid=(\d+) connections=1 truncated=false child_bytes=(\d+)',log)
    assert sorted(mode for mode,_,_ in wires)==['auto','kv','static'] and len({pid for _,pid,_ in wires})==3 and all(int(size)>0 for _,_,size in wires)
    return dict(tests=[worker_leaf_wire.TEST],scope='Packaged full static/KV/auto smoke through a real separate-domain leaf; complete wire proof verified separately. No faults or full release claim.')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, required=True)
    parser.add_argument('--case',choices=['commands','standalone-commands','standalone-leaf'],default='commands')
    args = parser.parse_args()
    root = args.root.absolute()
    assert not root.exists() and not root.is_relative_to(REPO)
    spec = importlib.util.spec_from_file_location('shared', REPO/'scripts/run-domain-runtime-controls.py')
    shared = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(shared)
    revision = subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=REPO, text=True).strip()
    before = shared.source_inventory(revision)
    root.mkdir(); (root/'stores').mkdir()
    save = lambda name, value: (root/name).write_text(json.dumps(value, indent=2)+'\n')
    save('source-before.json', before)
    for name in before['files']:
        target = root/'selected-source'/name
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(REPO/name, target)
    env = dict(os.environ, GOMAXPROCS='2', GOMEMLIMIT='1GiB', GOWORK='off', GOFLAGS='', WF_WORKER_TEST_ROOT=str(root/'stores'), WF_WORKER_STANDALONE='1')
    binary = root/'worker-race.test'
    build = ['go', 'test', '-race', '-buildvcs=true', '-c', '-o', str(binary), './cmd/wf-worker']
    tests=([worker_leaf_wire.TEST] if args.case=='standalone-leaf' else STANDALONE_TESTS if args.case=='standalone-commands' else TESTS)
    command = [str(binary), '-test.v', '-test.run=^('+'|'.join(tests)+')$', '-test.count=1', '-test.timeout=4m']
    run_directory = REPO/'cmd/wf-worker'
    save('commands.json', dict(build=build, build_working_directory=str(REPO), run=command, run_working_directory=str(run_directory)))
    with (root/'build.log').open('w') as log:
        subprocess.run(build, cwd=REPO, env=env, stdout=log, stderr=subprocess.STDOUT, check=True)
    info = subprocess.check_output(['go', 'version', '-m', str(binary)], text=True)
    assert '-race=true' in info and re.search(r'github.com/nats-io/nats-server/v2\s+v2\.15\.0\s', info)
    save('binary.json', dict(sha256=shared.sha(binary), build_info=info))
    started = time.monotonic()
    with (root/'native.log').open('w') as log:
        child = subprocess.Popen(command, cwd=run_directory, env=env, stdout=log, stderr=subprocess.STDOUT)
        keys=('GOMAXPROCS','GOMEMLIMIT','GOWORK','GOFLAGS','WF_WORKER_TEST_ROOT','WF_WORKER_STANDALONE')
        actual=shared.live_process_admission.admit(child,command,{key:env[key] for key in keys},run_directory,binary,shared.sha(binary))
        save('actual-sdk.json', actual)
        print('ACTUAL_WORKER_SDK', child.pid, flush=True)
        code = child.wait()
    save('execution.json', dict(source=revision, exit_code=code, elapsed_seconds=time.monotonic()-started,
                               finished_utc=datetime.now(timezone.utc).isoformat()))
    after = shared.source_inventory(revision)
    save('source-after.json', after); assert before==after
    save('closure.json', shared.closure(root))
    plugins = list((root/'stores').glob('wf-worker-plugin-*/handler.so'))
    plugin_records = [dict(path=str(p), sha256=shared.sha(p),
                      build_info=subprocess.check_output(['go','version','-m',str(p)],text=True)) for p in plugins]
    save('plugins.json', plugin_records)
    result = None; error = None
    try:
        assert code==0, 'native command tests failed'
        assert len(plugin_records)==1 and '-race=true' in plugin_records[0]['build_info'], 'exact retained race plugin missing'
        if args.case=='commands':
            result = verify_log((root/'native.log').read_text())
        else:
            result = (verify_leaf_log if args.case=='standalone-leaf' else verify_standalone_log)((root/'native.log').read_text())
            records=[json.loads(p.read_text()) for p in (root/'stores').rglob('standalone.process.json')]
            expected_processes=3 if args.case=='standalone-leaf' else 8
            assert len(records)==len({r['pid'] for r in records})==expected_processes
            assert all(r['signal']=='SIGTERM' and r['exit_code']==0 and r['argv'][0]==r['exe'] and Path(r['exe']).is_relative_to(root/'stores') and shared.sha(r['exe'])==r['exe_sha256'] and 'vcs.revision='+revision in r['build_info'] and 'vcs.modified=false' in r['build_info'] and '-race=true' in r['build_info'] and not Path('/proc',str(r['pid'])).exists() for r in records)
            save('standalone-processes.json',records)
            if args.case=='standalone-leaf':
                proofs=list((root/'stores').glob('wf-worker-leaf-*/leaf-proof.json'));assert len(proofs)==3
                reports=[]
                for proof_path in proofs:
                    report=worker_leaf_wire.validate(proof_path.parent)
                    proof=json.loads(proof_path.read_text());native=json.loads((proof_path.parent/'leaf.process.json').read_text())
                    assert native['pid']==proof['leaf_pid'] and native['reaped'] is True and native['exit_code']==0 and not Path('/proc',str(native['pid'])).exists()
                    assert shared.sha(native['exe'])==native['exe_sha256'] and re.search(r'\bgithub.com/nats-io/nats-server/v2\s+v2\.15\.0\b',native['build_info'])
                    child=next(record for record in records if record['test']==proof['test'])
                    assert child['argv'][child['argv'].index('-url')+1]==proof['proxy_url'] and child['argv'][child['argv'].index('-domain')+1]=='WFWORKER'
                    reports.append(report)
                assert {report['test'] for report in reports}=={worker_leaf_wire.TEST+'/'+mode for mode in ('static','kv','auto')}
                save('leaf-wire-review.json',reports)

    except (AssertionError,ValueError,KeyError) as exc:
        error = str(exc) or 'native coverage rejected'
    save('row-review.json', dict(qualification=result, rejection=error))
    proof = fixture_archive.capture(root, root.with_suffix('.tar.gz'), root.with_name(root.name+'-proof'), compresslevel=1)
    print(json.dumps(dict(qualification=result, rejection=error, proof=proof)), flush=True)
    assert error is None, error


if __name__ == '__main__':
    main()
