#!/usr/bin/env python3
"""Run default/domain operator controls under race and preserve closed originals."""
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
import live_process_admission

REPO = Path(__file__).resolve().parents[1]
TESTS = ['TestOperatorCommands', 'TestOperatorCommandsInJetStreamDomain']


def verify_log(log):
    assert log.rstrip().endswith('PASS')
    assert not any(s in log for s in ('DATA RACE', '--- FAIL:', '--- SKIP:'))
    assert sorted(re.findall(r'^--- PASS: (\w+) \([0-9.]+s\)$', log, re.M)) == sorted(TESTS)
    peers = re.findall(r'operator domain admitted node=(\d) domain=WFOPS server_id=(\w+)', log)
    assert sorted(n for n, _ in peers) == ['0', '1', '2'] and len({i for _, i in peers}) == 3
    routes = re.findall(r'operator real domain=WFOPS peers=3 domain_api_requests=(\d+) wrong_prefix_requests=(\d+)', log)
    assert len(routes) == 1 and int(routes[0][0]) > 0 and routes[0][1] == '0'
    return dict(tests=TESTS, real_domain_peers=3, traced_domain_api_requests=int(routes[0][0]),
                wrong_traced_api_prefix_requests=0,
                scope='Healthy default/domain command controls with real embedded NATS peers; no fault matrix, PostgreSQL/domain or full release qualification.')


DAEMON_TESTS = ['TestOperatorDaemonSignals', 'TestOperatorDaemonSignalsInJetStreamDomain']


def verify_daemon_log(log):
    assert log.rstrip().endswith('PASS') and not any(s in log for s in ('DATA RACE','--- FAIL:','--- SKIP:'))
    assert sorted(re.findall(r'^--- PASS: (\w+) \([0-9.]+s\)$',log,re.M))==sorted(DAEMON_TESTS)
    for name in DAEMON_TESTS:
        modes=re.findall(r'^\s+--- PASS: '+name+r'/(project|tombstone-loop)/(startup|running) \(',log,re.M)
        assert sorted(modes)==[('project','running'),('project','startup'),('tombstone-loop','running'),('tombstone-loop','startup')]
    peers=re.findall(r'operator daemon domain admitted node=(\d) domain=WFOPS server_id=(\w+)',log)
    assert sorted(n for n,_ in peers)==['0','1','2'] and len({i for _,i in peers})==3
    exits=re.findall(r'operator daemon stopped command=(project|tombstone-loop) stage=(startup|running) domain="(WFOPS)?" signal=(terminated|interrupt) exit=0 pid=(\d+)',log)
    assert len(exits)==8 and len({p for _,_,_,_,p in exits})==8
    assert len({(c,s,d) for c,s,d,_,_ in exits})==8
    assert all(signal==('terminated' if stage=='startup' else 'interrupt') for _,stage,_,signal,_ in exits)
    fatal=re.findall(r'operator daemon fatal startup domain="(WFOPS)?" error=stream-not-found exit=1',log)
    assert sorted(fatal)==['','WFOPS']
    return dict(tests=DAEMON_TESTS,real_domain_peers=3,real_signal_subprocesses=8,unrelated_fatal_startups=2,
                scope='Default/domain operator daemon startup/steady SIGTERM/SIGINT with controlled startup client-trace delay; no SQL/domain fullscale, native server fault or fullrelease qualification.')



STANDALONE_TESTS=['TestOperatorStandaloneCommands','TestOperatorStandaloneCommandsInJetStreamDomain']
LEAF_TESTS=['TestOperatorStandaloneCommandsThroughLeaf']

def verify_leaf_log(log):
    assert log.rstrip().endswith('PASS') and not any(s in log for s in ('DATA RACE','--- FAIL:','--- SKIP:'))
    assert re.findall(r'^--- PASS: (\w+) \([0-9.]+s\)$',log,re.M)==LEAF_TESTS
    peers=re.findall(r'operator domain admitted node=(\d) domain=WFOPS server_id=(\w+)',log)
    assert sorted(n for n,_ in peers)==['0','1','2'] and len({i for _,i in peers})==3
    summaries=re.findall(r'operator standalone commands domain="WFOPS" processes=(\d+) exe_sha256=([0-9a-f]{64})',log)
    assert len(summaries)==1 and summaries[0][0]=='23'
    processes=re.findall(r'operator standalone process domain="WFOPS" pid=(\d+) exit=(-?\d+)',log)
    assert len(processes)==len({pid for pid,_ in processes})==23 and sum(code=='1' for _,code in processes)==5
    assert all(code in ('0','1') for _,code in processes)
    wire=re.findall(r'operator child wire: pid=(\d+) offline=(true|false) connections=(\d+) truncated=false client_bytes=(\d+) server_bytes=(\d+)',log)
    assert len(wire)==len({row[0] for row in wire})==23 and {row[0] for row in wire}=={pid for pid,_ in processes}
    assert sum(row[1]=='true' for row in wire)==2
    assert all((row[2],row[3],row[4])==('0','0','0') if row[1]=='true' else row[2]=='1' and int(row[3])>0 and int(row[4])>0 for row in wire)
    assert len(re.findall(r'operator leaf: test='+LEAF_TESTS[0]+r' local=WFEDGE remote=WFOPS leaf_pid=\d+ local_streams=0',log))==1
    return dict(tests=LEAF_TESTS,actual_standalone_processes=23,online_children=21,offline_children=2,scope='Focused healthy packaged operator leaf suite only; no daemon, SQL, injected fault or full release acceptance.')

def verify_standalone_log(log):
    assert log.rstrip().endswith('PASS') and not any(s in log for s in ('DATA RACE','--- FAIL:','--- SKIP:'))
    assert sorted(re.findall(r'^--- PASS: (\w+) \([0-9.]+s\)$',log,re.M))==sorted(STANDALONE_TESTS)
    peers=re.findall(r'operator domain admitted node=(\d) domain=WFOPS server_id=(\w+)',log)
    assert sorted(n for n,_ in peers)==['0','1','2'] and len({i for _,i in peers})==3
    summaries=re.findall(r'operator standalone commands domain="(WFOPS)?" processes=(\d+) exe_sha256=([0-9a-f]{64})',log)
    assert len(summaries)==2 and sorted(d for d,_,_ in summaries)==['','WFOPS'] and len({h for _,_,h in summaries})==1
    processes=re.findall(r'operator standalone process domain="(WFOPS)?" pid=(\d+) exit=(-?\d+)',log)
    assert len({p for _,p,_ in processes})==len(processes)
    for domain,count,_ in summaries:
        group=[code for d,_,code in processes if d==domain]
        assert len(group)==int(count)==(23 if domain else 22) and set(group)=={'0','1'}
        assert group.count('1')==(5 if domain else 4)
    return dict(tests=STANDALONE_TESTS,real_domain_peers=3,actual_standalone_processes=len(processes),
                scope='Compiled wf default/domain command process coverage and actual exit/stdout contracts; no outgoing wire-prefix trace, daemon/SQL/fault/fullrelease qualification.')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, required=True)
    parser.add_argument('--case', choices=['commands','daemon-signals','standalone-commands','standalone-leaf'],default='commands')
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
    env = dict(os.environ, GOMAXPROCS='2', GOMEMLIMIT='1GiB', GOWORK='off', GOFLAGS='', WF_OPERATOR_TEST_ROOT=str(root/'stores'), WF_OPERATOR_STANDALONE='1')
    binary = root/'operator-race.test'
    build = ['go', 'test', '-race', '-buildvcs=true', '-c', '-o', str(binary), './cmd/wf']
    tests = TESTS if args.case=='commands' else DAEMON_TESTS if args.case=='daemon-signals' else LEAF_TESTS if args.case=='standalone-leaf' else STANDALONE_TESTS
    command = [str(binary), '-test.v', '-test.run=^('+'|'.join(tests)+')$', '-test.count=1', '-test.timeout=3m']
    run_directory = REPO/'cmd/wf'
    save('commands.json', dict(build=build, build_working_directory=str(REPO), run=command, run_working_directory=str(run_directory)))
    with (root/'build.log').open('w') as log:
        subprocess.run(build, cwd=REPO, env=env, stdout=log, stderr=subprocess.STDOUT, check=True)
    info = subprocess.check_output(['go', 'version', '-m', str(binary)], text=True)
    assert '-race=true' in info and re.search(r'github.com/nats-io/nats-server/v2\s+v2\.15\.0\s', info)
    save('binary.json', dict(sha256=shared.sha(binary), build_info=info))
    started = time.monotonic()
    with (root/'native.log').open('w') as log:
        child = subprocess.Popen(command, cwd=run_directory, env=env, stdout=log, stderr=subprocess.STDOUT)
        keys=('GOMAXPROCS','GOMEMLIMIT','GOWORK','GOFLAGS','WF_OPERATOR_TEST_ROOT','WF_OPERATOR_STANDALONE')
        actual=live_process_admission.admit(child,command,{k:env[k] for k in keys},run_directory,binary,shared.sha(binary))
        save('actual-sdk.json', actual)
        print('ACTUAL_OPERATOR_SDK', child.pid, flush=True)
        code = child.wait()
    save('execution.json', dict(source=revision, exit_code=code, elapsed_seconds=time.monotonic()-started,
                               finished_utc=datetime.now(timezone.utc).isoformat()))
    after = shared.source_inventory(revision)
    save('source-after.json', after); assert before==after
    save('closure.json', shared.closure(root))
    plugins = list((root/'stores').glob('js-wf-replay-plugin-*/handler.so'))
    plugin_records = [dict(path=str(p), sha256=shared.sha(p),
                      build_info=subprocess.check_output(['go','version','-m',str(p)],text=True)) for p in plugins]
    save('plugins.json', plugin_records)
    result = None; error = None
    try:
        assert code==0, 'native command tests failed'
        if args.case=='commands':
            assert len(plugin_records)==1 and '-race=true' in plugin_records[0]['build_info'], 'exact retained race plugin missing'
            result = verify_log((root/'native.log').read_text())
        elif args.case=='daemon-signals':
            records=[json.loads(p.read_text()) for p in (root/'stores').rglob('*.process.json')]
            assert len(records)==8 and len({r['pid'] for r in records})==8
            assert all(r['exe_sha256']==shared.sha(binary) and r['argv']==[str(binary),'-test.run=^TestOperatorDaemonProcessHelper$'] and not Path('/proc',str(r['pid'])).exists() for r in records)
            save('daemon-processes.json',records)
            result=verify_daemon_log((root/'native.log').read_text())
        else:
            assert len(plugin_records)==1 and '-race=true' in plugin_records[0]['build_info'], 'exact retained race plugin missing'
            records=[json.loads(p.read_text()) for p in (root/'stores').rglob('standalone.process.json')]
            result=(verify_leaf_log if args.case=='standalone-leaf' else verify_standalone_log)((root/'native.log').read_text())
            assert len(records)==result['actual_standalone_processes']
            assert all(r['argv'][0].startswith(str(root/'stores')) and shared.sha(r['argv'][0])==r['exe_sha256'] and 'vcs.revision='+revision in r['build_info'] and 'vcs.modified=false' in r['build_info'] and '-race=true' in r['build_info'] and not Path('/proc',str(r['pid'])).exists() for r in records)
            save('standalone-processes.json',records)
            if args.case=='standalone-leaf':
                import operator_leaf_wire
                wire=operator_leaf_wire.validate(root/'stores')
                assert {r['pid'] for r in wire['processes']}=={r['pid'] for r in records}
                save('leaf-wire-review.json',wire)
                leaves=[json.loads(p.read_text()) for p in (root/'stores').rglob('leaf.process.json')]
                assert len(leaves)==1 and leaves[0]['reaped'] is True and leaves[0]['exit_code']==0 and not Path('/proc',str(leaves[0]['pid'])).exists()
                assert shared.sha(leaves[0]['exe'])==leaves[0]['exe_sha256']
                save('leaf-processes.json',leaves)
    except (AssertionError,ValueError,KeyError,TypeError) as exc:
        error = str(exc) or 'native coverage rejected'
    save('row-review.json', dict(qualification=result, rejection=error))
    proof = fixture_archive.capture(root, root.with_suffix('.tar.gz'), root.with_name(root.name+'-proof'), compresslevel=1)
    print(json.dumps(dict(qualification=result, rejection=error, proof=proof)), flush=True)
    assert error is None, error


if __name__ == '__main__':
    main()
