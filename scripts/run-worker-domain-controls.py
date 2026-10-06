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



def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, required=True)
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
    env = dict(os.environ, GOMAXPROCS='2', GOMEMLIMIT='1GiB', GOWORK='off', GOFLAGS='', WF_WORKER_TEST_ROOT=str(root/'stores'))
    binary = root/'worker-race.test'
    build = ['go', 'test', '-race', '-c', '-o', str(binary), './cmd/wf-worker']
    command = [str(binary), '-test.v', '-test.run=^('+'|'.join(TESTS)+')$', '-test.count=1', '-test.timeout=4m']
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
        proc = Path('/proc', str(child.pid))
        actual_env = dict(v.split(b'=', 1) for v in (proc/'environ').read_bytes().split(b'\0') if b'=' in v)
        actual = dict(pid=child.pid, stat=(proc/'stat').read_text(), exe=str((proc/'exe').resolve()), working_directory=str((proc/'cwd').resolve()),
                      exe_sha256=shared.sha(proc/'exe'),
                      args=[os.fsdecode(v) for v in (proc/'cmdline').read_bytes().split(b'\0') if v],
                      environment={k:os.fsdecode(actual_env[k.encode()]) for k in ('GOMAXPROCS','GOMEMLIMIT','GOWORK','GOFLAGS','WF_WORKER_TEST_ROOT')})
        assert actual['args']==command and actual['exe_sha256']==shared.sha(binary) and actual['exe']==str(binary) and actual['working_directory']==str(run_directory)
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
        result = verify_log((root/'native.log').read_text())
    except AssertionError as exc:
        error = str(exc) or 'native coverage rejected'
    save('row-review.json', dict(qualification=result, rejection=error))
    proof = fixture_archive.capture(root, root.with_suffix('.tar.gz'), root.with_name(root.name+'-proof'), compresslevel=1)
    print(json.dumps(dict(qualification=result, rejection=error, proof=proof)), flush=True)
    assert error is None, error


if __name__ == '__main__':
    main()
