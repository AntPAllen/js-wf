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
    env = dict(os.environ, GOMAXPROCS='2', GOMEMLIMIT='1GiB', GOWORK='off', GOFLAGS='', WF_OPERATOR_TEST_ROOT=str(root/'stores'))
    binary = root/'operator-race.test'
    build = ['go', 'test', '-race', '-c', '-o', str(binary), './cmd/wf']
    command = [str(binary), '-test.v', '-test.run=^('+'|'.join(TESTS)+')$', '-test.count=1', '-test.timeout=3m']
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
        proc = Path('/proc', str(child.pid))
        actual_env = dict(v.split(b'=', 1) for v in (proc/'environ').read_bytes().split(b'\0') if b'=' in v)
        actual = dict(pid=child.pid, stat=(proc/'stat').read_text(), exe=str((proc/'exe').resolve()), working_directory=str((proc/'cwd').resolve()),
                      exe_sha256=shared.sha(proc/'exe'),
                      args=[os.fsdecode(v) for v in (proc/'cmdline').read_bytes().split(b'\0') if v],
                      environment={k:os.fsdecode(actual_env[k.encode()]) for k in ('GOMAXPROCS','GOMEMLIMIT','GOWORK','GOFLAGS','WF_OPERATOR_TEST_ROOT')})
        assert actual['args']==command and actual['exe_sha256']==shared.sha(binary) and actual['exe']==str(binary) and actual['working_directory']==str(run_directory)
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
