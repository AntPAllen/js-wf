#!/usr/bin/env python3
"""Run opt-in domain controls under race and retain complete closed evidence.

A passing focused row does not qualify the full chaos matrix or 24-hour soak.
"""
import argparse
from datetime import datetime, timezone
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import time

import fixture_archive

REPO = Path(__file__).resolve().parent.parent
RESULT = 'TestResultReadVerifiesWeakAbsence'
SNAPSHOT = 'TestSnapshotManifestReadsUseLeader'
KILL = 'TestContinuationRetirementReuseInJetStreamDomainWithManifestLossAndAllServerSIGKILL'
EXPIRY = 'TestContinuationRetirementReuseInJetStreamDomainWithManifestLossAndLeaseExpiryAcrossAllServerSIGKILL'
WEAK_FRAME = 'TestContinuationRetirementReuseInJetStreamDomainWithManifestLossWeakFrameAbsenceAndAllServerSIGKILL'
WEAK_EXPIRY = 'TestContinuationRetirementReuseInJetStreamDomainWithManifestLossWeakFrameAbsenceAndLeaseExpiryAcrossAllServerSIGKILL'
LEGACY_WEAK_EXPIRY = 'TestContinuationRetirementReuseInLegacyJetStreamDomainWithManifestLossWeakFrameAbsenceAndLeaseExpiryAcrossAllServerSIGKILL'
CASES = {'read-controls': [RESULT, RESULT+'InJetStreamDomain', SNAPSHOT, SNAPSHOT+'InJetStreamDomain'],
         'retirement-server-kill': [KILL, EXPIRY], 'retirement-weak-frame': [WEAK_FRAME],
         'retirement-weak-frame-expiry': [WEAK_EXPIRY], 'legacy-retirement-weak-frame-expiry': [LEGACY_WEAK_EXPIRY]}


def sha(path):
    with Path(path).open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def require(value, message):
    if not value:
        raise ValueError(message)


def verify_log(case, log):
    require(log.rstrip().endswith('PASS'), 'native terminal PASS missing')
    require('DATA RACE' not in log and '--- SKIP:' not in log and '--- FAIL:' not in log,
            'race, skipped case or native failure')
    expected = CASES[case]
    actual = re.findall(r'^--- PASS: (\w+) \([0-9.]+s\)$', log, re.M)
    require(sorted(actual) == sorted(expected), 'wrong or duplicate top-level test coverage')
    if case == 'read-controls':
        for name in expected:
            subs = (['worker', 'client', 'deletion', 'corruption', 'permanent_stale_absence_deadline',
                     'blocked_leader_deadline_and_cancel', 'leader_rejects_malformed_metadata']
                    if name.startswith(RESULT) else ['absent', 'old', 'object_absence',
                    'deleted_and_purged', 'malformed_manifest', 'blocked_leader_deadline_cancel'])
            for sub in subs:
                require(len(re.findall(r'^\s+--- PASS: '+re.escape(name+'/'+sub)+r' \(', log, re.M)) == 1,
                        'missing or duplicate subcase: '+name+'/'+sub)
        require(log.count('real domain admitted node=') == 6, 'actual domain peer admission missing')
        for prefix in ('$JS.API', '$JS.WFRESULT.API'):
            require(log.count('leader confirmations=1 route='+prefix+'.STREAM.MSG.GET.OBJ_WF_BLOB') == 2,
                    'worker/client real leader route not observed')
            require(log.count('leader requests=2 direct requests=0 route='+prefix+'.STREAM.MSG.GET.KV_WF_STATE') == 2,
                    'exact snapshot leader route not observed')
    else:
        nodes = 3*len(expected)
        require(log.count('native domain admitted node=') == nodes, 'domain admission missing')
        require(log.count('retirement fresh manifest cut:') == nodes and log.count('signal=killed') == nodes,
                'not all SIGKILL statuses observed')
        require(log.count('retirement restarted node=') == nodes, 'replacement processes missing')
        heal = re.findall(r'native domain healed node=\d pid=\d+ domain=WFRETIRE server_id=\w+ elapsed=([0-9.]+)s', log)
        require(len(heal) == nodes and all(float(value) < 30 for value in heal), 'whole-cut domain heal gate')
        epochs = re.findall(r'prior_epoch=(\d+) terminal_epoch=(\d+) records=(\d+)', log)
        if case in ('retirement-server-kill', 'retirement-weak-frame-expiry', 'legacy-retirement-weak-frame-expiry'):
            require(len(epochs) == 1 and int(epochs[0][1]) > int(epochs[0][0]), 'lease expiry successor epoch missing')
            outages = re.findall(r'all-server outage: held=([0-9.]+)s ttl=12s', log)
            require(len(outages) == 1 and float(outages[0]) > 12, 'outage did not exceed production lease TTL')
        else:
            require(not epochs, 'unexpected lease expiry profile')
        if case in ('retirement-weak-frame', 'retirement-weak-frame-expiry', 'legacy-retirement-weak-frame-expiry'):
            armed = re.findall(r'weak frame armed after domain heal: object=(step-result-[a-f0-9]{64})', log)
            confirmed = re.findall(r'weak frame confirmed: object=(step-result-[a-f0-9]{64}) generation=(\d+) drops=1 reads=(\d+) leader=1 direct=0 route=\$JS.WFRETIRE.API.STREAM.MSG.GET.OBJ_WF_BLOB', log)
            generations = re.findall(r'old_generation=(\d+) fresh_generation=(\d+)', log)
            require(len(armed) == len(confirmed) == 1 and armed[0] == confirmed[0][0]
                    and len(generations) == 1 and confirmed[0][1] == generations[0][1]
                    and int(generations[0][1]) > int(generations[0][0]) and int(confirmed[0][2]) >= 2,
                    'fresh frame weak absence and exact native domain confirmation missing')
        require(log.count('effects=3 terminals=2 shared_blob_retained=true') == len(expected) and log.count('manifest_drops=1') == len(expected),
                'strict retirement/reuse completion missing')
        if case == 'legacy-retirement-weak-frame-expiry':
            for stage in ('admitted', 'healed'):
                peers=re.findall(r'legacy domain '+stage+r' node=(\d) version=2\.11\.17 timer_backend=fallback', log)
                require(sorted(peers)==['0','1','2'], 'legacy peer versions/backend not verified: '+stage)
    return {'case': case, 'tests': expected, 'scope': 'Focused native row only; full matrix/24h not qualified.'}


def source_inventory(revision):
    require(not subprocess.check_output(['git', 'status', '--porcelain'], cwd=REPO), 'source checkout is dirty')
    names = subprocess.check_output(['git', 'ls-tree', '-r', '--name-only', revision], cwd=REPO, text=True).splitlines()
    selected = [n for n in names if n.endswith(('.go', '.py', '.yml')) or n in ('go.mod', 'go.sum') or n.startswith('sim/testdata/')]
    result = {}
    for name in selected:
        blob = subprocess.check_output(['git', 'cat-file', 'blob', revision+':'+name], cwd=REPO)
        require(hashlib.sha256(blob).hexdigest() == sha(REPO/name), 'source differs from Git: '+name)
        result[name] = sha(REPO/name)
    return {'revision': revision, 'files': result}


def closure(root):
    spec = importlib.util.spec_from_file_location('closed', REPO/'scripts/verify-tier2-closed-originals.py')
    closed = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(closed)
    limits = []
    for proc in Path('/proc').glob('[0-9]*'):
        try:
            require(not (proc/'exe').resolve().is_relative_to(root), 'live fixture executable')
            if int(proc.name) != os.getpid():
                args = (proc/'cmdline').read_bytes().split(b'\0')
                require(not any(str(root).encode() in arg for arg in args), 'live fixture process')
        except PermissionError:
            limits.append(str(proc))
        except (FileNotFoundError, ProcessLookupError):
            pass
    return {'visible_fds': closed.verify_no_open_originals(root), 'unobservable_processes': limits,
            'scope': 'Visible process/thread-FD observation, not exhaustive lifetime/container/mount closure.'}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--case', choices=CASES, required=True)
    parser.add_argument('--root', type=Path, required=True)
    args = parser.parse_args()
    root = args.root.absolute()
    require(not root.exists() and not root.is_symlink(), 'artifact root must be fresh')
    require(not root.is_relative_to(REPO), 'artifact root must be outside checkout')
    revision = subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=REPO, text=True).strip()
    before = source_inventory(revision)
    root.mkdir(parents=True)
    (root/'stores').mkdir()
    save = lambda name, value: (root/name).write_text(json.dumps(value, indent=2)+'\n')
    save('source-before.json', before)
    for name in before['files']:
        dest = root/'selected-source'/name
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(REPO/name, dest)
    env = dict(os.environ, GOMAXPROCS='2', GOMEMLIMIT='1GiB')
    legacy = args.case == 'legacy-retirement-weak-frame-expiry'
    if legacy:
        donor = Path(os.environ.get('WF_NATS_SERVER_BIN', ''))
        require(donor.is_file() and not donor.is_symlink(), 'legacy row requires a regular NATS2.11.17 binary')
        info = subprocess.check_output(['go','version','-m',str(donor)], text=True)
        require(re.search(r'\bgithub.com/nats-io/nats-server/v2\s+v2\.11\.17\b', info), 'legacy module identity mismatch')
        target = root/'inputs'/'nats-server'
        target.parent.mkdir()
        shutil.copyfile(donor, target)
        target.chmod(0o700)
        env['WF_NATS_SERVER_BIN'] = str(target)
        save('legacy-binary.json', {'donor': str(donor), 'retained': str(target), 'sha256': sha(target), 'build_info': info})
    for key in ('WF_RESULT_ABSENCE_ROOT', 'WF_SNAPSHOT_LEADER_ROOT', 'WF_CONTINUATION_RETIREMENT_PROCESS_ROOT'):
        env[key] = str(root/'stores')
    binary = root/'integration-race.test'
    build = ['go', 'test', '-race', '-c', '-o', str(binary), './integration']
    command = [str(binary), '-test.v', '-test.run=^('+'|'.join(CASES[args.case])+')$', '-test.count=1', '-test.timeout=3m']
    save('commands.json', {'build': build, 'run': command})
    with (root/'build.log').open('w') as log:
        subprocess.run(build, cwd=REPO, env=env, stdout=log, stderr=subprocess.STDOUT, check=True)
    info = subprocess.check_output(['go', 'version', '-m', str(binary)], text=True)
    require('-race=true' in info, 'race build missing')
    save('binary.json', {'sha256': sha(binary), 'build_info': info})
    started = time.monotonic()
    with (root/'native.log').open('w') as log:
        child = subprocess.Popen(command, cwd=REPO, env=env, stdout=log, stderr=subprocess.STDOUT)
        proc = Path('/proc', str(child.pid))
        actual_env = dict(v.split(b'=', 1) for v in (proc/'environ').read_bytes().split(b'\0') if b'=' in v)
        actual = {'pid': child.pid, 'exe': str((proc/'exe').resolve()), 'exe_sha256': sha(proc/'exe'),
                  'stat': (proc/'stat').read_text(), 'args': [os.fsdecode(v) for v in (proc/'cmdline').read_bytes().split(b'\0') if v],
                  'environment': {k: os.fsdecode(actual_env[k.encode()]) for k in ('GOMAXPROCS', 'GOMEMLIMIT', 'WF_RESULT_ABSENCE_ROOT', 'WF_SNAPSHOT_LEADER_ROOT', 'WF_CONTINUATION_RETIREMENT_PROCESS_ROOT') + (('WF_NATS_SERVER_BIN',) if legacy else ())}}
        require(actual['exe'] == str(binary) and actual['exe_sha256'] == sha(binary) and actual['args'] == command,
                'actual SDK identity mismatch')
        save('actual-sdk.json', actual)
        print('ACTUAL_SDK', child.pid, flush=True)
        servers = {}
        while child.poll() is None:
            for native in Path('/proc').glob('[0-9]*'):
                try:
                    exe = (native/'exe').resolve()
                    if exe.name != 'nats-server' or not exe.is_relative_to(root):
                        continue
                    stat = (native/'stat').read_text()
                    birth = stat[stat.rfind(')')+2:].split()[19]
                    key = native.name+':'+birth
                    if key in servers:
                        continue
                    servers[key] = {'pid': int(native.name), 'birth': birth, 'exe': str(exe),
                                    'exe_sha256': sha(native/'exe'), 'stat': stat,
                                    'args': [os.fsdecode(v) for v in (native/'cmdline').read_bytes().split(b'\0') if v],
                                    'build_info': subprocess.check_output(['go', 'version', '-m', str(native/'exe')], text=True)}
                    save('actual-servers.json', list(servers.values()))
                except (FileNotFoundError, ProcessLookupError, PermissionError):
                    pass
            time.sleep(.2)
        code = child.wait()
    save('execution.json', {'case': args.case, 'source': revision, 'exit_code': code,
                           'elapsed_seconds': time.monotonic()-started, 'finished_utc': datetime.now(timezone.utc).isoformat()})
    after = source_inventory(revision)
    save('source-after.json', after)
    require(before == after, 'source changed during native row')
    save('closure.json', closure(root))
    qualification = None
    error = None
    try:
        require(code == 0, 'native test failed')
        qualification = verify_log(args.case, (root/'native.log').read_text())
        if args.case != 'read-controls':
            require(len(servers) == 6*len(CASES[args.case]), 'all original/replacement native server incarnations must be observed')
            for row in servers.values():
                version = 'v2.11.17' if legacy else 'v2.15.0'
                require(sha(row['exe']) == row['exe_sha256'] and re.search(r'\bgithub.com/nats-io/nats-server/v2\s+'+re.escape(version)+r'\b', row['build_info']),
                        'observed native server executable changed or wrong version')
                if legacy:
                    require(row['exe_sha256']==sha(root/'inputs'/'nats-server'), 'legacy observed binary differs from retained input')
            qualification['observed_native_server_incarnations'] = len(servers)
    except ValueError as exc:
        error = str(exc)
    save('row-review.json', {'qualified': qualification, 'rejection': error})
    proof = fixture_archive.capture(root, root.with_suffix('.tar.gz'), root.with_name(root.name+'-proof'), compresslevel=1)
    print(json.dumps({'qualification': qualification, 'rejection': error, 'proof': proof}), flush=True)
    require(error is None, error)


if __name__ == '__main__':
    main()
