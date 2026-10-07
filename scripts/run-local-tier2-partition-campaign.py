#!/usr/bin/env python3
"""Run the original partition row for seeds 1..200 on one local VM.

This records native execution coverage only. Independent raw fault, latency,
history, source and store review is required before qualifying the full row.
"""
import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import signal
import subprocess
import time

from retained_input_cache import digest


SEEDS = tuple(range(1, 201))
TEST = 'TestMixedMatrixServerPartitionEveryThirtySeconds'


def coverage(records):
    seeds = [r.get('seed') for r in records]
    if seeds != list(SEEDS[:len(records)]) or len(records) > len(SEEDS):
        raise ValueError('missing, duplicate or reordered original seed coverage')
    return len(records) == 200 and all(r.get('exit_code') == 0 and r.get('profile_verified', True) is True for r in records)



def verify_seed_profile(evidence, profile, expected_candidate=None):
    acceptance = json.loads((evidence/'acceptance.json').read_text())
    if acceptance['server_profile'] != profile:
        raise ValueError('completed seed differs from campaign server profile')
    if profile == 'default':
        return None
    if profile != 'experimental-component-candidate':
        raise ValueError('unsupported campaign server profile')
    candidate = json.loads((evidence/'partition-server-input.json').read_text())
    peers = json.loads((evidence/'partition-server-verification.json').read_text())
    if peers['passed'] is not True or peers['expected_sha256'] != candidate['sha256'] or peers['observed_peers'] != 3:
        raise ValueError('completed seed lacks three exact candidate server identities')
    if expected_candidate != candidate['sha256'] or digest(evidence/'partition-server.bin') != expected_candidate:
        raise ValueError('candidate executable changed between original seeds')
    return expected_candidate


def now():
    return datetime.now(timezone.utc).isoformat()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, required=True)
    parser.add_argument('--minimum-free-bytes', type=int, default=5 * 1024**3,
                        help='Wait between seeds if free space is below this reserve; native deadlines stay unchanged')
    parser.add_argument('--partition-server', type=Path,
                        help='Exact source-bound experimental component executable for all200 seeds')
    parser.add_argument('--partition-server-proof', type=Path,
                        help='Committed repository-relative component evidence for the candidate')
    args = parser.parse_args()
    if (args.partition_server is None) != (args.partition_server_proof is None):
        parser.error('partition server and proof must be supplied together')
    if args.partition_server is not None:
        if not args.partition_server.is_absolute() or args.partition_server.is_symlink() or not args.partition_server.is_file():
            parser.error('partition server must be an absolute regular executable')
        if args.partition_server_proof.is_absolute() or '..' in args.partition_server_proof.parts:
            parser.error('partition proof must be a repository-relative directory')
    repo = Path(__file__).resolve().parents[1]
    root = args.root
    if not root.is_absolute() or root.exists() or root.resolve().is_relative_to(repo) or args.minimum_free_bytes < 1024**3:
        parser.error('require a fresh absolute external root and at least 1 GiB reserve')
    assert not subprocess.check_output(['git', 'status', '--porcelain'], cwd=repo)
    revision = subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=repo, text=True).strip()
    source = Path(__file__).read_bytes()
    assert source == subprocess.check_output(['git', 'show', revision + ':scripts/' + Path(__file__).name], cwd=repo)
    root.mkdir(mode=0o700)
    (root/'executed-campaign.py').write_bytes(source)
    cache = root/'input-cache'
    prepared = root/'prepared'
    producer = repo/'scripts/run-tier2-retained-row.py'
    environment = {k:v for k,v in os.environ.items() if not k.startswith(('WF_', 'MATRIX_', 'TIER3_MATRIX_'))}
    environment.update(GOMAXPROCS='2', GOMEMLIMIT='2GiB')
    records = []
    server_profile = 'experimental-component-candidate' if args.partition_server is not None else 'default'
    state = dict(schema='js-wf-local-tier2-partition-campaign-v1', source=revision,
                 producer_pid=os.getpid(), producer_start_ticks=Path('/proc/self/stat').read_text().rsplit(')', 1)[1].split()[19],
                 started_utc=now(), status='preparing', row='partition', test=TEST,
                 seeds=list(SEEDS), duration='10m', sdk_timeout='18m', race=False,
                 native_coverage_complete=False, qualifies_full_row=False, records=records,
                 server_profile=server_profile, partition_server=str(args.partition_server) if args.partition_server else None,
                 partition_server_proof=str(args.partition_server_proof) if args.partition_server_proof else None,
                 scope='Original local native 200-seed coverage at the recorded server profile; independent qualification remains required. Experimental candidate coverage does not qualify the default production dependency. No provider job/artifact identities are invented.')

    def save():
        pending = root/'campaign.json.new'
        pending.write_text(json.dumps(state, indent=2)+'\n')
        pending.replace(root/'campaign.json')

    def execute(evidence, seed, preparation=False):
        command = ['python3', str(producer), '--root', str(evidence), '--row', 'partition',
                   '--seed', str(seed), '--duration', '10m', '--input-cache', str(cache)]
        if args.partition_server is not None:
            command += ['--partition-server', str(args.partition_server), '--partition-server-proof', str(args.partition_server_proof)]
        command += ['--prepare-only'] if preparation else ['--prepared-inputs', str(prepared)]
        with (root/('prepare.log' if preparation else f'seed-{seed:03d}-producer.log')).open('w') as log:
            process = subprocess.Popen(command, cwd=repo, env=environment, stdout=log, stderr=subprocess.STDOUT)
            state.update(status='preparing' if preparation else 'running', current_seed=None if preparation else seed,
                         current_producer_pid=process.pid, current_command=command)
            save()
            # The SDK itself retains its original 18-minute timeout. There is
            # no observation timeout that restarts or replaces a native handle.
            try:
                return process.wait(timeout=25 * 60)
            except subprocess.TimeoutExpired:
                # Preserve the original hosted per-seed job envelope as well
                # as the SDK timeout. Never restart an expired seed.
                execution = evidence/'execution.json'
                if execution.is_file():
                    native = json.loads(execution.read_text())
                    try:
                        stat = Path(f'/proc/{native["pid"]}/stat').read_text()
                        if stat.rsplit(')', 1)[1].split()[19] == native['start_ticks']:
                            os.killpg(native['pid'], signal.SIGTERM)
                    except (FileNotFoundError, ProcessLookupError):
                        pass
                process.terminate()
                try:
                    process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()
                (root/f'seed-{seed:03d}-job-timeout.json').write_text(json.dumps(
                    dict(timeout_seconds=1500, seed=seed, preparation=preparation,
                         scope='Original 25-minute job envelope expired; partial evidence retained, no retry.'), indent=2)+'\n')
                return 124

    save()
    code = execute(prepared, 1, preparation=True)
    if code:
        state.update(status='preparation_failed', exit_code=code, finished_utc=now())
        save()
        return code
    if args.partition_server is not None:
        state['candidate_sha256'] = json.loads((prepared/'partition-server-input.json').read_text())['sha256']
        save()
    for seed in SEEDS:
        while True:
            fs = os.statvfs(root)
            free = fs.f_bavail * fs.f_frsize
            if free >= args.minimum_free_bytes:
                break
            state.update(status='waiting_for_disk_between_seeds', next_seed=seed, available_bytes=free)
            save()
            time.sleep(30)
        evidence = root/f'seed-{seed:03d}'
        started = now()
        code = execute(evidence, seed)
        record = dict(seed=seed, root=evidence.name, started_utc=started, finished_utc=now(), exit_code=code)
        for name in ('execution.json', 'commands.json', 'acceptance.json', 'source-before.json',
                     'source-after.json', 'external-source-before.json', 'external-source-after.json',
                     'partition-server-input.json', 'partition-server-verification.json'):
            if (evidence/name).is_file():
                record.setdefault('record_sha256', {})[name] = digest(evidence/name)
        record['server_profile'] = server_profile
        if code == 0:
            try:
                candidate = verify_seed_profile(evidence, server_profile, state.get('candidate_sha256'))
                record['profile_verified'] = True
                if candidate is not None:
                    record['candidate_sha256'] = candidate
            except (KeyError, ValueError, TypeError, OSError) as error:
                record['profile_verified'] = False
                record['profile_verification_error'] = repr(error)
                records.append(record)
                state.update(status='profile_verification_failed', exit_code=1, native_coverage_complete=False,
                             finished_utc=now())
                save()
                return 1
        records.append(record)
        state['native_coverage_complete'] = coverage(records)
        save()
        print('NATIVE_SEED_FINISHED', seed, code, flush=True)
        if code:
            state.update(status='failed', exit_code=code, finished_utc=now())
            save()
            return code
    state.update(status='native_execution_completed', exit_code=0, finished_utc=now())
    save()
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
