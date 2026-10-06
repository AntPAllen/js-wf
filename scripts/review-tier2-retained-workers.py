#!/usr/bin/env python3
"""Review a closed retained Tier2 pause or reply-isolation row; no store reopening."""
import argparse
import hashlib
import importlib.util
import json
from pathlib import Path
import subprocess

SCRIPTS = Path(__file__).resolve().parent
ROWS = {'pause': ('worker_pause', 'TestMixedMatrixWorkerPausedFortyFiveSeconds'),
        'isolation': ('worker_isolation', 'TestMixedMatrixWorkerReplyIsolationFortyFiveSeconds')}


def load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


clock = load('worker_review_clock', SCRIPTS/'tier3-worker-clock-evidence.py')
ns = clock.timestamp_ns


def require(value, message):
    if not value:
        raise ValueError(message)


def sha(path):
    with path.open('rb') as file:
        return hashlib.file_digest(file, 'sha256').hexdigest()


def read(path):
    return json.loads(path.read_text())


def review_faults(row, faults, workers, records):
    """Records come from complete retained files, never a running child."""
    require(row in ROWS and len(faults) == 10, 'require supported row and ten faults')
    first = ns(faults[0]['scheduled'])
    for index, fault in enumerate(faults):
        require(ns(fault['scheduled']) == first + index*60_000_000_000, 'fault schedule is not the original one-minute cadence')
        require(not fault.get('error') and type(fault['active_leases']) is int and type(fault['fencing_events']) is int and fault['active_leases'] > 0 and fault['fencing_events'] > 0,
                'fault lacks successful active ownership and fencing')
        require(fault['pid'] in workers and workers[fault['pid']]['worker_id'] == fault['worker'],
                'fault does not name the observed worker process')
        killed, resumed, healed = (ns(fault[k]) for k in ('killed', 'resumed', 'healed'))
        require(ns(fault['scheduled']) <= killed <= resumed <= healed, 'reversed fault interval')
        if row == 'pause':
            paused = ns(fault['paused'])
            require(killed <= paused and resumed - paused >= 45_000_000_000, 'short or reversed pause')
            leases = fault['paused_leases']
            require(len(leases) == fault['active_leases'] and all(
                lease['worker_id'] == fault['worker'] and lease['epoch'] > 0 and lease['revision'] > 0
                for lease in leases), 'paused leases do not match retained ownership')
            held = {(lease['key'], lease['epoch']) for lease in leases}
            matches = 0
            for sequence, record in enumerate(records(fault['worker'], 'fencing'), 1):
                event = record['event']
                require(record['pid'] == fault['pid'] and record['sequence'] == sequence and
                        event['Worker'] == fault['worker'], 'fencing process or sequence mismatch')
                if resumed <= ns(event['At']) <= healed and (event['Type']+'.'+event['ID'], event['Epoch']) in held:
                    matches += 1
        else:
            require(resumed - killed >= 45_000_000_000 and resumed < ns(fault['worker_ping_at']) <= healed,
                    'short isolation or missing post-resume PING')
            before, blocked, after = (fault[k] for k in ('proxy_before', 'proxy_blocked', 'proxy_healed'))
            require(blocked['responses_held'] and blocked['held_bytes'] > before['held_bytes'] and
                    blocked['client_to_server'] > before['client_to_server'], 'asymmetric transport fault unconfirmed')
            require(not after['responses_held'] and after['server_to_client'] > blocked['server_to_client'] and
                    all(x['buffer_overflows'] == 0 for x in (before, blocked, after)), 'invalid transport recovery')
            target = fault['isolation_target']
            delivery = target['delivery']
            require(target['token'] and delivery['Worker'] == fault['worker'] and
                    delivery['Stage'] == 'lease_acquired' and delivery['RunSequence'] > 0 and delivery['Delivery'] > 0,
                    'missing exact acquisition handoff')
            matches = 0
            for event in records(fault['worker'], 'dispatch'):
                if not killed <= ns(event['At']) <= healed:
                    continue
                if not all(event[k] == delivery[k] for k in ('Worker', 'Type', 'ID', 'RunSequence', 'Delivery')):
                    continue
                if 'workflow lease was lost' in event.get('Error', '') or 'journal compare-and-swap lost' in event.get('Error', ''):
                    matches += 1
        require(matches >= fault['fencing_events'], 'no matching fencing within recorded fault observation')
    return len(faults)


def review(root, repo):
    execution = read(root/'execution.json')
    row = execution['row']
    require(row in ROWS, 'review supports pause and isolation only')
    fault_row, test = ROWS[row]
    require(execution['status'] == 'passed' and execution['exit_code'] == 0 and
            execution['server_observer_errors'] == 0 and execution['test'] == test and
            execution['duration'] == '10m' and execution['sustained_ten_minutes'] and not execution['race'],
            'require successful original normal ten-minute execution')
    require(not Path('/proc/'+str(execution['pid'])).exists(), 'SDK remains live')
    require(sha(root/'integration.test') == execution['sha256'], 'SDK executable changed')
    revision = execution['source']
    require('vcs.revision='+revision in execution['build_info'] and 'vcs.modified=false' in execution['build_info'],
            'SDK lacks clean original source identity')
    git = lambda name: subprocess.check_output(['git', 'show', revision+':'+name], cwd=repo)
    before = read(root/'source-before.json')
    require(before == read(root/'source-after.json') and before['revision'] == revision, 'source inventory changed')
    for name, digest in before['files'].items():
        require(sha(root/'source'/name) == digest == hashlib.sha256(git(name)).hexdigest(), 'source bytes changed: '+name)
    external = read(root/'external-source-before.json')
    require(external == read(root/'external-source-after.json'), 'external inputs changed')
    paths = read(root/'external-captured-paths.json')
    require(set(paths) == set(external), 'external capture incomplete')
    for name, digest in external.items():
        require(sha(root/paths[name]) == digest, 'external captured bytes changed')
    for name, captured in [('run-tier2-retained-row.py', 'executed-producer.py'),
                           ('matrix_process_observer.py', 'matrix_process_observer.py'),
                           ('matrix_worker_observer.py', 'matrix_worker_observer.py'),
                           ('tier2_retained_profiles.py', 'tier2_retained_profiles.py'),
                           ('check-matrix-result.py', 'executed-checker.py')]:
        require((root/captured).read_bytes() == git('scripts/'+name), 'executed helper differs from Git')
    for name in ('check-matrix-result.py', 'check-matrix-campaign.py', 'tier3-worker-clock-evidence.py'):
        require((SCRIPTS/name).read_bytes() == git('scripts/'+name), 'review helper differs from original source')
    servers = read(root/'observed-servers.json')
    require(len(servers) == 3 and {x['node'] for x in servers} == {0, 1, 2}, 'missing actual server observations')
    for server in servers:
        require(not Path('/proc/'+str(server['pid'])).exists() and
                sha(root/server['captured']) == server['actual_executable_sha256'] and
                Path(server['store']).is_relative_to(root/'originals'), 'server identity, closure or store scope invalid')
    workers = read(root/'observed-workers.json')
    by_pid = {x['pid']: x for x in workers}
    require(len(workers) == len(by_pid) == 3, 'missing actual worker observations')
    for worker in workers:
        require(not Path('/proc/'+str(worker['pid'])).exists() and
                sha(root/worker['captured']) == worker['actual_executable_sha256'] == execution['sha256'] and
                worker['argv'][1:] == ['-test.run=^TestMixedMatrixWorkerProcessChild$'] and
                Path(worker['process_root']) == root/'originals', 'worker identity or closure invalid')
    acceptance = read(root/'acceptance.json')
    require(acceptance['exit_code'] == acceptance['native_exit_code'] == 0, 'native duration checker failed')
    guard = load('retained_worker_duration', SCRIPTS/'check-matrix-result.py')
    guard.check([json.loads(s) for s in (root/'converted-events.jsonl').read_text().splitlines()], test, '10m')
    campaign = load('retained_worker_campaign', SCRIPTS/'check-matrix-campaign.py')
    environment = read(root/'commands.json')['environment']
    require((environment['GOMAXPROCS'], environment['GOMEMLIMIT'], environment['WF_MATRIX_DURATION']) == ('2', '2GiB', '10m'), 'wrong original execution profile')
    faults = read(root/f'matrix-{row}-{environment["FAULT_SEED"]}-faults.json')
    seed = faults['seed']
    require(type(seed) is int and str(seed) == environment['FAULT_SEED'], 'fault seed does not match executed environment')
    log = (root/'native.log').read_text()
    seed_review = campaign.check_seed(log, fault_row, seed, test)
    require(campaign.seconds(faults['duration']) == 600 and seed_review['faults'] == 10,
            'fault duration or counts disagree')
    require(f'worker faults=10 active_worker_faults=10 row={fault_row}' in log, 'missing ten active faults')
    prefix = f'matrix-{row}-{seed}-'
    def records(worker, kind):
        data = (root/(prefix+worker+'-'+kind+'.jsonl')).read_bytes()
        require(data.endswith(b'\n'), 'incomplete closed worker records')
        return [json.loads(line) for line in data.splitlines()]
    review_faults(row, faults['faults'], by_pid, records)
    count = seed_review['invocations']
    return dict(source=revision, row=row, seed=seed, actual_sdk_pid=execution['pid'],
                actual_sdk_sha256=execution['sha256'], source_files=len(before['files']),
                external_files=len(external), workers_verified=3, servers_verified=3,
                exact_active_faults_verified=10, seed_review=seed_review,
                expected_original_report=dict(Invocations=count, Journals=count,
                                              Entries=seed_review['journal_entries'], Terminal=count),
                scope='Original normal ten-minute worker row; copied-store and full-matrix qualification remain separate')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    require(args.root.is_absolute(), 'require absolute closed fixture root')
    result = review(args.root.resolve(), SCRIPTS.parent)
    args.output.write_text(json.dumps(result, indent=2)+'\n')
    print(json.dumps(result, indent=2))


if __name__ == '__main__':
    main()
