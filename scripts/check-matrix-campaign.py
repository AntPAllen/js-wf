#!/usr/bin/env python3
"""Verify one sustained Tier 2 row from terminal Actions metadata and full logs.

This validates the campaign's recorded checks; it does not independently audit
raw JetStream state or establish the other matrix rows or later revisions.
"""
import argparse
import hashlib
import json
from pathlib import Path
import re
import zipfile

TYPES = ('matrixshort', 'matrixtimer', 'matrixsignal', 'matrixfanout',
         'matrixchild', 'matrixgrandchild')


def seconds(value):
    units = {'h': 3600, 'm': 60, 's': 1, 'ms': .001, 'us': .000001,
             'µs': .000001, 'ns': .000000001}
    matches = list(re.finditer(r'(\d+(?:\.\d+)?)(ns|us|µs|ms|h|m|s)', value))
    if not matches or ''.join(m.group() for m in matches) != value:
        raise ValueError(f'invalid duration {value!r}')
    return sum(float(m[1]) * units[m[2]] for m in matches)


def one(pattern, log):
    matches = re.findall(pattern, log)
    if len(matches) != 1:
        raise ValueError(f'expected exactly one {pattern!r}, found {len(matches)}')
    return matches[0]


def check_seed(log, row, seed, test):
    result = one(r'MATRIX_RESULT row=(\w+) seed=(\d+) duration=(\S+) '
                 r'release_duration=(\w+) batches=(\d+) invocations=(\d+) '
                 r'faults=(\d+) terminal_p99=(\S+)', log)
    found_row, found_seed, duration, release, batches, invocations, faults, latency = result
    if (found_row, int(found_seed), seconds(duration), release) != (row, seed, 600, 'true'):
        raise ValueError(f'seed {seed}: incorrect row, seed or release duration')
    batches, invocations, faults = int(batches), int(invocations), int(faults)
    if batches <= 0 or invocations != batches * 28 or faults <= 0 or seconds(latency) >= 30:
        raise ValueError(f'seed {seed}: invalid workload, faults or aggregate p99')
    retained = one(r'MATRIX_RETAINED row=(\w+) report=\{Invocations:(\d+) '
                   r'Journals:(\d+) Entries:(\d+) Terminal:(\d+)\} expected_invocations=(\d+)', log)
    if retained[0] != row or any(int(retained[i]) != invocations for i in (1, 2, 4, 5)) or int(retained[3]) <= invocations:
        raise ValueError(f'seed {seed}: retained audit counts disagree')
    elapsed = float(one(r'--- PASS: ' + re.escape(test) + r' \(([\d.]+)s\)', log))
    if elapsed < 600 or re.search(r'--- FAIL:|##\[error\]', log):
        raise ValueError(f'seed {seed}: failed or shortened test')
    active_faults = None
    if row == 'worker_kill':
        total, active, fault_row = one(r'worker faults=(\d+) active_worker_faults=(\d+) row=(\w+)', log)
        active_faults = int(active)
        if int(total) != faults or not 0 < active_faults <= faults or fault_row != row:
            raise ValueError(f'seed {seed}: no confirmed active-worker kill or inconsistent fault counts')
    cells, progress = {}, {}
    for typ, each in zip(TYPES, (4, 3, 2, 1, 6, 12)):
        count, p99 = one(r'MATRIX_CELL type=' + typ + r' invocations=(\d+) terminal_p99=(\S+)', log)
        events, pp99, maximum, above = one(r'MATRIX_PROGRESS type=' + typ + r' enabled_events=(\d+) '
                                         r'progress_p99=(\S+) progress_max=(\S+) above_30s=(\d+)', log)
        if int(count) != batches * each or int(events) <= 0 or seconds(p99) >= 30 or seconds(pp99) >= 30:
            raise ValueError(f'seed {seed}: {typ} missing workload or over-budget p99')
        cells[typ] = {'invocations': int(count), 'terminal_p99_seconds': seconds(p99)}
        progress[typ] = {'events': int(events), 'p99_seconds': seconds(pp99),
                         'max_seconds': seconds(maximum), 'above_30s': int(above)}
    if sum(cell['invocations'] for cell in cells.values()) != invocations:
        raise ValueError(f'seed {seed}: workload counts disagree')
    return {'seed': seed, 'batches': batches, 'invocations': invocations, 'faults': faults,
            'journal_entries': int(retained[3]), 'active_worker_faults': active_faults, 'test_seconds': elapsed,
            'terminal_p99_seconds': seconds(latency), 'cells': cells, 'progress': progress}


def check_campaign(metadata, logs, row, test, count):
    if count not in (20, 200):
        raise ValueError('require twenty or 200 consecutive seeds')
    if metadata.get('status') != 'completed' or metadata.get('conclusion') != 'success':
        raise ValueError('campaign is not terminal and successful')
    if not re.fullmatch(r'[0-9a-f]{40}', metadata.get('headSha', '')):
        raise ValueError('missing source revision')
    jobs = metadata['jobs']
    expected = {'seeds'} | {f'leader ({seed})' for seed in range(1, count + 1)}
    if len(jobs) != len(expected) or {job['name'] for job in jobs} != expected:
        raise ValueError('missing, duplicate or unexpected seed jobs')
    if any(job['status'] != 'completed' or job['conclusion'] != 'success' for job in jobs):
        raise ValueError('unfinished or unsuccessful job')
    if set(logs) != set(range(1, count + 1)):
        raise ValueError('missing or unexpected seed logs')
    seeds = [check_seed(logs[seed], row, seed, test) for seed in range(1, count + 1)]
    return {'revision': metadata['headSha'], 'row': row, 'test': test,
            'consecutive_seeds': count, 'duration_seconds_per_seed': 600,
            'scope': 'one fault row at the recorded revision',
            'invocations': sum(s['invocations'] for s in seeds),
            'journal_entries': sum(s['journal_entries'] for s in seeds),
            'faults': sum(s['faults'] for s in seeds),
            'active_worker_faults': sum(s['active_worker_faults'] or 0 for s in seeds),
            'worst_progress_seconds': max(p['max_seconds'] for s in seeds for p in s['progress'].values()),
            'progress_events_above_30s': sum(p['above_30s'] for s in seeds for p in s['progress'].values()),
            'worst_terminal_p99_seconds': max(s['terminal_p99_seconds'] for s in seeds),
            'worst_cell_terminal_p99_seconds': max(c['terminal_p99_seconds'] for s in seeds for c in s['cells'].values()),
            'seeds': seeds}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--jobs', required=True, type=Path)
    parser.add_argument('--logs', required=True, type=Path)
    parser.add_argument('--row', required=True)
    parser.add_argument('--test', required=True)
    parser.add_argument('--seeds', required=True, type=int, choices=(20, 200))
    parser.add_argument('--output', required=True, type=Path)
    args = parser.parse_args()
    logs = {}
    with zipfile.ZipFile(args.logs) as archive:
        for name in archive.namelist():
            match = re.fullmatch(r'\d+_leader \((\d+)\)\.txt', name)
            if match:
                seed = int(match[1])
                if seed in logs:
                    raise ValueError(f'duplicate log for seed {seed}')
                logs[seed] = archive.read(name).decode('utf-8')
    report = check_campaign(json.loads(args.jobs.read_text()), logs, args.row, args.test, args.seeds)
    report['input_sha256'] = {name: hashlib.sha256(path.read_bytes()).hexdigest()
                              for name, path in (('jobs', args.jobs), ('logs_zip', args.logs))}
    args.output.write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps({key: value for key, value in report.items() if key != 'seeds'}, indent=2))


if __name__ == '__main__':
    main()
