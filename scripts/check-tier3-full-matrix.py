#!/usr/bin/env python3
"""Verify complete R5 row campaigns; sustained coverage is not a 24-hour soak."""
import argparse
import hashlib
import importlib.util
import json
from pathlib import Path
import re
import subprocess
import sys
import tempfile
import zipfile


def load(name, file):
    spec = importlib.util.spec_from_file_location(name, Path(__file__).with_name(file))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


planner = load('r5_planner', 'tier3-matrix-campaign.py')
single = load('r5_single_campaign', 'check-tier3-campaign.py')


def job_name(job):
    return f"journal ({job['row']}, {job['artifact_seed']})"


def check(metadata, logs, reports, count, duration, require_clock=False,
          require_upgrade_start_gap=False, expected_upgrade_shutdown=None):
    jobs = planner.campaign('all', count)
    expected = {job_name(j) for j in jobs}
    source = metadata.get('headSha', '')
    if not re.fullmatch(r'[0-9a-f]{40}', source):
        raise ValueError('missing exact source revision')
    if metadata.get('status') != 'completed' or metadata.get('conclusion') != 'success':
        raise ValueError('whole campaign is not terminal and successful')
    actual = metadata.get('jobs', [])
    if len(actual) != len(expected)+1 or {j.get('name') for j in actual} != expected | {'seeds'}:
        raise ValueError('missing, duplicate or unexpected matrix jobs')
    if any(j.get('status') != 'completed' or j.get('conclusion') != 'success' for j in actual):
        raise ValueError('unfinished, skipped or failed job')
    if set(logs) != expected or set(reports) != set(planner.ROWS):
        raise ValueError('missing or unexpected job logs or row artifacts')
    if duration not in ('35s', '10m'):
        raise ValueError('unsupported hosted campaign duration')
    if expected_upgrade_shutdown not in (None, 'sigkill', 'ldm'):
        raise ValueError('unsupported upgrade shutdown profile')
    for job in jobs:
        log = logs[job_name(job)]
        if source not in log:
            raise ValueError('checkout revision missing from job log')
        headers = re.findall(r'TIER3_CAMPAIGN row=(\w+) seed=(\d+) duration=(\S+)', log)
        if headers != [(job['row'], str(seed), duration) for seed in range(job['first'], job['last']+1)]:
            raise ValueError('missing, duplicate or incorrect actual seed executions')
    rows = {}
    # Reuse the single-row proof checks after the real matrix jobs are checked.
    single_metadata = dict(metadata, jobs=[dict(name=name, status='completed', conclusion='success')
        for name in ['seeds', *(f'journal ({seed})' for seed in range(1, count+1))]])
    for row in planner.ROWS:
        rows[row] = single.check_campaign(single_metadata, reports[row], row, count, duration,
                                         require_clock and row.startswith('server_clock_'))
        if row == 'rolling_upgrade':
            for report in reports[row].values():
                if (expected_upgrade_shutdown is not None
                        and report.get('upgrade_shutdown_mode') != expected_upgrade_shutdown):
                    raise ValueError('required upgrade shutdown profile missing or different')
                if require_upgrade_start_gap:
                    progress = report.get('start_scan_progress_checks', {})
                    if (report.get('upgrade_start_gap') != 'cohort-short-v1'
                            or type(progress.get('gaps_with_progress')) is not int
                            or progress['gaps_with_progress'] != report['confirmed_faults']
                            or progress['gaps_with_progress'] <= 0):
                        raise ValueError('required forced Start-gap progress proof missing')
    return dict(source=source, rows=rows, fault_variants=len(rows), seeds_per_row=count,
                executions=len(rows)*count, duration_seconds=600 if duration == '10m' else 35,
                invocations=sum(r['invocations'] for r in rows.values()),
                confirmed_faults=sum(r['confirmed_faults'] for r in rows.values()),
                requires_upgrade_start_gap=require_upgrade_start_gap,
                expected_upgrade_shutdown=expected_upgrade_shutdown,
                scope='Complete requested R5 fault-row coverage; not the 24-hour full-matrix soak.',
                clears_full_tier3_release=False)


def locate_seed_events(artifact, seed):
    matches = [p for p in artifact.rglob('tier3-mixed-journal-events.jsonl')
               if p.parent.name == f'seed-{seed}']
    if len(matches) != 1:
        raise ValueError('missing or duplicate per-seed raw events')
    return matches[0]


def verify_seed(events, output, row, seed, duration, require_clock,
                require_upgrade_start_gap=False, expected_upgrade_shutdown=None):
    args = [sys.executable, str(Path(__file__).with_name('check-tier3-journal-row.py')),
            '--row', row, '--root', str(events.parent/'tier3-mixed-journal'),
            '--events', str(events), '--duration', duration, '--expected-seed', str(seed),
            '--output', str(output), '--require-checkpoint-audits']
    if require_clock and row.startswith('server_clock_'):
        args += ['--require-common-timer-clock', '--require-clock-timer-cut']
    if row == 'rolling_upgrade':
        if require_upgrade_start_gap:
            args += ['--require-upgrade-start-gap', '--require-start-scan-progress']
        if expected_upgrade_shutdown is not None:
            args += ['--expected-upgrade-shutdown', expected_upgrade_shutdown]
    subprocess.run(args, check=True, stdout=subprocess.DEVNULL)
    if output.read_bytes() != (events.parent/'tier3-mixed-journal-result.json').read_bytes():
        raise ValueError('uploaded report differs from raw artifact verification')
    report = json.loads(output.read_text())
    for script, name in [('explain-tier3-events.py', 'event-explanations.json'),
                         ('review-tier3-fencing.py', 'fencing-timeline-review.json')]:
        subprocess.run([sys.executable, str(Path(__file__).with_name(script)),
                        '--root', str(events.parent/'tier3-mixed-journal'),
                        '--output', str(output)], check=True, stdout=subprocess.DEVNULL)
        if output.read_bytes() != (events.parent/'tier3-mixed-journal'/name).read_bytes():
            raise ValueError('uploaded event explanation/fencing review disagrees with raw artifacts')
    return report


def verify_artifacts(root, count, duration, require_clock,
                     require_upgrade_start_gap=False, expected_upgrade_shutdown=None):
    reports = {row: {} for row in planner.ROWS}
    with tempfile.TemporaryDirectory() as tmp:
        for job in planner.campaign('all', count):
            artifact = root / f"tier3-{job['row']}-seed-{job['artifact_seed']}"
            for seed in range(job['first'], job['last']+1):
                events = locate_seed_events(artifact, seed)
                reports[job['row']][seed] = verify_seed(events, Path(tmp)/'result.json',
                    job['row'], seed, duration, require_clock,
                    require_upgrade_start_gap, expected_upgrade_shutdown)
    return reports


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--metadata', required=True, type=Path)
    parser.add_argument('--logs', required=True, type=Path)
    parser.add_argument('--artifacts', required=True, type=Path)
    parser.add_argument('--seeds', required=True, type=int, choices=(1, 20, 200))
    parser.add_argument('--duration', required=True, choices=('35s', '10m'))
    parser.add_argument('--require-admitted-clock', action='store_true')
    parser.add_argument('--require-upgrade-start-gap', action='store_true')
    parser.add_argument('--expected-upgrade-shutdown', choices=('sigkill', 'ldm'))
    parser.add_argument('--output', required=True, type=Path)
    args = parser.parse_args()
    logs = {}
    with zipfile.ZipFile(args.logs) as archive:
        for name in archive.namelist():
            match = re.fullmatch(r'\d+_(journal \(\w+, [\d-]+\))\.txt', name)
            if match:
                if match[1] in logs: raise ValueError('duplicate job log')
                logs[match[1]] = archive.read(name).decode()
    reports = verify_artifacts(args.artifacts, args.seeds, args.duration, args.require_admitted_clock,
                               args.require_upgrade_start_gap, args.expected_upgrade_shutdown)
    result = check(json.loads(args.metadata.read_text()), logs, reports, args.seeds, args.duration,
                   args.require_admitted_clock, args.require_upgrade_start_gap, args.expected_upgrade_shutdown)
    result['input_sha256'] = {label: hashlib.sha256(path.read_bytes()).hexdigest() for label, path in [('metadata', args.metadata), ('logs', args.logs)]}
    args.output.write_text(json.dumps(result, indent=2)+'\n')
