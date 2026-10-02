#!/usr/bin/env python3
"""Verify a complete consecutive-seed R5 row from original execution artifacts."""
import argparse
import json
from pathlib import Path
import re
import subprocess
import sys
import tempfile


def check_campaign(metadata, reports, row, count, duration, require_clock=False):
    if count not in (1, 20, 200) or duration not in ('35s', '10m'):
        raise ValueError('unsupported campaign size or duration')
    source = metadata.get('headSha', '')
    if not re.fullmatch(r'[0-9a-f]{40}', source):
        raise ValueError('missing exact source revision')
    if metadata.get('status') != 'completed' or metadata.get('conclusion') != 'success':
        raise ValueError('campaign is incomplete or failed')
    expected = {'seeds', *(f'journal ({seed})' for seed in range(1, count + 1))}
    jobs = metadata.get('jobs', [])
    names = [job.get('name') for job in jobs]
    if len(names) != len(expected) or set(names) != expected:
        raise ValueError('missing, duplicated or unexpected campaign jobs')
    if any(job.get('status') != 'completed' or job.get('conclusion') != 'success' for job in jobs):
        raise ValueError('a required seed job did not pass')
    if set(reports) != set(range(1, count + 1)) or any(type(seed) is not int for seed in reports):
        raise ValueError('missing or unexpected seed artifacts')
    seconds = 600 if duration == '10m' else 35
    for seed, report in reports.items():
        if report.get('seed') != seed or report.get('duration_seconds') != seconds:
            raise ValueError('artifact seed or duration mismatch')
        if report.get('shortened_smoke') is not (duration == '35s') or report.get('clears_full_tier3_release') is not False:
            raise ValueError('incorrect campaign release scope')
        if require_clock:
            if not row.startswith('server_clock_'):
                raise ValueError('clock admission requires a clock row')
            if report.get('common_timer_clock') != 'utc-quorum-v1':
                raise ValueError('missing common timer clock')
            if not report.get('clock_timer_cut_artifact_checks', {}).get('all_cuts_remove_source_before_duration'):
                raise ValueError('missing admitted timer cut proof')
            if report.get('common_timer_clock_artifact_checks', {}).get('physical_probes') != 5:
                raise ValueError('missing independent clock probes')
        if not report.get('checkpoint_audit_checks', {}).get('all_expected_checkpoints_present'):
            raise ValueError('missing completed cohort checkpoints')
    return dict(source=source, row=row, first_seed=1, last_seed=count, seeds=count,
                duration_seconds=seconds, shortened_smoke=duration == '35s',
                invocations=sum(report['invocations'] for report in reports.values()),
                journal_entries=sum(report['journal_entries'] for report in reports.values()),
                confirmed_faults=sum(report['confirmed_faults'] for report in reports.values()),
                scope='Complete requested seed range for one R5 row at the recorded source; not the full matrix or 24-hour release.',
                clears_full_tier3_release=False)


def verify_artifacts(root, row, count, duration, require_clock):
    reports = {}
    verifier = Path(__file__).with_name('check-tier3-journal-row.py')
    with tempfile.TemporaryDirectory() as tmp:
        for seed in range(1, count + 1):
            artifact = root / f'tier3-{row}-seed-{seed}'
            output = Path(tmp) / f'{seed}.json'
            args = [sys.executable, str(verifier), '--row', row, '--duration', duration,
                    '--root', str(artifact / 'tier3-mixed-journal'),
                    '--events', str(artifact / 'tier3-mixed-journal-events.jsonl'),
                    '--output', str(output), '--expected-seed', str(seed), '--require-checkpoint-audits']
            if require_clock:
                args += ['--require-common-timer-clock', '--require-clock-timer-cut']
            subprocess.run(args, check=True, stdout=subprocess.DEVNULL)
            if output.read_bytes() != (artifact / 'tier3-mixed-journal-result.json').read_bytes():
                raise ValueError(f'seed {seed}: uploaded report differs from current artifact verification')
            reports[seed] = json.loads(output.read_text())
    return reports


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--metadata', required=True, type=Path, help='gh run view JSON with jobs/headSha/status/conclusion')
    parser.add_argument('--artifacts', required=True, type=Path, help='root populated by gh run download')
    parser.add_argument('--row', required=True)
    parser.add_argument('--seeds', required=True, type=int, choices=(1, 20, 200))
    parser.add_argument('--duration', required=True, choices=('35s', '10m'))
    parser.add_argument('--require-admitted-clock', action='store_true')
    parser.add_argument('--output', required=True, type=Path)
    args = parser.parse_args()
    reports = verify_artifacts(args.artifacts, args.row, args.seeds, args.duration, args.require_admitted_clock)
    result = check_campaign(json.loads(args.metadata.read_text()), reports, args.row, args.seeds, args.duration, args.require_admitted_clock)
    args.output.write_text(json.dumps(result, indent=2) + '\n')
    print(json.dumps(result, indent=2))
