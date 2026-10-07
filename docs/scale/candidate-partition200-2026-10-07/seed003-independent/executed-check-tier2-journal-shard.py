#!/usr/bin/env python3
"""Review a complete Tier2 leader or all-server-kill shard using raw faults, latency and history.

Only the requested consecutive seeds of the selected row qualify. The parent campaign,
200-seed row, full matrix and 24h soak are never promoted by this command.
"""
import argparse
from datetime import datetime
import hashlib
import importlib.util
import json
from pathlib import Path
import re
import subprocess
import tempfile

REPO = Path(__file__).resolve().parents[1]
ROWS = {
    'journal': ('journal_leader', 'TestMixedMatrixJournalLeaderEveryThirtySeconds'),
    'consumer': ('consumer_leader', 'TestMixedMatrixConsumerLeaderEveryThirtySeconds'),
    'cluster': ('all_servers', 'TestMixedMatrixAllServersKilledEveryThirtySeconds'),
    'partition': ('server_partition', 'TestMixedMatrixServerPartitionEveryThirtySeconds'),
}

def row_contract(row):
    if row not in ROWS:
        raise ValueError('unsupported Tier2 row')
    return ROWS[row]

def timestamp_ns(value):
    # Independent RFC3339Nano evidence parsing; no R5 runtime helper dependency.
    if not isinstance(value, str) or not re.fullmatch(
            r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,9})?(?:Z|[+-]\d{2}:\d{2})', value):
        raise ValueError('invalid evidence timestamp')
    dt = datetime.fromisoformat(value.replace('Z', '+00:00'))
    fraction = re.search(r'\.(\d+)', value)
    return int(dt.replace(microsecond=0).timestamp()) * 1_000_000_000 + int(
        (fraction[1] if fraction else '').ljust(9, '0'))


def read(path):
    return json.loads(path.read_text())

def check_fault_identity(fault, row):
    if type(fault.get('node')) is not int:
        raise ValueError('fault node must be an integer')
    if row == 'cluster':
        nodes = fault.get('nodes')
        if (fault['node'] != -1 or not isinstance(nodes, list)
                or any(type(node) is not int for node in nodes) or nodes != [0, 1, 2]):
            raise ValueError('all-server fault must record every node before restart')
    elif not 0 <= fault['node'] < 3:
        raise ValueError('leader fault lacks a valid node')
    if row == 'partition':
        routes = fault.get('partition_routes')
        if (fault['node'] != 2 or not isinstance(routes, list) or len(routes) != 3
                or any(type(v) is not int for v in routes) or routes != [4, 4, 0]
                or type(fault.get('majority_sequence')) is not int or fault['majority_sequence'] <= 0):
            raise ValueError('partition lacks exact minority isolation and acknowledged majority write')
    if row == 'consumer':
        consumer = fault.get('consumer', '')
        if (not isinstance(consumer, str) or not re.fullmatch(r'WF_P_[0-9]{2}', consumer)
                or not 0 <= int(consumer[-2:]) < 64
                or any(type(fault.get(k, 0)) is not int or fault.get(k, 0) < 0
                       for k in ('pending', 'ack_pending'))):
            raise ValueError('consumer fault lacks valid durable identity or pending counts')

def module(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    loaded = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(loaded)
    return loaded

shared = module('tier2_shard_inventory', Path(__file__).with_name('check-tier3-matrix-shard.py'))

def bind(run, job, artifact, log, first, last, row='journal', job_layout='shard'):
    row_contract(row)
    source = run.get('head_sha', '')
    if (type(first) is not int or type(last) is not int or not 1 <= first <= last <= 200
            or not isinstance(source, str) or not re.fullmatch('[0-9a-f]{40}', source)):
        raise ValueError('invalid seed range or exact source')
    if job_layout not in ('shard', 'single') or job_layout == 'single' and first != last:
        raise ValueError('single-job layout requires exactly one seed')
    span = str(first) if first == last else f'{first}-{last}'
    expected_name = f'leader ({first})' if job_layout == 'single' else f'leader ({row}, {span})'
    if (type(run.get('id')) is not int or type(job.get('id')) is not int
            or job.get('run_id') != run['id'] or job.get('head_sha') != source
            or job.get('name') != expected_name
            or (job.get('status'), job.get('conclusion')) != ('completed', 'success')
            or source not in log):
        raise ValueError('job identity, source, checkout or terminal success differs')
    origin = artifact.get('workflow_run', {})
    if (type(artifact.get('id')) is not int or artifact.get('expired') is not False
            or artifact.get('name') != f'matrix-{row}-{span}-10m'
            or origin.get('id') != run['id'] or origin.get('head_sha') != source):
        raise ValueError('artifact identity, expiry, source or run differs')
    headers = re.findall(r'Sustained matrix row=(\w+) seed=(\d+) duration=(\S+)', log)
    if headers != [(row, str(seed), '10m') for seed in range(first,last+1)]:
        raise ValueError('missing, duplicate or substituted actual ten-minute seed execution')
    return source


def review(run, job, artifact, job_log, artifact_root, first, last, temporary_root=None,
           model_root=None, model_binary_out=None, row='journal', job_layout='shard'):
    revision = bind(run, job, artifact, job_log, first, last, row, job_layout)
    headers = list(re.finditer(r'Sustained matrix row=(\w+) seed=(\d+) duration=(\S+)', job_log))
    segments = {seed:job_log[headers[seed-first].end():headers[seed-first+1].start()
                    if seed < last else len(job_log)] for seed in range(first,last+1)}
    result = review_raw(revision, artifact_root, first, last, segments, temporary_root,
                        model_root, model_binary_out, row)
    result.update(shard_qualified=True, run=run['id'], job=job['id'], artifact=artifact['id'], job_layout=job_layout)
    return result


def review_raw(revision, artifact_root, first, last, segments, temporary_root=None,
               model_root=None, model_binary_out=None, row='journal', event_files=None):
    """Shared raw corpus checks only; caller must bind provider or native provenance."""
    if not re.fullmatch('[0-9a-f]{40}', revision) or type(first) is not int or type(last) is not int or not 1 <= first <= last <= 200:
        raise ValueError('invalid recorded source or original seed range')
    report_row, test = row_contract(row)
    if model_binary_out is not None:
        model_binary_out = Path(model_binary_out).resolve()
        if model_binary_out.exists() or model_binary_out.is_relative_to(artifact_root.resolve()):
            raise ValueError('model executable output must be fresh and outside original artifacts')
    repo = REPO if model_root is None else Path(model_root).resolve()
    before = shared.inventory(artifact_root)
    event_files = event_files or {seed:f'matrix-{row}-{seed}-test.jsonl' for seed in range(first,last+1)}
    if set(event_files) != set(range(first,last+1)) or set(segments) != set(range(first,last+1)):
        raise ValueError('incomplete raw event or execution log mapping')
    required = {f'matrix-{row}-{seed}-{suffix}' for seed in range(first,last+1)
                for suffix in ('faults.json', 'latencies.json', 'history.jsonl')} | set(event_files.values())
    if not required <= set(before) or {p for p in before if p.endswith('test.jsonl')} != {
            value for value in event_files.values() if value.endswith('test.jsonl')}:
        raise ValueError('missing, duplicate or unexpected raw seed evidence')
    # Compile production models only after proving the model dependency inputs
    # match the actually executed revision. Captured runtime binaries/stores are
    # unavailable in this workflow and are not claimed by this reviewer.
    source_inputs = shared.source_hashes(revision)
    dependencies = subprocess.check_output(
        ['go', 'list', '-deps', '-f', '{{.Dir}}|{{join .GoFiles " "}}|{{join .CgoFiles " "}}',
         'js-wf/history'], cwd=repo, text=True)
    model_inputs = {'go.mod', 'go.sum'}
    for line in dependencies.splitlines():
        directory, go_files, cgo_files = line.split('|')
        directory = Path(directory)
        if directory.is_relative_to(repo):
            model_inputs.update(str((directory/name).relative_to(repo))
                                for name in (go_files+' '+cgo_files).split())
    model_hashes = {name:hashlib.sha256((repo/name).read_bytes()).hexdigest()
                    for name in sorted(model_inputs)}
    if any(source_inputs.get(name) != digest for name,digest in model_hashes.items()):
        raise ValueError('current model dependency inputs differ from executed source')
    with tempfile.TemporaryDirectory(prefix='tier2-recorded-tools-', dir=temporary_root) as directory:
        recorded = Path(directory)
        def load(name, file):
            data = subprocess.check_output(['git', 'show', revision+':scripts/'+file], cwd=repo)
            path = recorded/file
            path.write_bytes(data)
            return module(name, path)
        campaign = load('journal_campaign', 'check-matrix-campaign.py')
        execution = load('journal_execution', 'check-matrix-result.py')
    reports = []
    with tempfile.TemporaryDirectory(prefix='tier2-history-shard-', dir=temporary_root) as temporary:
        binary = Path(temporary) / 'history-review'
        go = Path(temporary) / 'review.go'
        go.write_bytes(Path(__file__).with_name('tier2-history-review.go.txt').read_bytes())
        subprocess.run(['go', 'build', '-p=1', '-o', str(binary), str(go)], cwd=repo, check=True)
        binary_hash = hashlib.sha256(binary.read_bytes()).hexdigest()
        for seed in range(first, last + 1):
            r = artifact_root
            prefix = f'matrix-{row}-{seed}-'
            events = [json.loads(l) for l in (r / event_files[seed]).read_text().splitlines()]
            if any(e['Action'] in ('skip', 'fail', 'build-fail') for e in events):
                raise ValueError("raw evidence check failed: not any((e['Action'] in ('skip', 'fail', 'build-fail') for e in events))")
            execution.check(events, test, '10m')
            log = ''.join((e.get('Output', '') for e in events))
            report = campaign.check_seed(log, report_row, seed, test)
            segment = segments[seed]
            guard = f'Verified executed sustained test {test} duration=10m'
            if segment.count(guard) != 1 or campaign.check_seed(segment, report_row, seed, test) != report:
                raise ValueError('raw events differ from bound job log or its duration guard')
            fault_data = read(r / (prefix + 'faults.json'))
            faults = fault_data['faults']
            if not (type(fault_data['seed']) is int and fault_data['seed'] == seed and campaign.seconds(fault_data['duration']) == 600 and (len(faults) == report['faults'] == 19)):
                raise ValueError("raw evidence check failed: fault_data['seed'] == seed and campaign.seconds(fault_data['duration']) == 600 and (len(faults) == report['faults'] == 19)")
            previous = None
            for fault in faults:
                scheduled, killed, healed = map(timestamp_ns, [fault[k] for k in ('scheduled', 'killed', 'healed')])
                if not (scheduled <= killed <= healed and not fault.get('error')):
                    raise ValueError('invalid fault timing or recorded fault error')
                check_fault_identity(fault, row)
                if row == 'partition' and healed - killed > 35000000000:
                    raise ValueError('partition whole-cut fault recovery exceeds original35s budget')
                if previous is not None:
                    if not scheduled - previous == 30000000000:
                        raise ValueError('raw evidence check failed: scheduled - previous == 30000000000')
                previous = scheduled
            samples = read(r / (prefix + 'latencies.json'))
            terminal = {}
            progress = {t: [] for t in campaign.TYPES}
            last_deadline = timestamp_ns(faults[-1]['healed']) + 300000000000
            for sample in samples:
                typ, id, event = (sample['type'], sample['id'], sample['event'])
                delay = sample['delay_ns']
                if not (typ in campaign.TYPES and type(delay) is int and (delay >= 0)):
                    raise ValueError('raw evidence check failed: typ in campaign.TYPES and type(delay) is int and (delay >= 0)')
                enabled, observed = map(timestamp_ns, [sample['enabled'], sample['observed']])
                if not observed - enabled == delay:
                    raise ValueError('raw evidence check failed: observed - enabled == delay')
                if event == 'terminal':
                    if not ((typ, id) not in terminal and observed <= last_deadline):
                        raise ValueError('raw evidence check failed: (typ, id) not in terminal and observed <= last_deadline')
                    terminal[typ, id] = delay
                else:
                    progress[typ].append(delay)

            def p99(values):
                return sorted(values)[(len(values) * 99 + 99) // 100 - 1]

            def matches(ns, seconds):
                if not abs(ns - round(seconds * 1000000000.0)) <= 1:
                    raise ValueError('raw evidence check failed: abs(ns - round(seconds * 1000000000.0)) <= 1')
            if not len(terminal) == report['invocations']:
                raise ValueError("raw evidence check failed: len(terminal) == report['invocations']")
            matches(p99(list(terminal.values())), report['terminal_p99_seconds'])
            for typ in campaign.TYPES:
                values = [v for (t, id), v in terminal.items() if t == typ]
                cell = report['cells'][typ]
                pr = report['progress'][typ]
                if not (len(values) == cell['invocations'] and len(progress[typ]) == pr['events']):
                    raise ValueError("raw evidence check failed: len(values) == cell['invocations'] and len(progress[typ]) == pr['events']")
                matches(p99(values), cell['terminal_p99_seconds'])
                matches(p99(progress[typ]), pr['p99_seconds'])
                matches(max(progress[typ]), pr['max_seconds'])
                if not sum((v >= 30000000000 for v in progress[typ])) == pr['above_30s']:
                    raise ValueError("raw evidence check failed: sum((v >= 30000000000 for v in progress[typ])) == pr['above_30s']")
            result = subprocess.check_output([str(binary), str(r / (prefix + 'history.jsonl'))], cwd=repo, text=True)
            operations = len((r / (prefix + 'history.jsonl')).read_text().splitlines())
            if not result == ''.join((f'whole {name} operations={operations} verdict=Ok error=<nil>\n' for name in ('starts', 'signals', 'results'))):
                raise ValueError("raw evidence check failed: result == ''.join((f'whole {name} operations={operations} verdict=Ok error=<nil>\\n' for name in ('starts', 'signals', 'results')))")
            reports.append(dict(seed=seed, report=report, raw_latency_samples=len(samples), raw_faults_verified=len(faults), raw_history_operations=operations, independent_history_output=result))

        retained_binary = binary.read_bytes() if model_binary_out is not None else None
        if retained_binary is not None and hashlib.sha256(retained_binary).hexdigest() != binary_hash:
            raise ValueError('model executable changed during review')

    if shared.inventory(artifact_root) != before:
        raise ValueError('original evidence changed during review')
    if any(hashlib.sha256((repo/name).read_bytes()).hexdigest() != digest
           for name,digest in model_hashes.items()):
        raise ValueError('model dependency inputs changed during review')
    if model_binary_out is not None:
        with model_binary_out.open('xb') as output:
            output.write(retained_binary)
        model_binary_out.chmod(0o755)
        if hashlib.sha256(model_binary_out.read_bytes()).hexdigest() != binary_hash:
            raise ValueError('retained model executable failed readback verification')
    return dict(raw_data_verified=True, revision=revision, row=row, first=first, last=last,
                duration_seconds_per_seed=600, seeds=reports,
                all_three_independent_history_models_pass=True,
                model_source_root=str(repo),
                model_dependency_sha256=model_hashes, model_binary_sha256=binary_hash,
                model_binary_retained=model_binary_out is not None,
                model_binary_path=str(model_binary_out) if model_binary_out is not None else None,
                independent_reviewer_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
                timestamp_parser_scope='Independent strict RFC3339Nano parser; campaign/duration and history models remain bound to executed source.',
                model_helper_sha256=hashlib.sha256(Path(__file__).with_name('tier2-history-review.go.txt').read_bytes()).hexdigest(),
                invocations=sum(s['report']['invocations'] for s in reports),
                journal_entries=sum(s['report']['journal_entries'] for s in reports),
                faults=sum(s['raw_faults_verified'] for s in reports),
                input_sha256=before, executed_source_hashes=source_inputs,
                qualifies_parent_campaign=False, qualifies_full_row=False,
                clears_tier2_200_seed_gate=False, clears_tier3_24_hour_soak=False,
                scope='Requested complete selected fault shard only. Raw faults/latencies and independent history models verified. Named-test final integrity/drain assertions; no captured workload binary, source ledgers or physical stores.')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ('run', 'job', 'artifact', 'log', 'root', 'output'):
        parser.add_argument('--'+name, required=True, type=Path)
    parser.add_argument('--row', choices=tuple(ROWS), default='journal')
    parser.add_argument('--job-layout', choices=('shard', 'single'), default='shard',
                        help='Require the exact selected workflow job naming layout; single requires one seed')
    parser.add_argument('--first', required=True, type=int)
    parser.add_argument('--last', required=True, type=int)
    parser.add_argument('--temporary-root', type=Path)
    parser.add_argument('--model-root', type=Path,
                        help='Source checkout for models; dependencies must match the executed revision')
    parser.add_argument('--model-binary-out', type=Path,
                        help='Retain the actual model executable at a fresh path outside original artifacts')
    args = parser.parse_args()
    root = args.root.resolve()
    output = args.output.resolve()
    if output.exists() or output.is_relative_to(root):
        parser.error('output must be fresh and outside original artifacts')
    report = review(read(args.run), read(args.job), read(args.artifact),
                    args.log.read_text(), root, args.first, args.last, args.temporary_root,
                    args.model_root, args.model_binary_out, args.row, args.job_layout)
    report['metadata_sha256'] = {name:hashlib.sha256(getattr(args,name).read_bytes()).hexdigest()
                                for name in ('run','job','artifact','log')}
    output.write_text(json.dumps(report, indent=2)+'\n')
    print(json.dumps({key:report[key] for key in ('shard_qualified','revision','first','last',
                                               'invocations','journal_entries','faults')},indent=2))

if __name__ == '__main__':
    main()
