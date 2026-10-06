#!/usr/bin/env python3
"""Review closed original Tier2 server clock evidence without opening stores."""
import argparse
import json
from pathlib import Path
import subprocess
import importlib.util

SCRIPTS = Path(__file__).resolve().parent

def load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module

common = load('fanout_common', SCRIPTS/'review-tier2-retained-workers.py')
require, read, sha, ns = common.require, common.read, common.sha, common.ns


def review_clock_faults(faults, offset):
    require(offset in (-60_000_000_000,60_000_000_000) and len(faults)==19, 'require original skew and nineteen observations')
    first = ns(faults[0]['scheduled'])
    for index, fault in enumerate(faults):
        require(not fault.get('error') and fault['node']==2 and ns(fault['scheduled'])==first+index*30_000_000_000 and
                ns(fault['healed'])>=ns(fault['scheduled']), 'clock observation failed or cadence invalid')
        samples = fault['server_clocks']
        require(len(samples)==3 and all(type(x['node']) is int for x in samples) and {x['node'] for x in samples}=={0,1,2}, 'missing or duplicate clock peers')
        for sample in samples:
            want = offset if sample['node']==2 else 0
            require(type(sample['offset_ns']) is int and abs(sample['offset_ns']-want)<=2_000_000_000,
                    'measured peer clock differs from original offset tolerance')
            ns(sample['server_at'])


def review_latency_offsets(samples, offset, count):
    require(bool(samples) and all(type(s.get('server_clock_offset_ns')) is int and s['server_clock_offset_ns']==offset for s in samples), 'latency clock correction differs from observed skew')
    require(sum(s['event']=='terminal' for s in samples)==count, 'terminal sample count differs from complete cohort')


def review(root):
    e = read(root/'execution.json'); revision = e['source']
    row = e['row']; require(row in ('serverclockplus','serverclockminus'), 'unsupported server clock row')
    positive = row == 'serverclockplus'
    test = 'TestMixedMatrixServerClockSkew'+('Positive' if positive else 'Negative')
    offset = 60_000_000_000 if positive else -60_000_000_000
    require(e['test'] == test and e['status'] == 'passed' and
            e['exit_code'] == e['server_observer_errors'] == 0 and e['duration'] == '10m' and
            e['sustained_ten_minutes'] and not e['race'], 'require original successful normal ten-minute row')
    require(not Path('/proc/'+str(e['pid'])).exists() and sha(root/'integration.test') == e['sha256'],
            'SDK is live or executable bytes changed')
    require('vcs.revision='+revision in e['build_info'] and 'vcs.modified=false' in e['build_info'],
            'SDK source identity invalid')
    git = lambda name: subprocess.check_output(['git', 'show', revision+':'+name], cwd=SCRIPTS.parent)
    before = read(root/'source-before.json')
    require(before == read(root/'source-after.json') and before['revision'] == revision, 'source inventory changed')
    for name, digest in before['files'].items():
        require(sha(root/'source'/name) == digest == common.hashlib.sha256(git(name)).hexdigest(), 'source bytes changed')
    external = read(root/'external-source-before.json'); paths = read(root/'external-captured-paths.json')
    require(external == read(root/'external-source-after.json') and set(external) == set(paths), 'external inputs changed')
    for name, digest in external.items():
        require(sha(root/paths[name]) == digest, 'captured external bytes changed')
    for name, captured in [('run-tier2-retained-row.py', 'executed-producer.py'),
                           ('matrix_process_observer.py', 'matrix_process_observer.py'),
                           ('matrix_worker_observer.py', 'matrix_worker_observer.py'),
                           ('tier2_retained_profiles.py', 'tier2_retained_profiles.py'),
                           ('check-matrix-result.py', 'executed-checker.py')]:
        require((root/captured).read_bytes() == git('scripts/'+name), 'executed helper differs from original Git')
    for name in ('check-matrix-result.py', 'check-matrix-campaign.py', 'tier3-worker-clock-evidence.py'):
        require((SCRIPTS/name).read_bytes() == git('scripts/'+name), 'review dependency differs from executed source')
    servers = read(root/'observed-servers.json')
    require({v['node'] for v in servers} == {0, 1, 2} and
            len({(v['pid'], v['start_ticks']) for v in servers}) == len(servers), 'missing or duplicate server observations')
    for server in servers:
        require(not Path('/proc/'+str(server['pid'])).exists() and
                sha(root/server['captured']) == server['actual_executable_sha256'] and
                Path(server['store']).is_relative_to(root/'originals'), 'server identity, closure or store scope invalid')
    acceptance = read(root/'acceptance.json')
    require(acceptance['exit_code'] == acceptance['native_exit_code'] == 0, 'native duration check failed')
    load('server_clock_duration', SCRIPTS/'check-matrix-result.py').check(
        [json.loads(line) for line in (root/'converted-events.jsonl').read_text().splitlines()], test, '10m')
    environment = read(root/'commands.json')['environment']
    require((environment['GOMAXPROCS'], environment['GOMEMLIMIT'], environment['WF_MATRIX_DURATION']) ==
            ('2', '2GiB', '10m'), 'wrong native execution profile')
    seed = int(environment['FAULT_SEED']); prefix = f'matrix-{row}-{seed}'
    faults = read(root/(prefix+'-faults.json'))
    campaign = load('server_clock_campaign', SCRIPTS/'check-matrix-campaign.py')
    result = campaign.check_seed((root/'native.log').read_text(), 'server_clock_plus' if positive else 'server_clock_minus', seed, test)
    require(type(faults['seed']) is int and faults['seed'] == seed and campaign.seconds(faults['duration']) == 600 and
            len(faults['faults']) == result['faults'] == 19, 'wrong original duration or fault count')
    review_clock_faults(faults['faults'], offset)
    fixture = root/'originals'/test
    overlay = read(fixture/'clock/skew-overlay.json')['Replace']
    require(len(overlay)==1, 'clock overlay has unexpected inputs')
    original_time, patched_time = next(iter(overlay.items()))
    require(Path(patched_time)==fixture/'clock/skew-time.go' and original_time in paths,
            'clock source or patch not retained')
    original_bytes = (root/paths[original_time]).read_bytes()
    marker = b'\tsec, nsec, mono := runtimeNow()\n'
    require(original_bytes.count(marker)==1, 'unknown original Go clock source shape')
    patch = marker+f'\tsec += {offset//1_000_000_000} // test-only wall-clock skew\n'.encode()
    require(Path(patched_time).read_bytes()==original_bytes.replace(marker,patch), 'clock overlay differs from exact recorded offset')
    require(len(servers)==3, 'unexpected server process generations')
    skewed = [v for v in servers if v['node']==2]
    require(len(skewed)==1 and sha(fixture/'nats-server-skewed')==skewed[0]['actual_executable_sha256'] and
            skewed[0]['argv'][0]==str(fixture/'nats-server-skewed'), 'actual node2 executable does not match clock build')
    require(all(v['actual_executable_sha256']==sha(fixture/'nats-server') and
                v['argv'][0]==str(fixture/'nats-server') for v in servers if v['node']!=2),
            'neutral peer executable differs from normal build')
    count = result['invocations']
    review_latency_offsets(read(root/(prefix+'-latencies.json')), offset, count)
    return dict(source=revision, row=e['row'], seed=seed, actual_sdk_pid=e['pid'],
                actual_sdk_sha256=e['sha256'], source_files=len(before['files']), external_files=len(external),
                server_observations=len(servers), server_clock_offset_ns=offset, exact_clock_observations_verified=19, seed_review=result,
                expected_original_report=dict(Invocations=count, Journals=count, Entries=result['journal_entries'], Terminal=count),
                scope='Original normal ten-minute native server-clock row; independent copied stores and full matrix remain separate')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    require(args.root.is_absolute(), 'require absolute closed fixture root')
    result = review(args.root.resolve())
    args.output.write_text(json.dumps(result, indent=2)+'\n')
    print(json.dumps(result, indent=2))

if __name__ == '__main__':
    main()
