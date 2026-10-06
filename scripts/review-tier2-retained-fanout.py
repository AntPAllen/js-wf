#!/usr/bin/env python3
"""Review closed original Tier2 fanout restart evidence without opening stores."""
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


def children(records, count, terminal=False):
    require(bool(records), 'empty parent or child journal')
    if terminal:
        require(records[-1]['kind'] == 'Completed' and
                sum(r['kind'] in ('Completed', 'Failed') for r in records) == 1,
                'missing or duplicate terminal completion')
    ids = [r['payload']['child_id'] for r in records
           if r['kind'] == 'StepRequested' and r.get('payload', {}).get('kind') == 'call_async']
    require(len(ids) == len(set(ids)) == count and all(ids), 'wrong, empty or duplicate child identity')
    return set(ids)


def review_cut(fault, cut, recovered, final):
    require(not fault.get('error') and fault['node'] == -1 and fault['nodes'] == [0, 1, 2],
            'fault lacks successful three-node restart')
    require(ns(fault['scheduled']) <= ns(cut['ObservedAt']) <= ns(fault['killed']) <= ns(fault['healed']),
            'cut or restart timing is reversed')
    for key in ('scheduled', 'fanout_parent', 'fanout_tail', 'fanout_children', 'fanout_pending_children'):
        require(cut['Fault'][key] == fault[key], 'cut identity differs from recorded fault')
    require(bool(fault['fanout_parent']) and type(fault['fanout_tail']) is int and fault['fanout_tail'] > 0,
            'missing parent identity or journal tail')
    prefix = cut['ParentPrefix']
    require(prefix and prefix[-1]['kind'] == 'Suspended' and prefix[-1]['sequence'] == fault['fanout_tail'],
            'cut does not hold the suspended parent tail')
    expected = children(prefix, 6)
    require(len(fault['fanout_children']) == 6 and set(fault['fanout_children']) == expected,
            'cut child identities disagree')
    pending = fault['fanout_pending_children']
    require(pending and len(pending) == len(set(pending)) and set(pending) <= expected,
            'missing, duplicate or unrelated unfinished child')
    child_prefixes = cut['ChildPrefixes']
    require(set(child_prefixes) == expected, 'incomplete child cut journals')
    unfinished = {child for child, records in child_prefixes.items()
                  if not records or records[-1]['kind'] not in ('Completed', 'Failed')}
    require(set(pending) == unfinished, 'unfinished selection disagrees with child cut journals')
    require(recovered[:len(prefix)] == prefix and final['Parent'][:len(prefix)] == prefix,
            'parent prefix changed after restart or before terminal')
    require(children(final['Parent'], 6, True) == expected and set(final['Children']) == expected,
            'terminal parent or children changed identity')
    grandchild_ids = set()
    for child, records in final['Children'].items():
        before = child_prefixes[child]
        require(records[:len(before)] == before, 'child prefix changed after restart')
        descendants = children(records, 2, True)
        require(not descendants & grandchild_ids, 'grandchild shared across distinct children')
        grandchild_ids.update(descendants)
    require(len(grandchild_ids) == 12 and set(final['Grandchildren']) == grandchild_ids,
            'missing or unrelated final grandchild journals')
    for records in final['Grandchildren'].values():
        require(records and records[-1]['kind'] == 'Completed' and
                sum(r['kind'] in ('Completed', 'Failed') for r in records) == 1,
                'grandchild did not complete exactly once')


def review(root):
    e = read(root/'execution.json'); revision = e['source']
    test = 'TestMixedMatrixFanoutRestartEveryThirtySeconds'
    require(e['row'] == 'fanoutrestart' and e['test'] == test and e['status'] == 'passed' and
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
    load('fanout_duration', SCRIPTS/'check-matrix-result.py').check(
        [json.loads(line) for line in (root/'converted-events.jsonl').read_text().splitlines()], test, '10m')
    environment = read(root/'commands.json')['environment']
    require((environment['GOMAXPROCS'], environment['GOMEMLIMIT'], environment['WF_MATRIX_DURATION']) ==
            ('2', '2GiB', '10m'), 'wrong native execution profile')
    seed = int(environment['FAULT_SEED']); prefix = f'matrix-fanoutrestart-{seed}'
    faults = read(root/(prefix+'-faults.json'))
    campaign = load('fanout_campaign', SCRIPTS/'check-matrix-campaign.py')
    result = campaign.check_seed((root/'native.log').read_text(), 'fanout_restart', seed, test)
    require(faults['seed'] == seed and campaign.seconds(faults['duration']) == 600 and
            len(faults['faults']) == result['faults'] == 19, 'wrong original duration or fault count')
    first = ns(faults['faults'][0]['scheduled'])
    for index, fault in enumerate(faults['faults']):
        scheduled = ns(fault['scheduled'])
        require(scheduled == first + index*30_000_000_000, 'wrong original fault cadence')
        base = root/(prefix+f'-fault-{scheduled}')
        review_cut(fault, read(Path(str(base)+'-fanout-cut.json')),
                   read(Path(str(base)+'-fanout-recovered-prefix.json')),
                   read(root/(prefix+'-fanout-final')/f'fault-{index+1}-fanout-final.json'))
    count = result['invocations']
    return dict(source=revision, row=e['row'], seed=seed, actual_sdk_pid=e['pid'],
                actual_sdk_sha256=e['sha256'], source_files=len(before['files']), external_files=len(external),
                server_observations=len(servers), exact_unfinished_fanout_cuts_verified=19, seed_review=result,
                expected_original_report=dict(Invocations=count, Journals=count, Entries=result['journal_entries'], Terminal=count),
                scope='Original normal ten-minute native fanout restart; independent copied stores and full matrix remain separate')


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
