#!/usr/bin/env python3
"""Review closed original Tier2 rolling upgrade evidence without opening stores."""
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

common = load('upgrade_common', SCRIPTS/'review-tier2-retained-workers.py')
require, read, sha, ns = common.require, common.read, common.sha, common.ns



def review_physical_snapshot(record, phase, drained=False):
    require(record['phase']==phase and len(record['peers'])==3 and
            all(type(p['node']) is int for p in record['peers']) and
            {p['node'] for p in record['peers']}=={0,1,2}, 'missing pinned peers or wrong phase')
    identities=set()
    for peer in record['peers']:
        identity=peer['state'].get('server_id')
        require(not peer.get('error') and ns(peer['started'])<=ns(peer['finished']) and
                isinstance(identity,str) and bool(identity) and identity not in identities,
                'peer monitoring unavailable, interval reversed or identity duplicated')
        identities.add(identity)
        streams=[stream for account in peer['state'].get('account_details',[])
                 for stream in account.get('stream_detail',[]) if stream['name']=='WF_RUN']
        require(len(streams)==1, 'missing or duplicate local physical run stream')
        state=streams[0]['state']
        require(type(state.get('messages')) is int and state['messages']>=0 and
                type(state.get('consumer_count')) is int and state['consumer_count']==64,
                'invalid physical queue or original durable census')
        require(not drained or state['messages']==0, 'physical run stream not drained on every server')
        peer['run_state']=state
    return record


def review_upgrade_faults(faults, snapshots):
    require(len(faults)==3, 'require all three original server upgrades')
    versions=['2.11.17']*3;upgraded=set();first=ns(faults[0]['scheduled']);records=[]
    for index,fault in enumerate(faults):
        scheduled=ns(fault['scheduled']);node=fault['node']
        require(not fault.get('error') and type(node) is int and node in (0,1,2) and
                node not in upgraded and scheduled==first+index*270_000_000_000 and
                scheduled<=ns(fault['killed'])<=ns(fault['healed']), 'upgrade fault failed or original cadence invalid')
        require(fault['versions_before']==versions and versions[node]=='2.11.17', 'wrong starting upgrade versions')
        versions=versions.copy();versions[node]='2.15.0';upgraded.add(node)
        require(fault['versions_after']==versions, 'upgrade changed another node or target version')
        before_phase=f'upgrade-{scheduled}-{node}-before';healed_phase=f'upgrade-{scheduled}-{node}-healed'
        before_snapshot=review_physical_snapshot(snapshots(before_phase),before_phase)
        healed_snapshot=review_physical_snapshot(snapshots(healed_phase),healed_phase)
        require(all(ns(p['finished'])<=ns(fault['killed']) for p in before_snapshot['peers']) and
                all(ns(p['started'])>=ns(fault['healed']) for p in healed_snapshot['peers']),
                'monitor snapshot not at recorded upgrade cut')
        records.extend((before_snapshot,healed_snapshot))
    drained=review_physical_snapshot(snapshots('drained'),'drained',True)
    require(all(ns(p['started'])>=ns(faults[-1]['healed']) for p in drained['peers']),
            'physical drain snapshot predates final upgrade healing')
    records.append(drained)
    return records


def review(root):
    e = read(root/'execution.json'); revision = e['source']
    test = 'TestMixedMatrixRollingServerUpgrade'
    require(e['row'] == 'upgrade' and e['test'] == test and e['status'] == 'passed' and
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
    load('upgrade_duration', SCRIPTS/'check-matrix-result.py').check(
        [json.loads(line) for line in (root/'converted-events.jsonl').read_text().splitlines()], test, '10m')
    environment = read(root/'commands.json')['environment']
    require((environment['GOMAXPROCS'], environment['GOMEMLIMIT'], environment['WF_MATRIX_DURATION']) ==
            ('2', '2GiB', '10m'), 'wrong native execution profile')
    seed = int(environment['FAULT_SEED']); prefix = f'matrix-upgrade-{seed}'
    faults = read(root/(prefix+'-faults.json'))
    campaign = load('upgrade_campaign', SCRIPTS/'check-matrix-campaign.py')
    result = campaign.check_seed((root/'native.log').read_text(), 'rolling_upgrade', seed, test)
    require(type(faults['seed']) is int and faults['seed'] == seed and campaign.seconds(faults['duration']) == 600 and
            len(faults['faults']) == result['faults'] == 3, 'wrong original duration or fault count')
    legacy = read(root/'legacy-server-input.json')
    require(sha(root/'legacy-server.bin') == legacy['sha256'] and '\tmod\tgithub.com/nats-io/nats-server/v2\tv2.11.17\t' in legacy['build_info'], 'legacy input identity invalid')
    require({v['node'] for v in servers if v['actual_executable_sha256']==legacy['sha256']}=={0,1,2}, 'actual legacy executables missing on peers')
    require({v['node'] for v in servers if v['actual_executable_sha256']!=legacy['sha256']}=={0,1,2}, 'actual replacement executables missing on peers')
    current = root/'originals'/test/'nats-server'
    current_sha = sha(current)
    current_info = subprocess.check_output(['go','version','-m',str(current)],text=True)
    require('\tmod\tgithub.com/nats-io/nats-server/v2\tv2.15.0\t' in current_info and current_sha != legacy['sha256'],
            'replacement input is not the recorded current server version')
    require(len(servers)==6, 'require initial and replacement generations for every node')
    for server in servers:
        is_legacy = server['actual_executable_sha256']==legacy['sha256']
        expected_path=root/'legacy-server.bin' if is_legacy else current
        require(server['argv'][0]==str(expected_path) and
                server['actual_executable_sha256']==(legacy['sha256'] if is_legacy else current_sha),
                'actual server executable differs from legacy/current input')
    snapshots = review_upgrade_faults(faults['faults'], lambda phase: read(root/(prefix+'-peer-queues-'+phase+'.json')))
    count = result['invocations']
    return dict(source=revision, row=e['row'], seed=seed, actual_sdk_pid=e['pid'],
                actual_sdk_sha256=e['sha256'], source_files=len(before['files']), external_files=len(external),
                server_observations=len(servers), exact_server_upgrades_verified=3, physical_peer_snapshots=snapshots, seed_review=result,
                expected_original_report=dict(Invocations=count, Journals=count, Entries=result['journal_entries'], Terminal=count),
                scope='Original normal ten-minute native rolling upgrade; independent copied stores and full matrix remain separate')


def main():
    parser = argparse.ArgumentParser(description='Review closed original ten-minute rolling upgrade, source/process identities and pinned physical monitoring')
    parser.add_argument('--root', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    require(args.root.is_absolute(), 'require absolute closed fixture root')
    result = review(args.root.resolve())
    args.output.write_text(json.dumps(result, indent=2)+'\n')
    print(json.dumps(result, indent=2))

if __name__ == '__main__':
    main()
