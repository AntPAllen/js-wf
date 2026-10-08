#!/usr/bin/env python3
"""Independently review closed local partition seeds without opening stores."""
import argparse
import hashlib
import importlib.util
import json
from pathlib import Path
import re
import subprocess

import fixture_archive

REPO = Path(__file__).resolve().parents[1]


def load(name, filename):
    spec = importlib.util.spec_from_file_location(name, REPO/'scripts'/filename)
    result = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(result)
    return result


raw = load('local_partition_raw', 'check-tier2-journal-shard.py')
closed = load('local_partition_closed', 'verify-tier2-closed-originals.py')
TEST = 'TestMixedMatrixServerPartitionEveryThirtySeconds'


def require(value, message):
    if not value:
        raise ValueError(message)


def read(path):
    return json.loads(Path(path).read_text())


def sha(path):
    with Path(path).open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def closed_instance(pid, birth):
    require(type(pid) is int and pid > 0 and isinstance(birth, str) and birth.isdigit(), 'invalid process birth identity')
    try:
        current = Path(f'/proc/{pid}/stat').read_text().rsplit(')', 1)[1].split()[19]
    except (FileNotFoundError, ProcessLookupError):
        return
    require(current != birth, 'recorded process incarnation remains live')


def contract(state, seed=None, server_profile="default"):
    require(state.get('schema') == 'js-wf-local-tier2-partition-campaign-v1'
            and re.fullmatch('[0-9a-f]{40}', state.get('source', ''))
            and state.get('row') == 'partition' and state.get('test') == TEST
            and state.get('duration') == '10m' and state.get('sdk_timeout') == '18m'
            and state.get('race') is False and state.get('seeds') == list(range(1, 201))
            and all(type(n) is int for n in state['seeds']), 'campaign contract/source/range differs')
    require(server_profile in ('default','experimental-component-candidate') and state.get('server_profile','default')==server_profile, 'campaign server profile differs from explicit review selection')
    records = state.get('records', [])
    require(len(records) <= 200 and all(type(r.get('seed')) is int for r in records)
            and [r['seed'] for r in records] == list(range(1, len(records)+1)), 'incomplete, duplicate or reordered recorded seed range')
    if seed is None:
        require(state.get('status') == 'native_execution_completed' and type(state.get('exit_code')) is int and state.get('exit_code') == 0
                and state.get('native_coverage_complete') is True and len(records) == 200,
                'full row requires terminal original200 execution')
        closed_instance(state['producer_pid'], state['producer_start_ticks'])
        chosen = records
    else:
        require(type(seed) is int and 1 <= seed <= len(records), 'requested seed has no terminal campaign record')
        chosen = [records[seed-1]]
    require(all(type(r.get('exit_code')) is int and r['exit_code'] == 0
                and r.get('root') == f'seed-{r["seed"]:03d}' and r.get('profile_verified',True) is True
                and r.get('server_profile','default')==server_profile for r in chosen), 'selected seed failed or root identity differs')
    if server_profile=='experimental-component-candidate':
        require(re.fullmatch('[0-9a-f]{64}',state.get('candidate_sha256','')) and all(r.get('server_profile')==server_profile and r.get('profile_verified') is True and r.get('candidate_sha256')==state['candidate_sha256'] for r in chosen), 'candidate seed profile/hash verification missing or mixed')
    return chosen



def candidate_binding(root, revision, canonical):
    canonical=Path(canonical)
    require(not canonical.is_absolute() and '..' not in canonical.parts, 'candidate proof must be repository-relative')
    retained=root/'inputs/partition-server-proof'
    input_record=read(root/'partition-server-input.json')
    require(input_record['parent_proof']==str(canonical) and input_record['parent_proof_revision']==revision
            and input_record['captured']=='partition-server.bin', 'candidate parent route/revision differs')
    names=['archive-verification.json','fixture-inventory.json','s3-readback.json','independent-review.json',
           'candidate-build.json','candidate-module-inputs.json','candidate-nats-source-before.json',
           'candidate-nats-source-after.json','candidate-dependencies-before.json','candidate-dependencies-after.json',
           'candidate-overlay.json','source-before.json','source-after.json']
    require(set(input_record['retained_parent_records'])==set(names),'candidate parent record census differs')
    for name in names:
        committed=subprocess.check_output(['git','cat-file','blob',revision+':'+str(canonical/name)],cwd=REPO)
        require((retained/name).read_bytes()==committed and sha(retained/name)==input_record['retained_parent_records'][name], 'candidate committed parent input changed: '+name)
    meta=read(retained/'archive-verification.json');manifest=read(retained/'fixture-inventory.json')
    receipt=read(retained/'s3-readback.json');review=read(retained/'independent-review.json');build=read(retained/'candidate-build.json')
    expected=build['executable_sha256']
    require(meta['schema']==fixture_archive.SCHEMA and meta['all_archive_members_read_back'] is True
            and meta['all_current_fixture_files_unchanged_after_capture'] is True
            and sha(retained/'fixture-inventory.json')==meta['inventory_sha256']
            and receipt['canonical_metadata']==str(canonical/'archive-verification.json')
            and receipt['archive']['full_readback']==dict(bytes=meta['archive_bytes'],sha256=meta['archive_sha256'])
            and review['complete_archive']==meta and review['source']==build['source']==input_record['parent_source']
            and review['result']['recovered'] is True and review['configuration_equal_prior_failed_production_component'] is True
            and review['three_live_candidate_executable_argv_birth_captures_verified'] is True
            and review['candidate_sha256']==expected==input_record['sha256']==manifest['files']['candidate-server']['sha256']
            and (root/'partition-server.bin').stat().st_size==manifest['files']['candidate-server']['bytes']
            and sha(root/'partition-server.bin')==expected
            and read(retained/'candidate-nats-source-before.json')==read(retained/'candidate-nats-source-after.json')
            and read(retained/'candidate-dependencies-before.json')==read(retained/'candidate-dependencies-after.json')
            and read(retained/'source-before.json')==read(retained/'source-after.json'), 'candidate recovery/source/executable/full archive parent binding differs')
    peers=read(root/'partition-server-verification.json')
    require(peers['passed'] is True and peers['expected_sha256']==expected and type(peers['observed_peers']) is int
            and peers['observed_peers']==3 and peers['server_profile']=='experimental-component-candidate', 'candidate actual three-peer terminal verification differs')
    return expected


def terminal_cohort(segment, invocations, required):
    """Bind corrected-source latency population to the retained report."""
    marker = 'MATRIX_TERMINAL_COHORT '
    if not required:
        require(marker not in segment, 'terminal cohort marker absent from executed source')
        return None
    matches = list(re.finditer(
        r'MATRIX_TERMINAL_COHORT cutoff=(\d+) expected_invocations=(\d+) visited_invocations=(\d+)(?=\s|$)',
        segment))
    require(segment.count(marker) == len(matches) == 1, 'requires exactly one terminal cohort proof')
    cutoff, expected, visited = map(int, matches[0].groups())
    require(type(invocations) is int and invocations > 0 and invocations % 28 == 0
            and cutoff >= invocations and expected == visited == invocations,
            'terminal latency population differs from complete retained cohort')
    retained = segment.find('MATRIX_RETAINED row=server_partition ')
    require(retained > matches[0].end(), 'terminal cohort must precede final retained report')
    checkpoints = [(int(batch), int(tail)) for batch, tail in re.findall(
        r'checkpoint audit batch=(\d+) invocation_cutoff=(\d+) started', segment)]
    require(all(batch * 28 <= expected and tail <= cutoff for batch, tail in checkpoints),
            'terminal cohort precedes acknowledged checkpoint population')
    return dict(cutoff=cutoff, expected_invocations=expected, visited_invocations=visited,
                bound_to_raw_retained_invocations=True)


def seed_review(campaign_root, record, revision, model_root, output_root, candidate_proof=None):
    profile='experimental-component-candidate' if candidate_proof is not None else 'default'
    seed = record['seed']
    root = campaign_root/record['root']
    execution = read(root/'execution.json')
    require(execution.get('source') == revision and execution.get('row') == 'partition'
            and execution.get('test') == TEST and execution.get('duration') == '10m'
            and execution.get('race') is False and execution.get('sustained_ten_minutes') is True
            and execution.get('status') == 'passed' and execution.get('exit_code') == 0
            and execution.get('server_observer_errors') == 0, 'requires original successful closed native execution')
    closed_instance(execution['pid'], execution['start_ticks'])
    before = fixture_archive.inventory(root)
    expected_records = {'execution.json', 'commands.json', 'acceptance.json', 'source-before.json',
                        'source-after.json', 'external-source-before.json', 'external-source-after.json'}
    if candidate_proof is not None:expected_records.update(('partition-server-input.json','partition-server-verification.json'))
    require(set(record['record_sha256']) == expected_records, 'campaign terminal record census differs')
    for name, digest in record['record_sha256'].items():
        require(sha(root/name) == digest, 'campaign terminal record bytes changed')
    command = read(root/'commands.json')
    require(command['test_command'] == execution['actual_argv'] == [str(root/'integration.test'),
            '-test.run=^'+TEST+'$', '-test.count=1', '-test.v', '-test.timeout=18m'], 'SDK actual selector/count/deadline differs')
    env = command['environment']
    require(command.get('server_profile','default')==execution.get('server_profile','default')==profile, 'native/command server profile differs')
    candidate_sha=None
    if candidate_proof is None:
        require('WF_MATRIX_PARTITION_SERVER_BIN' not in env, 'original campaign review rejects experimental server profiles')
    else:
        candidate_sha=candidate_binding(root,revision,candidate_proof)
        require(env.get('WF_MATRIX_PARTITION_SERVER_BIN')==str(root/'partition-server.bin')
                and record.get('candidate_sha256')==candidate_sha and record.get('profile_verified') is True, 'candidate native environment/terminal record differs')
    require((env['GOMAXPROCS'], env['GOMEMLIMIT'], env['WF_MATRIX_DURATION'], env['FAULT_SEED'])
            == ('2', '2GiB', '10m', str(seed)) and env['WF_MATRIX_CHAOS'] == '1'
            and env['WF_MATRIX_OPERATION_TIMINGS'] == '1'
            and env['WF_MATRIX_PROCESS_ROOT'] == str(root/'originals')
            and env['MATRIX_ARTIFACT_PREFIX'] == str(root/f'matrix-partition-{seed}')
            and command['prepared_input_root'] == str(campaign_root/'prepared')
            and command['build'] is None, 'original profile or preparation route differs')
    binary_info = subprocess.check_output(['go', 'version', '-m', str(root/'integration.test')], text=True)
    require(binary_info.splitlines()[1:] == execution['build_info'].splitlines()[1:]
            and 'vcs.revision='+revision in binary_info and 'vcs.modified=false' in binary_info
            and '-race=true' not in binary_info and sha(root/'integration.test') == execution['sha256'], 'actual SDK bytes/build identity differ')
    git = lambda name: subprocess.check_output(['git', 'show', revision+':'+name], cwd=REPO)
    source = read(root/'source-before.json')
    require(source == read(root/'source-after.json') and source['revision'] == revision, 'source ledger changed')
    names = subprocess.check_output(['git', 'ls-tree', '-r', '--name-only', revision], cwd=REPO, text=True).splitlines()
    names = {n for n in names if n.endswith('.go') or n in ('go.mod', 'go.sum')}
    require(set(source['files']) == names, 'selected Git source census incomplete')
    for name, digest in source['files'].items():
        require(sha(root/'source'/name) == digest == hashlib.sha256(git(name)).hexdigest(), 'selected Git input changed: '+name)
    external = read(root/'external-source-before.json')
    paths = read(root/'external-captured-paths.json')
    require(external == read(root/'external-source-after.json') and set(paths) == set(external), 'external ledger changed or incomplete')
    for name, digest in external.items():
        relative = Path(paths[name])
        require(not relative.is_absolute() and '..' not in relative.parts
                and sha(root/relative) == digest == sha(name), 'external capture/current model input differs')
    prepared = campaign_root/'prepared'
    prepared_ref = read(root/'prepared-inputs.json')
    proof = read(prepared/'preparation.json')
    require(prepared_ref['root'] == str(prepared) and prepared_ref['preparation'] == proof
            and proof['source'] == revision and proof['status'] == 'prepared' and proof['race'] is False
            and proof['sha256'] == execution['sha256'] == sha(prepared/'integration.test')
            and prepared_ref['preparation_sha256'] == sha(prepared/'preparation.json')
            and source == read(prepared/'source-before.json') == read(prepared/'source-after.json')
            and external == read(prepared/'external-source-before.json') == read(prepared/'external-source-after.json')
            and prepared_ref['source_before_sha256'] == sha(prepared/'source-before.json')
            and prepared_ref['external_before_sha256'] == sha(prepared/'external-source-before.json'), 'prepared SDK/input binding differs')
    for name, captured in [('run-tier2-retained-row.py','executed-producer.py'),
                           ('matrix_process_observer.py','matrix_process_observer.py'),
                           ('matrix_worker_observer.py','matrix_worker_observer.py'),
                           ('tier2_retained_profiles.py','tier2_retained_profiles.py'),
                           ('retained_input_cache.py','retained_input_cache.py'),
                           ('check-matrix-result.py','executed-checker.py')]:
        require((root/captured).read_bytes() == git('scripts/'+name), 'executed capture/check helper differs from Git')
    servers = read(root/'observed-servers.json')
    require(len(servers) == 3 and {r['node'] for r in servers} == {0,1,2}
            and len({(r['pid'],r['start_ticks']) for r in servers}) == 3, 'requires actual three server identities')
    for server in servers:
        closed_instance(server['pid'], server['start_ticks'])
        require(server['captured'] == 'server-executables/'+server['actual_executable_sha256']+'.bin'
                and sha(root/server['captured']) == server['actual_executable_sha256']
                and Path(server['store']) == root/'originals'/TEST/f'node-{server["node"]}'
                and '\tmod\tgithub.com/nats-io/nats-server/v2\tv2.15.0' in server['build_info'], 'observed server bytes/version/store route differs')
        info = subprocess.check_output(['go','version','-m',str(root/server['captured'])],text=True)
        require(info.splitlines()[1:] == server['build_info'].splitlines()[1:], 'retained server build metadata differs')
        args = server['argv']
        if candidate_sha is not None:
            require(server['actual_executable_sha256']==candidate_sha and args[0]==str(root/'partition-server.bin'), 'observed candidate server differs from component bytes')
        require(args.count('-sd') == args.count('-n') == 1 and args[args.index('-sd')+1] == server['store']
                and args[args.index('-n')+1] == f'wf-process-{server["node"]}', 'server actual argv differs')
    # This original partition row runs workers in the SDK process. Separate
    # worker processes are selected by other original matrix rows only.
    require(read(root/'observed-workers.json') == [], 'unexpected separate worker process evidence for partition row')
    closure = closed.verify_no_open_originals(root)
    acceptance = read(root/'acceptance.json')
    require(acceptance['source'] == revision and acceptance['row'] == 'partition'
            and acceptance['duration'] == '10m' and acceptance['exit_code'] == acceptance['native_exit_code'] == 0
            and acceptance['server_observer_errors'] == 0 and acceptance.get('server_profile','default')==profile, 'duration acceptance differs')
    segment = (root/'native.log').read_text()+'\n'+(root/'acceptance.log').read_text()
    result = raw.review_raw(revision, root, seed, seed, {seed:segment}, model_root=model_root,
                            model_binary_out=output_root/f'history-model-{seed:03d}', row='partition',
                            event_files={seed:'converted-events.jsonl'})
    cohort = terminal_cohort(segment, result['invocations'],
                             b'MATRIX_TERMINAL_COHORT ' in git('integration/mixed_matrix_leader_test.go'))
    require(fixture_archive.inventory(root) == before, 'original fixture changed during independent review')
    require(all(sha(name) == digest for name,digest in external.items()), 'external model input changed during review')
    return dict(seed=seed, source=revision, actual_sdk_pid=execution['pid'], actual_sdk_sha256=execution['sha256'],
                source_inputs=len(names), external_inputs=len(external), observed_servers=servers,
                server_profile=profile, candidate_sha256=candidate_sha,
                closure=closure, fixture_inventory=before, raw_review=result, terminal_cohort=cohort,
                native_seed_qualified=True,
                scope='Original closed normal10m local partition seed; raw nineteen faults, latency samples, three history models and native final assertions. No broker opened or independent disk-store reconstruction.')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, required=True)
    parser.add_argument('--output-root', type=Path, required=True)
    parser.add_argument('--model-root', type=Path, required=True)
    parser.add_argument('--seed', type=int, help='One closed seed only; never qualifies the full200 row')
    parser.add_argument('--candidate-proof', type=Path, help='Explicit committed component proof; experimental candidate scope only')
    args = parser.parse_args()
    root = args.root.resolve()
    state = read(root/'campaign.json')
    profile='experimental-component-candidate' if args.candidate_proof is not None else 'default'
    records = contract(state, args.seed, profile)
    require(args.output_root.is_absolute() and not args.output_root.exists()
            and not args.output_root.resolve().is_relative_to(root), 'review output must be fresh and outside originals')
    require((root/'executed-campaign.py').read_bytes() == subprocess.check_output(
            ['git','show',state['source']+':scripts/run-local-tier2-partition-campaign.py'],cwd=REPO), 'campaign producer differs from executed Git')
    args.output_root.mkdir(parents=True)
    results = []
    for record in records:
        result = seed_review(root, record, state['source'], args.model_root, args.output_root, args.candidate_proof)
        (args.output_root/f'seed-{record["seed"]:03d}.json').write_text(json.dumps(result,indent=2)+'\n')
        results.append(result)
    full = args.seed is None
    result = dict(source=state['source'], row='partition', seeds=[r['seed'] for r in results],
                  server_profile=profile, qualifies_full_row=full, clears_partition_200_seed_gate=full and profile=='default',
                  qualifies_candidate_partition200=full and profile=='experimental-component-candidate',
                  qualifies_full_tier2_matrix=False, qualifies_24h_soak=False,
                  scope='Independent original local partition evidence only; hosted provider qualification remains separate.')
    (args.output_root/'review.json').write_text(json.dumps(result,indent=2)+'\n')
    print(json.dumps(result,indent=2))


if __name__ == '__main__':
    main()
